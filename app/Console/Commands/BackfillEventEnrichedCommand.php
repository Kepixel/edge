<?php

namespace App\Console\Commands;

use App\Jobs\ProcessEventEnrichedJob;
use ClickHouseDB\Client;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Cache;
use Throwable;

class BackfillEventEnrichedCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'events:backfill-enriched
                            {--chunk=1000 : Number of rows to process per batch}
                            {--delay=100 : Delay in milliseconds between batches to prevent queue overload}
                            {--queue=backfill : Queue name for dispatched jobs}
                            {--from= : Start from specific date (Y-m-d)}
                            {--to= : End at specific date (Y-m-d)}
                            {--team= : Filter by specific team_id}
                            {--source= : Filter by specific source_id}
                            {--skip-existing : Skip events that already exist in event_enriched}
                            {--dry-run : Show what would be processed without actually dispatching jobs}
                            {--resume : Resume from last processed position}
                            {--reset : Reset progress tracking and start fresh}
                            {--timeout=30 : ClickHouse query timeout in seconds}';

    /**
     * The console command description.
     */
    protected $description = 'Backfill event_enriched table from event_upload_logs (handles millions of rows)';

    private const CACHE_KEY_PROGRESS = 'backfill_enriched_progress';
    private const CACHE_KEY_STATS = 'backfill_enriched_stats';

    private Client $client;
    private int $processed = 0;
    private int $skipped = 0;
    private int $failed = 0;
    private int $total = 0;

    public function handle(): int
    {
        $this->client = app(Client::class);

        // Set query timeout
        $timeout = (int) $this->option('timeout');
        $this->client->setTimeout($timeout);
        $this->client->setConnectTimeOut($timeout);

        // Handle reset flag
        if ($this->option('reset')) {
            $this->resetProgress();
            $this->info('Progress tracking has been reset.');
            return self::SUCCESS;
        }

        $chunkSize = (int) $this->option('chunk');
        $delay = (int) $this->option('delay');
        $queue = $this->option('queue');
        $dryRun = $this->option('dry-run');
        $skipExisting = $this->option('skip-existing');
        $resume = $this->option('resume');

        // Build date filters
        $fromDate = $this->option('from');
        $toDate = $this->option('to');
        $teamId = $this->option('team');
        $sourceId = $this->option('source');

        // Get last processed offset if resuming
        $offset = $resume ? $this->getLastOffset() : 0;

        if ($resume && $offset > 0) {
            $this->info("Resuming from offset: {$offset}");
        }

        // Count total rows to process
        $this->total = $this->countTotalRows($fromDate, $toDate, $teamId, $sourceId);

        if ($this->total === 0) {
            $this->warn('No rows found matching the criteria.');
            return self::SUCCESS;
        }

        $this->info("Total rows to process: {$this->total}");
        $this->info("Chunk size: {$chunkSize}");
        $this->info("Queue: {$queue}");

        if ($dryRun) {
            $this->warn('DRY RUN MODE - No jobs will be dispatched');
        }

        $this->newLine();

        // Create progress bar
        $progressBar = $this->output->createProgressBar($this->total);
        $progressBar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | Elapsed: %elapsed:6s% | ETA: %remaining:-6s% | Processed: %processed% | Skipped: %skipped%');
        $progressBar->setMessage((string) $this->processed, 'processed');
        $progressBar->setMessage((string) $this->skipped, 'skipped');
        $progressBar->start();

        // Advance to current offset if resuming
        if ($offset > 0) {
            $progressBar->advance($offset);
            $this->processed = $offset;
        }

        // Get existing message_ids if skip-existing is enabled
        $existingMessageIds = [];
        if ($skipExisting) {
            $existingMessageIds = $this->getExistingMessageIds($fromDate, $toDate, $teamId, $sourceId);
            $this->info("\nLoaded " . count($existingMessageIds) . " existing message IDs to skip");
        }

        // Process in batches
        $hasMore = true;
        while ($hasMore) {
            try {
                $rows = $this->fetchBatch($offset, $chunkSize, $fromDate, $toDate, $teamId, $sourceId);

                if (empty($rows)) {
                    $hasMore = false;
                    break;
                }

                foreach ($rows as $row) {
                    $messageId = $row['message_id'] ?? null;

                    // Skip if already exists
                    if ($skipExisting && $messageId && isset($existingMessageIds[$messageId])) {
                        $this->skipped++;
                        $progressBar->setMessage((string) $this->skipped, 'skipped');
                        $progressBar->advance();
                        continue;
                    }

                    if (!$dryRun) {
                        $dispatched = $this->dispatchEnrichmentJob($row, $queue);
                        if (!$dispatched) {
                            // Row has invalid data (missing team_id, source_id, etc.)
                            $this->skipped++;
                            $progressBar->setMessage((string) $this->skipped, 'skipped');
                            $progressBar->advance();
                            continue;
                        }
                    }

                    $this->processed++;
                    $progressBar->setMessage((string) $this->processed, 'processed');
                    $progressBar->advance();
                }

                $offset += $chunkSize;

                // Save progress periodically
                $this->saveProgress($offset);

                // Delay between batches to prevent queue overload
                if ($delay > 0 && !$dryRun) {
                    usleep($delay * 1000);
                }

                // Check if we've processed all rows
                if (count($rows) < $chunkSize) {
                    $hasMore = false;
                }

            } catch (Throwable $e) {
                $this->failed++;
                $this->error("\nError at offset {$offset}: " . $e->getMessage());

                // Save progress so we can resume
                $this->saveProgress($offset);

                // Continue with next batch
                $offset += $chunkSize;
            }
        }

        $progressBar->finish();
        $this->newLine(2);

        // Clear progress on successful completion
        if (!$dryRun) {
            $this->clearProgress();
        }

        // Summary
        $this->displaySummary($dryRun);

        return self::SUCCESS;
    }

    /**
     * Count total rows matching the criteria.
     */
    private function countTotalRows(?string $fromDate, ?string $toDate, ?string $teamId, ?string $sourceId): int
    {
        $where = $this->buildWhereClause($fromDate, $toDate, $teamId, $sourceId);

        $sql = "SELECT count() as cnt FROM event_upload_logs {$where}";
        $result = $this->client->select($sql);

        return (int) ($result->rows()[0]['cnt'] ?? 0);
    }

    /**
     * Fetch a batch of rows from event_upload_logs.
     */
    private function fetchBatch(int $offset, int $limit, ?string $fromDate, ?string $toDate, ?string $teamId, ?string $sourceId): array
    {
        $where = $this->buildWhereClause($fromDate, $toDate, $teamId, $sourceId);

        $sql = "
            SELECT
                team_id,
                source_id,
                event_name,
                event_type,
                user_id,
                anonymous_id,
                message_id,
                session_id,
                rudder_id,
                properties,
                event_timestamp
            FROM event_upload_logs
            {$where}
            ORDER BY event_timestamp ASC, message_id ASC
            LIMIT {$limit} OFFSET {$offset}
        ";

        $result = $this->client->select($sql);

        return $result->rows();
    }

    /**
     * Build WHERE clause for queries.
     */
    private function buildWhereClause(?string $fromDate, ?string $toDate, ?string $teamId, ?string $sourceId): string
    {
        // Always filter out rows with null required fields
        $conditions = [
            "team_id IS NOT NULL",
            "team_id != ''",
            "source_id IS NOT NULL",
            "source_id != ''",
        ];

        if ($fromDate) {
            $conditions[] = "event_timestamp >= '{$fromDate} 00:00:00'";
        }

        if ($toDate) {
            $conditions[] = "event_timestamp <= '{$toDate} 23:59:59'";
        }

        if ($teamId) {
            $conditions[] = "team_id = '{$teamId}'";
        }

        if ($sourceId) {
            $conditions[] = "source_id = '{$sourceId}'";
        }

        return 'WHERE ' . implode(' AND ', $conditions);
    }

    /**
     * Get existing message IDs from event_enriched to skip duplicates.
     */
    private function getExistingMessageIds(?string $fromDate, ?string $toDate, ?string $teamId, ?string $sourceId): array
    {
        $where = $this->buildWhereClause($fromDate, $toDate, $teamId, $sourceId);

        // For large datasets, we query in batches
        $sql = "SELECT DISTINCT message_id FROM event_enriched {$where}";
        $result = $this->client->select($sql);

        $messageIds = [];
        foreach ($result->rows() as $row) {
            if (!empty($row['message_id'])) {
                $messageIds[$row['message_id']] = true;
            }
        }

        return $messageIds;
    }

    /**
     * Dispatch the enrichment job for a single row.
     * Returns false if row has invalid data and should be skipped.
     */
    private function dispatchEnrichmentJob(array $row, string $queue): bool
    {
        // Skip rows with missing required fields
        $teamId = $row['team_id'] ?? null;
        $sourceId = $row['source_id'] ?? null;
        $eventTimestamp = $row['event_timestamp'] ?? null;

        if (empty($teamId) || empty($sourceId) || empty($eventTimestamp)) {
            return false;
        }

        $properties = json_decode($row['properties'] ?? '{}', true) ?: [];

        ProcessEventEnrichedJob::dispatch(
            teamId: (string) $teamId,
            sourceId: (string) $sourceId,
            eventName: $row['event_name'] ?? 'unknown',
            eventType: $row['event_type'] ?? 'track',
            userId: $row['user_id'] ?? null,
            anonymousId: $row['anonymous_id'] ?? null,
            messageId: $row['message_id'] ?? null,
            sessionId: $row['session_id'] ?? null,
            rudderId: $row['rudder_id'] ?? null,
            properties: $properties,
            eventTimestamp: (string) $eventTimestamp,
        );

        return true;
    }

    /**
     * Get the last processed offset from cache.
     */
    private function getLastOffset(): int
    {
        return (int) Cache::get(self::CACHE_KEY_PROGRESS, 0);
    }

    /**
     * Save current progress to cache.
     */
    private function saveProgress(int $offset): void
    {
        Cache::put(self::CACHE_KEY_PROGRESS, $offset, now()->addDays(7));
        Cache::put(self::CACHE_KEY_STATS, [
            'processed' => $this->processed,
            'skipped' => $this->skipped,
            'failed' => $this->failed,
            'total' => $this->total,
            'last_updated' => now()->toDateTimeString(),
        ], now()->addDays(7));
    }

    /**
     * Clear progress tracking.
     */
    private function clearProgress(): void
    {
        Cache::forget(self::CACHE_KEY_PROGRESS);
        Cache::forget(self::CACHE_KEY_STATS);
    }

    /**
     * Reset progress tracking.
     */
    private function resetProgress(): void
    {
        $this->clearProgress();
    }

    /**
     * Display summary of the backfill operation.
     */
    private function displaySummary(bool $dryRun): void
    {
        $this->info('=== Backfill Summary ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Total Rows', number_format($this->total)],
                [$dryRun ? 'Would Process' : 'Processed', number_format($this->processed)],
                ['Skipped (existing)', number_format($this->skipped)],
                ['Failed', number_format($this->failed)],
            ]
        );

        if ($dryRun) {
            $this->warn('This was a DRY RUN. No jobs were dispatched.');
            $this->info('Run without --dry-run to actually process the events.');
        } else {
            $this->info('Jobs have been dispatched to the queue.');
            $this->info('Monitor queue workers to track processing progress.');
        }
    }
}
