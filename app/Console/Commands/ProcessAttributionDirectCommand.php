<?php

namespace App\Console\Commands;

use ClickHouseDB\Client;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Direct ClickHouse attribution processing - NO QUEUE, processes synchronously.
 * Combines all three steps: Build Journeys -> Calculate Attribution -> Update Summaries
 * Uses the same logic as the individual jobs but processes in batches directly.
 */
class ProcessAttributionDirectCommand extends Command
{
    protected $signature = 'attribution:process-direct
                            {--chunk=5000 : Number of events to process per batch}
                            {--from= : Start from specific date (Y-m-d)}
                            {--to= : End at specific date (Y-m-d)}
                            {--team= : Filter by specific team_id}
                            {--step= : Run only specific step (journeys|attribution|summaries)}
                            {--dry-run : Show what would be processed without inserting}';

    protected $description = 'Process attribution directly via ClickHouse (fastest method for backfill)';

    private Client $client;

    // Attribution models
    private const MODEL_LAST_CLICK = 'last_click';
    private const MODEL_FIRST_CLICK = 'first_click';
    private const MODEL_LINEAR = 'linear';
    private const MODEL_POSITION_BASED = 'position_based';
    private const MODEL_TIME_DECAY = 'time_decay';
    private const MODEL_J_SHAPED = 'j_shaped';

    private const HALF_LIFE_DAYS = 7;

    // Status definitions for journey summaries
    private const STATUS_ACTIVE = 'active';
    private const STATUS_CONVERTED = 'converted';
    private const STATUS_DORMANT = 'dormant';
    private const DORMANT_THRESHOLD_DAYS = 30;

    public function handle(): int
    {
        $this->client = app(Client::class);
        $this->client->setTimeout(600);
        $this->client->setConnectTimeOut(60);

        $chunkSize = (int) $this->option('chunk');
        $fromDate = $this->option('from');
        $toDate = $this->option('to');
        $teamId = $this->option('team');
        $step = $this->option('step');
        $dryRun = $this->option('dry-run');

        $this->info('=== Attribution Direct Processing ===');
        $this->info("Chunk size: {$chunkSize}");
        if ($fromDate) $this->info("From: {$fromDate}");
        if ($toDate) $this->info("To: {$toDate}");
        if ($teamId) $this->info("Team: {$teamId}");
        $this->info("Mode: " . ($dryRun ? 'DRY RUN' : 'DIRECT INSERT'));
        $this->newLine();

        if ($dryRun) {
            $this->warn('DRY RUN - No data will be inserted');
        }

        $steps = $step ? [$step] : ['journeys', 'attribution', 'summaries'];

        try {
            foreach ($steps as $currentStep) {
                $this->processStep($currentStep, $chunkSize, $fromDate, $toDate, $teamId, $dryRun);
            }

            $this->newLine();
            $this->info('Attribution processing completed successfully!');

            return self::SUCCESS;

        } catch (Throwable $e) {
            $this->error('Error: ' . $e->getMessage());
            Log::error('ProcessAttributionDirectCommand: Error', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            return self::FAILURE;
        }
    }

    private function processStep(string $step, int $chunkSize, ?string $fromDate, ?string $toDate, ?string $teamId, bool $dryRun): void
    {
        switch ($step) {
            case 'journeys':
                $this->processJourneys($chunkSize, $fromDate, $toDate, $teamId, $dryRun);
                break;

            case 'attribution':
                $this->processAttribution($chunkSize, $fromDate, $toDate, $teamId, $dryRun);
                break;

            case 'summaries':
                $this->processSummaries($chunkSize, $fromDate, $toDate, $teamId, $dryRun);
                break;

            default:
                $this->warn("Unknown step: {$step}");
        }
    }

    // ============================================================
    // STEP 1: BUILD USER JOURNEYS
    // ============================================================

    private function processJourneys(int $chunkSize, ?string $fromDate, ?string $toDate, ?string $teamId, bool $dryRun): void
    {
        $this->info('Step 1/3: Building User Journeys...');

        $where = $this->buildWhereClause($fromDate, $toDate, $teamId, 'event_timestamp');
        $total = $this->countEvents($where);

        if ($total === 0) {
            $this->info('  -> No events to process');
            return;
        }

        $this->info("  -> Processing {$total} events");

        $progressBar = $this->output->createProgressBar($total);
        $progressBar->start();

        $offset = 0;
        $processedUsers = 0;

        while ($offset < $total) {
            $events = $this->fetchEnrichedEvents($where, $offset, $chunkSize);

            if (empty($events)) {
                break;
            }

            // Group by user
            $groupedEvents = $this->groupEventsByUser($events);

            // Get conversion events config
            $conversionEvents = $this->getConversionEvents();

            // Process each user
            $touchpointRows = [];
            foreach ($groupedEvents as $teamIdKey => $userEvents) {
                foreach ($userEvents as $resolvedUserId => $userEventList) {
                    $touchpoints = $this->buildUserTouchpoints($teamIdKey, $resolvedUserId, $userEventList, $conversionEvents);
                    $touchpointRows = array_merge($touchpointRows, $touchpoints);
                    $processedUsers++;
                }
            }

            // Insert touchpoints
            if (!$dryRun && !empty($touchpointRows)) {
                $this->insertTouchpoints($touchpointRows);
            }

            $progressBar->advance(count($events));
            $offset += $chunkSize;
        }

        $progressBar->finish();
        $this->newLine();
        $this->info("  -> Built journeys for {$processedUsers} users");
    }

    private function countEvents(string $where): int
    {
        $sql = "SELECT count() as cnt FROM event_enriched {$where}";
        $result = $this->client->select($sql);
        return (int) ($result->rows()[0]['cnt'] ?? 0);
    }

    private function fetchEnrichedEvents(string $where, int $offset, int $limit): array
    {
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
                event_timestamp,
                page_url,
                page_path,
                page_domain,
                landing_referrer,
                landing_referring_domain,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                utm_term,
                traffic_channel,
                platform,
                click_id,
                is_paid,
                is_direct
            FROM event_enriched
            {$where}
            ORDER BY event_timestamp ASC
            LIMIT {$limit} OFFSET {$offset}
        ";

        return $this->client->select($sql)->rows();
    }

    private function groupEventsByUser(array $events): array
    {
        $grouped = [];

        foreach ($events as $event) {
            $teamId = $event['team_id'];
            $userId = $event['user_id'] ?? '';
            $anonymousId = $event['anonymous_id'] ?? '';

            // Resolve identity
            $resolvedUserId = $this->resolveIdentity($teamId, $userId, $anonymousId);

            if (!isset($grouped[$teamId])) {
                $grouped[$teamId] = [];
            }
            if (!isset($grouped[$teamId][$resolvedUserId])) {
                $grouped[$teamId][$resolvedUserId] = [];
            }

            $event['resolved_user_id'] = $resolvedUserId;
            $grouped[$teamId][$resolvedUserId][] = $event;
        }

        return $grouped;
    }

    private function resolveIdentity(string $teamId, string $userId, string $anonymousId): string
    {
        if (!empty($userId)) {
            return $userId;
        }

        if (!empty($anonymousId)) {
            // Check identity mappings
            $sql = "
                SELECT user_id
                FROM identity_mappings
                WHERE team_id = '{$teamId}'
                AND anonymous_id = '{$anonymousId}'
                ORDER BY last_seen_at DESC
                LIMIT 1
            ";

            try {
                $result = $this->client->select($sql);
                $rows = $result->rows();

                if (!empty($rows) && !empty($rows[0]['user_id'])) {
                    return $rows[0]['user_id'];
                }
            } catch (Throwable $e) {
                // Table might not exist
            }

            return 'anon_' . $anonymousId;
        }

        return 'unknown_' . uniqid();
    }

    private function getConversionEvents(): array
    {
        try {
            $sql = "SELECT event_name, score FROM conversion_events_config_default";
            $result = $this->client->select($sql);

            $events = [];
            foreach ($result->rows() as $row) {
                $events[$row['event_name']] = ['score' => (float) $row['score']];
            }
            return $events;
        } catch (Throwable $e) {
            return [
                'purchase' => ['score' => 1.0],
                'order_completed' => ['score' => 1.0],
                'sign_up' => ['score' => 0.5],
                'lead' => ['score' => 0.3],
            ];
        }
    }

    private function buildUserTouchpoints(string $teamId, string $resolvedUserId, array $events, array $conversionEvents): array
    {
        // Sort events by timestamp
        usort($events, fn($a, $b) => strtotime($a['event_timestamp']) <=> strtotime($b['event_timestamp']));

        if (empty($events)) {
            return [];
        }

        // First touch data
        $firstEvent = $events[0];
        $firstTouchAt = $firstEvent['event_timestamp'];
        $firstTouchSource = $firstEvent['utm_source'] ?? '';
        $firstTouchMedium = $firstEvent['utm_medium'] ?? '';
        $firstTouchCampaign = $firstEvent['utm_campaign'] ?? '';
        $firstTouchPlatform = $firstEvent['platform'] ?? '';
        $firstTouchChannel = $firstEvent['traffic_channel'] ?? '';

        $touchpoints = [];
        $touchpointNumber = 1;
        $prevTimestamp = null;

        foreach ($events as $event) {
            $eventTimestamp = $event['event_timestamp'];
            $isConversion = isset($conversionEvents[$event['event_name']]);
            $conversionScore = $isConversion ? ($conversionEvents[$event['event_name']]['score'] ?? 1.0) : 0.0;

            $daysSinceFirst = $this->calculateDaysBetween($firstTouchAt, $eventTimestamp);
            $hoursSinceFirst = $this->calculateHoursBetween($firstTouchAt, $eventTimestamp);
            $hoursSincePrev = $prevTimestamp ? $this->calculateHoursBetween($prevTimestamp, $eventTimestamp) : 0;

            $touchpoints[] = [
                substr($eventTimestamp, 0, 10),
                $teamId,
                $resolvedUserId,
                $event['user_id'] ?? null,
                $event['anonymous_id'] ?? null,
                $event['session_id'] ?? null,
                $event['message_id'] ?? uniqid(),
                $touchpointNumber,
                $eventTimestamp,
                $firstTouchAt,
                $firstTouchSource,
                $firstTouchMedium,
                $firstTouchCampaign,
                $firstTouchPlatform,
                $firstTouchChannel,
                $event['utm_source'] ?? '',
                $event['utm_medium'] ?? '',
                $event['utm_campaign'] ?? '',
                $event['utm_content'] ?? '',
                $event['utm_term'] ?? '',
                $event['traffic_channel'] ?? '',
                $event['platform'] ?? '',
                $event['click_id'] ?? '',
                (int) ($event['is_paid'] ?? 0),
                (int) ($event['is_direct'] ?? 0),
                $event['page_url'] ?? '',
                $event['page_path'] ?? '',
                $event['page_domain'] ?? '',
                $event['landing_referrer'] ?? '',
                $event['landing_referring_domain'] ?? '',
                $event['event_name'] ?? '',
                $event['event_type'] ?? '',
                $isConversion ? 1 : 0,
                $conversionScore,
                0, // conversion_value
                0, // conversion_revenue
                'USD',
                '', // order_id
                $daysSinceFirst,
                $hoursSinceFirst,
                $hoursSincePrev,
                $event['message_id'] ?? '',
            ];

            $prevTimestamp = $eventTimestamp;
            $touchpointNumber++;
        }

        return $touchpoints;
    }

    private function insertTouchpoints(array $rows): void
    {
        $this->client->insert('user_touchpoints', $rows, [
            'event_date',
            'team_id',
            'resolved_user_id',
            'original_user_id',
            'anonymous_id',
            'session_id',
            'message_id',
            'touchpoint_number',
            'touchpoint_timestamp',
            'first_touch_at',
            'first_touch_source',
            'first_touch_medium',
            'first_touch_campaign',
            'first_touch_platform',
            'first_touch_channel',
            'utm_source',
            'utm_medium',
            'utm_campaign',
            'utm_content',
            'utm_term',
            'traffic_channel',
            'platform',
            'click_id',
            'is_paid',
            'is_direct',
            'page_url',
            'page_path',
            'page_domain',
            'landing_referrer',
            'landing_referring_domain',
            'event_name',
            'event_type',
            'is_conversion',
            'conversion_score',
            'conversion_value',
            'conversion_revenue',
            'conversion_currency',
            'order_id',
            'days_since_first_touch',
            'hours_since_first_touch',
            'hours_since_prev_touch',
            'source_message_id',
        ]);
    }

    // ============================================================
    // STEP 2: CALCULATE ATTRIBUTION
    // ============================================================

    private function processAttribution(int $chunkSize, ?string $fromDate, ?string $toDate, ?string $teamId, bool $dryRun): void
    {
        $this->info('Step 2/3: Calculating Attribution...');

        $where = $this->buildWhereClause($fromDate, $toDate, $teamId, 'touchpoint_timestamp');
        $where .= " AND is_conversion = 1";
        $total = $this->countConversions($where);

        if ($total === 0) {
            $this->info('  -> No conversions to process');
            return;
        }

        $this->info("  -> Processing {$total} conversions");

        $progressBar = $this->output->createProgressBar($total);
        $progressBar->start();

        // Get platform configs
        $platformConfigs = $this->getPlatformConfigs();

        $offset = 0;
        $processedConversions = 0;

        while ($offset < $total) {
            $conversions = $this->fetchConversions($where, $offset, $chunkSize);

            if (empty($conversions)) {
                break;
            }

            $platformResults = [];
            $deduplicatedResults = [];

            foreach ($conversions as $conversion) {
                // Get touchpoints for this user
                $touchpoints = $this->fetchUserTouchpoints(
                    $conversion['team_id'],
                    $conversion['resolved_user_id'],
                    $conversion['touchpoint_timestamp']
                );

                if (!empty($touchpoints)) {
                    // Calculate both views
                    $platformResults = array_merge(
                        $platformResults,
                        $this->calculatePlatformView($touchpoints, $conversion, $platformConfigs)
                    );
                    $deduplicatedResults = array_merge(
                        $deduplicatedResults,
                        $this->calculateDeduplicatedView($touchpoints, $conversion, $platformConfigs)
                    );
                }

                $processedConversions++;
            }

            // Insert results
            if (!$dryRun) {
                if (!empty($platformResults)) {
                    $this->insertAttributionResults($platformResults, 'platform');
                }
                if (!empty($deduplicatedResults)) {
                    $this->insertAttributionResults($deduplicatedResults, 'deduplicated');
                }
            }

            $progressBar->advance(count($conversions));
            $offset += $chunkSize;
        }

        $progressBar->finish();
        $this->newLine();
        $this->info("  -> Calculated attribution for {$processedConversions} conversions");
    }

    private function countConversions(string $where): int
    {
        $sql = "SELECT count() as cnt FROM user_touchpoints {$where}";
        $result = $this->client->select($sql);
        return (int) ($result->rows()[0]['cnt'] ?? 0);
    }

    private function fetchConversions(string $where, int $offset, int $limit): array
    {
        $sql = "
            SELECT
                team_id,
                resolved_user_id,
                message_id,
                touchpoint_timestamp,
                event_name,
                conversion_score,
                conversion_value,
                conversion_revenue,
                conversion_currency,
                order_id
            FROM user_touchpoints
            {$where}
            ORDER BY touchpoint_timestamp ASC
            LIMIT {$limit} OFFSET {$offset}
        ";

        return $this->client->select($sql)->rows();
    }

    private function fetchUserTouchpoints(string $teamId, string $resolvedUserId, string $beforeTimestamp): array
    {
        $sql = "
            SELECT *
            FROM user_touchpoints
            WHERE team_id = '{$teamId}'
            AND resolved_user_id = '{$resolvedUserId}'
            AND touchpoint_timestamp < '{$beforeTimestamp}'
            ORDER BY touchpoint_timestamp ASC
        ";

        return $this->client->select($sql)->rows();
    }

    private function getPlatformConfigs(): array
    {
        try {
            $sql = "SELECT platform, click_window_days, view_window_days, priority, model FROM ad_platform_config_default";
            $result = $this->client->select($sql);

            $configs = [];
            foreach ($result->rows() as $row) {
                $configs[$row['platform']] = [
                    'platform' => $row['platform'],
                    'click_window_days' => (int) $row['click_window_days'],
                    'view_window_days' => (int) $row['view_window_days'],
                    'priority' => (int) $row['priority'],
                    'model' => $row['model'],
                ];
            }
            return $configs;
        } catch (Throwable $e) {
            return [
                'default' => [
                    'platform' => 'default',
                    'click_window_days' => 30,
                    'view_window_days' => 1,
                    'priority' => 1,
                    'model' => 'last_click',
                ],
            ];
        }
    }

    private function getDefaultConfig(string $platform): array
    {
        return [
            'platform' => $platform,
            'click_window_days' => 30,
            'view_window_days' => 1,
            'priority' => 1,
            'model' => 'last_click',
        ];
    }

    private function calculatePlatformView(array $touchpoints, array $conversion, array $platformConfigs): array
    {
        $results = [];

        // Group touchpoints by platform
        $byPlatform = [];
        foreach ($touchpoints as $tp) {
            $platform = $tp['platform'] ?? 'other';
            $byPlatform[$platform][] = $tp;
        }

        foreach ($byPlatform as $platform => $platformTouchpoints) {
            $config = $platformConfigs[$platform] ?? $this->getDefaultConfig($platform);

            $validTouchpoints = $this->filterByWindow($platformTouchpoints, $conversion, $config['click_window_days']);

            if (empty($validTouchpoints)) {
                continue;
            }

            $attributed = $this->applyModel($validTouchpoints, $conversion, $config['model']);

            foreach ($attributed as $tp) {
                $results[] = $this->buildAttributionResult($tp, $conversion, $config, count($validTouchpoints));
            }
        }

        return $results;
    }

    private function calculateDeduplicatedView(array $touchpoints, array $conversion, array $platformConfigs): array
    {
        $platformPriorities = [];

        foreach ($touchpoints as $tp) {
            $platform = $tp['platform'] ?? 'other';
            $config = $platformConfigs[$platform] ?? $this->getDefaultConfig($platform);

            $daysToConversion = $this->calculateDaysToConversion($tp, $conversion);
            if ($daysToConversion <= $config['click_window_days']) {
                $priority = $config['priority'];
                if (!isset($platformPriorities[$platform]) || $priority > $platformPriorities[$platform]) {
                    $platformPriorities[$platform] = $priority;
                }
            }
        }

        if (empty($platformPriorities)) {
            return [];
        }

        arsort($platformPriorities);
        $winningPlatform = array_key_first($platformPriorities);
        $config = $platformConfigs[$winningPlatform] ?? $this->getDefaultConfig($winningPlatform);

        $validTouchpoints = [];
        foreach ($touchpoints as $tp) {
            if (($tp['platform'] ?? 'other') === $winningPlatform) {
                $daysToConversion = $this->calculateDaysToConversion($tp, $conversion);
                if ($daysToConversion <= $config['click_window_days']) {
                    $validTouchpoints[] = $tp;
                }
            }
        }

        if (empty($validTouchpoints)) {
            return [];
        }

        $attributed = $this->applyModel($validTouchpoints, $conversion, $config['model']);

        $results = [];
        foreach ($attributed as $tp) {
            $results[] = $this->buildAttributionResult($tp, $conversion, $config, count($validTouchpoints));
        }

        return $results;
    }

    private function filterByWindow(array $touchpoints, array $conversion, int $clickWindowDays): array
    {
        $validTouchpoints = [];

        foreach ($touchpoints as $tp) {
            $days = $this->calculateDaysToConversion($tp, $conversion);
            if ($days <= $clickWindowDays) {
                $tp['days_to_conversion'] = $days;
                $validTouchpoints[] = $tp;
            }
        }

        return $validTouchpoints;
    }

    private function calculateDaysToConversion(array $touchpoint, array $conversion): int
    {
        $tpTime = strtotime($touchpoint['touchpoint_timestamp']);
        $convTime = strtotime($conversion['touchpoint_timestamp']);
        return (int) floor(($convTime - $tpTime) / 86400);
    }

    private function applyModel(array $touchpoints, array $conversion, string $model): array
    {
        return match ($model) {
            self::MODEL_LAST_CLICK => $this->applyLastClick($touchpoints),
            self::MODEL_FIRST_CLICK => $this->applyFirstClick($touchpoints),
            self::MODEL_LINEAR => $this->applyLinear($touchpoints),
            self::MODEL_POSITION_BASED => $this->applyPositionBased($touchpoints),
            self::MODEL_TIME_DECAY => $this->applyTimeDecay($touchpoints, $conversion),
            self::MODEL_J_SHAPED => $this->applyJShaped($touchpoints),
            default => $this->applyLastClick($touchpoints),
        };
    }

    private function applyLastClick(array $touchpoints): array
    {
        if (empty($touchpoints)) return [];

        $lastIndex = count($touchpoints) - 1;
        foreach ($touchpoints as $i => &$tp) {
            $tp['attribution_credit'] = ($i === $lastIndex) ? 1.0 : 0.0;
            $tp['is_assisted'] = ($i !== $lastIndex) ? 1 : 0;
        }
        return $touchpoints;
    }

    private function applyFirstClick(array $touchpoints): array
    {
        if (empty($touchpoints)) return [];

        foreach ($touchpoints as $i => &$tp) {
            $tp['attribution_credit'] = ($i === 0) ? 1.0 : 0.0;
            $tp['is_assisted'] = ($i !== 0) ? 1 : 0;
        }
        return $touchpoints;
    }

    private function applyLinear(array $touchpoints): array
    {
        if (empty($touchpoints)) return [];

        $credit = 1.0 / count($touchpoints);
        foreach ($touchpoints as &$tp) {
            $tp['attribution_credit'] = $credit;
            $tp['is_assisted'] = 0;
        }
        return $touchpoints;
    }

    private function applyPositionBased(array $touchpoints): array
    {
        $count = count($touchpoints);
        if ($count === 0) return [];

        if ($count === 1) {
            $touchpoints[0]['attribution_credit'] = 1.0;
            $touchpoints[0]['is_assisted'] = 0;
            return $touchpoints;
        }

        if ($count === 2) {
            $touchpoints[0]['attribution_credit'] = 0.5;
            $touchpoints[1]['attribution_credit'] = 0.5;
            $touchpoints[0]['is_assisted'] = 0;
            $touchpoints[1]['is_assisted'] = 0;
            return $touchpoints;
        }

        $middleCount = $count - 2;
        $middleCredit = 0.2 / $middleCount;

        foreach ($touchpoints as $i => &$tp) {
            if ($i === 0) {
                $tp['attribution_credit'] = 0.4;
            } elseif ($i === $count - 1) {
                $tp['attribution_credit'] = 0.4;
            } else {
                $tp['attribution_credit'] = $middleCredit;
            }
            $tp['is_assisted'] = 0;
        }

        return $touchpoints;
    }

    private function applyTimeDecay(array $touchpoints, array $conversion): array
    {
        if (empty($touchpoints)) return [];

        $conversionTime = strtotime($conversion['touchpoint_timestamp']);
        $halfLifeSeconds = self::HALF_LIFE_DAYS * 24 * 60 * 60;

        $weights = [];
        foreach ($touchpoints as $tp) {
            $touchpointTime = strtotime($tp['touchpoint_timestamp']);
            $timeDiff = max(0, $conversionTime - $touchpointTime);
            $weights[] = pow(2, -$timeDiff / $halfLifeSeconds);
        }

        $totalWeight = array_sum($weights);

        foreach ($touchpoints as $i => &$tp) {
            $tp['attribution_credit'] = $totalWeight > 0 ? $weights[$i] / $totalWeight : 0;
            $tp['is_assisted'] = 0;
        }

        return $touchpoints;
    }

    private function applyJShaped(array $touchpoints): array
    {
        $count = count($touchpoints);
        if ($count === 0) return [];

        if ($count === 1) {
            $touchpoints[0]['attribution_credit'] = 1.0;
            $touchpoints[0]['is_assisted'] = 0;
            return $touchpoints;
        }

        if ($count === 2) {
            $touchpoints[0]['attribution_credit'] = 0.25;
            $touchpoints[1]['attribution_credit'] = 0.75;
            $touchpoints[0]['is_assisted'] = 0;
            $touchpoints[1]['is_assisted'] = 0;
            return $touchpoints;
        }

        $middleCount = $count - 2;
        $middleCredit = 0.2 / $middleCount;

        foreach ($touchpoints as $i => &$tp) {
            if ($i === 0) {
                $tp['attribution_credit'] = 0.2;
            } elseif ($i === $count - 1) {
                $tp['attribution_credit'] = 0.6;
            } else {
                $tp['attribution_credit'] = $middleCredit;
            }
            $tp['is_assisted'] = 0;
        }

        return $touchpoints;
    }

    private function buildAttributionResult(array $touchpoint, array $conversion, array $config, int $totalTouchpoints): array
    {
        $credit = $touchpoint['attribution_credit'] ?? 0;
        $conversionValue = (float) ($conversion['conversion_value'] ?? 0);
        $conversionRevenue = (float) ($conversion['conversion_revenue'] ?? 0);
        $conversionScore = (float) ($conversion['conversion_score'] ?? 0);
        $isAssisted = (int) ($touchpoint['is_assisted'] ?? 0);

        $daysToConversion = $touchpoint['days_to_conversion'] ?? $this->calculateDaysToConversion($touchpoint, $conversion);
        $hoursToConversion = (int) floor($daysToConversion * 24);

        return [
            'conversion_date' => substr($conversion['touchpoint_timestamp'], 0, 10),
            'team_id' => $conversion['team_id'],
            'resolved_user_id' => $conversion['resolved_user_id'],
            'conversion_timestamp' => $conversion['touchpoint_timestamp'],
            'conversion_event' => $conversion['event_name'],
            'conversion_value' => $conversionValue,
            'conversion_revenue' => $conversionRevenue,
            'conversion_currency' => $conversion['conversion_currency'] ?? 'USD',
            'conversion_score' => $conversionScore,
            'order_id' => $conversion['order_id'] ?? '',
            'conversion_message_id' => $conversion['message_id'],
            'touchpoint_number' => $touchpoint['touchpoint_number'] ?? 0,
            'touchpoint_timestamp' => $touchpoint['touchpoint_timestamp'],
            'touchpoint_message_id' => $touchpoint['message_id'] ?? '',
            'platform' => $touchpoint['platform'] ?? '',
            'traffic_channel' => $touchpoint['traffic_channel'] ?? '',
            'utm_source' => $touchpoint['utm_source'] ?? '',
            'utm_medium' => $touchpoint['utm_medium'] ?? '',
            'utm_campaign' => $touchpoint['utm_campaign'] ?? '',
            'utm_content' => $touchpoint['utm_content'] ?? '',
            'utm_term' => $touchpoint['utm_term'] ?? '',
            'click_id' => $touchpoint['click_id'] ?? '',
            'is_paid' => (int) ($touchpoint['is_paid'] ?? 0),
            'model' => $config['model'],
            'attribution_credit' => $credit,
            'attributed_value' => $conversionValue * $credit,
            'attributed_revenue' => $conversionRevenue * $credit,
            'attributed_score' => $conversionScore * $credit,
            'is_assisted' => $isAssisted,
            'assisted_value' => $isAssisted ? $conversionValue : 0,
            'assisted_revenue' => $isAssisted ? $conversionRevenue : 0,
            'days_to_conversion' => $daysToConversion,
            'hours_to_conversion' => $hoursToConversion,
            'within_click_window' => $daysToConversion <= $config['click_window_days'] ? 1 : 0,
            'within_view_window' => $daysToConversion <= $config['view_window_days'] ? 1 : 0,
            'platform_priority' => $config['priority'],
            'platform_click_window' => $config['click_window_days'],
            'platform_view_window' => $config['view_window_days'],
            'is_first_touch' => ($touchpoint['touchpoint_number'] ?? 0) === 1 ? 1 : 0,
            'is_last_touch' => ($touchpoint['touchpoint_number'] ?? 0) === $totalTouchpoints ? 1 : 0,
            'total_touchpoints' => $totalTouchpoints,
        ];
    }

    private function insertAttributionResults(array $results, string $attributionType): void
    {
        if (empty($results)) return;

        $rows = [];
        foreach ($results as $result) {
            $rows[] = [
                $result['conversion_date'],
                $result['team_id'],
                $result['resolved_user_id'],
                $result['conversion_timestamp'],
                $result['conversion_event'],
                $result['conversion_value'],
                $result['conversion_revenue'],
                $result['conversion_currency'],
                $result['conversion_score'],
                $result['order_id'],
                $result['conversion_message_id'],
                $result['touchpoint_number'],
                $result['touchpoint_timestamp'],
                $result['touchpoint_message_id'],
                $result['platform'],
                $result['traffic_channel'],
                $result['utm_source'],
                $result['utm_medium'],
                $result['utm_campaign'],
                $result['utm_content'],
                $result['utm_term'],
                $result['click_id'],
                $result['is_paid'],
                $attributionType,
                $result['model'],
                $result['attribution_credit'],
                $result['attributed_value'],
                $result['attributed_revenue'],
                $result['attributed_score'],
                $result['is_assisted'],
                $result['assisted_value'],
                $result['assisted_revenue'],
                $result['days_to_conversion'],
                $result['hours_to_conversion'],
                $result['within_click_window'],
                $result['within_view_window'],
                $result['platform_priority'],
                $result['platform_click_window'],
                $result['platform_view_window'],
                $result['is_first_touch'],
                $result['is_last_touch'],
                $result['total_touchpoints'],
            ];
        }

        $this->client->insert('attributed_conversions', $rows, [
            'conversion_date',
            'team_id',
            'resolved_user_id',
            'conversion_timestamp',
            'conversion_event',
            'conversion_value',
            'conversion_revenue',
            'conversion_currency',
            'conversion_score',
            'order_id',
            'conversion_message_id',
            'touchpoint_number',
            'touchpoint_timestamp',
            'touchpoint_message_id',
            'platform',
            'traffic_channel',
            'utm_source',
            'utm_medium',
            'utm_campaign',
            'utm_content',
            'utm_term',
            'click_id',
            'is_paid',
            'attribution_type',
            'model',
            'attribution_credit',
            'attributed_value',
            'attributed_revenue',
            'attributed_score',
            'is_assisted',
            'assisted_value',
            'assisted_revenue',
            'days_to_conversion',
            'hours_to_conversion',
            'within_click_window',
            'within_view_window',
            'platform_priority',
            'platform_click_window',
            'platform_view_window',
            'is_first_touch',
            'is_last_touch',
            'total_touchpoints',
        ]);
    }

    // ============================================================
    // STEP 3: UPDATE JOURNEY SUMMARIES
    // ============================================================

    private function processSummaries(int $chunkSize, ?string $fromDate, ?string $toDate, ?string $teamId, bool $dryRun): void
    {
        $this->info('Step 3/3: Updating Journey Summaries...');

        $where = $this->buildWhereClause($fromDate, $toDate, $teamId, 'touchpoint_timestamp');

        // Get distinct users
        $sql = "SELECT DISTINCT team_id, resolved_user_id FROM user_touchpoints {$where}";
        $users = $this->client->select($sql)->rows();

        $total = count($users);

        if ($total === 0) {
            $this->info('  -> No users to update');
            return;
        }

        $this->info("  -> Updating summaries for {$total} users");

        $progressBar = $this->output->createProgressBar($total);
        $progressBar->start();

        $chunks = array_chunk($users, $chunkSize);
        $processedCount = 0;

        foreach ($chunks as $chunk) {
            $summaries = [];

            foreach ($chunk as $user) {
                $summary = $this->buildUserSummary($user['team_id'], $user['resolved_user_id']);
                if ($summary) {
                    $summaries[] = $summary;
                }
            }

            if (!$dryRun && !empty($summaries)) {
                $this->insertSummaries($summaries);
            }

            $progressBar->advance(count($chunk));
            $processedCount += count($chunk);
        }

        $progressBar->finish();
        $this->newLine();
        $this->info("  -> Updated {$processedCount} user summaries");
    }

    private function buildUserSummary(string $teamId, string $resolvedUserId): ?array
    {
        $sql = "
            SELECT
                min(touchpoint_timestamp) as first_seen,
                max(touchpoint_timestamp) as last_seen,
                count() as touchpoint_count,
                sum(is_conversion) as conversion_count,
                sum(conversion_value) as total_conversion_value,
                sum(conversion_revenue) as total_conversion_revenue,
                groupUniqArray(platform) as platforms,
                groupUniqArray(traffic_channel) as channels,
                dateDiff('day', max(touchpoint_timestamp), now()) as days_since_last_touch
            FROM user_touchpoints
            WHERE team_id = '{$teamId}'
            AND resolved_user_id = '{$resolvedUserId}'
        ";

        $result = $this->client->select($sql);
        $rows = $result->rows();

        if (empty($rows) || empty($rows[0]['first_seen'])) {
            return null;
        }

        $journeyData = $rows[0];

        // Get user profile
        $profileData = $this->getUserProfile($teamId, $resolvedUserId);

        // Calculate status
        $status = $this->calculateStatus($journeyData);

        return [
            'team_id' => $teamId,
            'canonical_user_id' => $resolvedUserId,
            'email' => $profileData['email'] ?? null,
            'phone' => $profileData['phone'] ?? null,
            'name' => $profileData['name'] ?? null,
            'username' => $profileData['username'] ?? null,
            'first_seen' => $journeyData['first_seen'],
            'last_seen' => $journeyData['last_seen'],
            'touchpoint_count' => (int) $journeyData['touchpoint_count'],
            'conversion_count' => (int) $journeyData['conversion_count'],
            'total_conversion_value' => (float) $journeyData['total_conversion_value'],
            'total_conversion_revenue' => (float) $journeyData['total_conversion_revenue'],
            'platforms' => $journeyData['platforms'],
            'channels' => $journeyData['channels'],
            'status' => $status,
            'days_since_last_touch' => (int) $journeyData['days_since_last_touch'],
        ];
    }

    private function getUserProfile(string $teamId, string $resolvedUserId): array
    {
        $canonicalId = $resolvedUserId;

        if (str_starts_with($resolvedUserId, 'anon_')) {
            $anonymousId = substr($resolvedUserId, 5);

            try {
                $sql = "
                    SELECT user_id
                    FROM identity_mappings
                    WHERE team_id = '{$teamId}'
                    AND anonymous_id = '{$anonymousId}'
                    ORDER BY last_seen_at DESC
                    LIMIT 1
                ";

                $result = $this->client->select($sql);
                $rows = $result->rows();

                if (!empty($rows) && !empty($rows[0]['user_id'])) {
                    $canonicalId = $rows[0]['user_id'];
                }
            } catch (Throwable $e) {
                // Table might not exist
            }
        }

        try {
            $sql = "
                SELECT email, phone, name, username
                FROM user_profiles
                WHERE team_id = '{$teamId}'
                AND canonical_user_id = '{$canonicalId}'
                ORDER BY last_seen DESC
                LIMIT 1
            ";

            $result = $this->client->select($sql);
            $rows = $result->rows();

            if (!empty($rows)) {
                return $rows[0];
            }
        } catch (Throwable $e) {
            // Table might not exist
        }

        return [];
    }

    private function calculateStatus(array $journeyData): string
    {
        if ((int) $journeyData['conversion_count'] > 0) {
            return self::STATUS_CONVERTED;
        }

        if ((int) $journeyData['days_since_last_touch'] >= self::DORMANT_THRESHOLD_DAYS) {
            return self::STATUS_DORMANT;
        }

        return self::STATUS_ACTIVE;
    }

    private function insertSummaries(array $summaries): void
    {
        $rows = [];

        foreach ($summaries as $summary) {
            $platforms = $summary['platforms'];
            $channels = $summary['channels'];

            if (is_array($platforms)) {
                $platforms = array_filter($platforms, fn($p) => !empty($p));
            } else {
                $platforms = [];
            }

            if (is_array($channels)) {
                $channels = array_filter($channels, fn($c) => !empty($c));
            } else {
                $channels = [];
            }

            $rows[] = [
                $summary['team_id'],
                $summary['canonical_user_id'],
                $summary['email'],
                $summary['phone'],
                $summary['name'],
                $summary['username'],
                $summary['first_seen'],
                $summary['last_seen'],
                $summary['touchpoint_count'],
                $summary['conversion_count'],
                $summary['total_conversion_value'],
                $summary['total_conversion_revenue'],
                $platforms,
                $channels,
                $summary['status'],
                $summary['days_since_last_touch'],
            ];
        }

        $this->client->insert('user_journey_summary', $rows, [
            'team_id',
            'canonical_user_id',
            'email',
            'phone',
            'name',
            'username',
            'first_seen',
            'last_seen',
            'touchpoint_count',
            'conversion_count',
            'total_conversion_value',
            'total_conversion_revenue',
            'platforms',
            'channels',
            'status',
            'days_since_last_touch',
        ]);
    }

    // ============================================================
    // HELPERS
    // ============================================================

    private function buildWhereClause(?string $fromDate, ?string $toDate, ?string $teamId, string $timestampColumn): string
    {
        $conditions = [];

        if ($fromDate) {
            $conditions[] = "{$timestampColumn} >= '{$fromDate} 00:00:00'";
        }
        if ($toDate) {
            $conditions[] = "{$timestampColumn} <= '{$toDate} 23:59:59'";
        }
        if ($teamId) {
            $conditions[] = "team_id = '{$teamId}'";
        }

        if (empty($conditions)) {
            return '';
        }

        return 'WHERE ' . implode(' AND ', $conditions);
    }

    private function calculateDaysBetween(string $start, string $end): int
    {
        $startTime = strtotime($start);
        $endTime = strtotime($end);
        return (int) floor(($endTime - $startTime) / 86400);
    }

    private function calculateHoursBetween(string $start, string $end): int
    {
        $startTime = strtotime($start);
        $endTime = strtotime($end);
        return (int) floor(($endTime - $startTime) / 3600);
    }
}
