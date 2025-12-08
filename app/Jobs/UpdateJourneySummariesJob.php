<?php

namespace App\Jobs;

use ClickHouseDB\Client;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\Log;
use Throwable;

class UpdateJourneySummariesJob implements ShouldQueue
{
    use Queueable;

    public int $tries = 3;
    public array $backoff = [30, 60, 120];

    private const JOB_TYPE = 'journey_summary';
    private const BATCH_SIZE = 1000;

    // Status definitions
    private const STATUS_ACTIVE = 'active';
    private const STATUS_CONVERTED = 'converted';
    private const STATUS_DORMANT = 'dormant';

    // Dormant threshold in days (users inactive for this many days are dormant)
    private const DORMANT_THRESHOLD_DAYS = 30;

    public function __construct(
        public ?string $teamId = null,
        public ?string $since = null,
    ) {}

    public function handle(): void
    {
        $client = app(Client::class);
        $client->setTimeout(300);
        $client->setConnectTimeOut(30);

        try {
            // Get last processed timestamp
            $lastProcessed = $this->since ?? $this->getLastProcessedTime($client);

            Log::info("UpdateJourneySummariesJob: Starting from {$lastProcessed}", [
                'team_id' => $this->teamId,
            ]);

            // Find users with updated touchpoints
            $usersToUpdate = $this->fetchUsersWithUpdates($client, $lastProcessed);

            if (empty($usersToUpdate)) {
                Log::info('UpdateJourneySummariesJob: No users to update');
                return;
            }

            Log::info('UpdateJourneySummariesJob: Updating ' . count($usersToUpdate) . ' user journeys');

            // Process in batches
            $chunks = array_chunk($usersToUpdate, self::BATCH_SIZE);
            $processedCount = 0;

            foreach ($chunks as $chunk) {
                $this->processUserBatch($client, $chunk);
                $processedCount += count($chunk);
            }

            // Update processing state
            $this->updateProcessingState($client, $usersToUpdate);

            Log::info("UpdateJourneySummariesJob: Completed. Updated {$processedCount} user summaries");

        } catch (Throwable $e) {
            Log::error('UpdateJourneySummariesJob: Error - ' . $e->getMessage(), [
                'team_id' => $this->teamId,
                'exception' => $e,
            ]);
            throw $e;
        }
    }

    private function getLastProcessedTime(Client $client): string
    {
        $teamFilter = $this->teamId ? "AND team_id = '{$this->teamId}'" : '';

        $sql = "
            SELECT last_event_timestamp
            FROM attribution_processing_state
            WHERE job_type = '" . self::JOB_TYPE . "'
            {$teamFilter}
            ORDER BY last_event_timestamp DESC
            LIMIT 1
        ";

        $result = $client->select($sql);
        $rows = $result->rows();

        if (!empty($rows)) {
            return $rows[0]['last_event_timestamp'];
        }

        // Default: 30 days ago
        return date('Y-m-d H:i:s', strtotime('-30 days'));
    }

    private function fetchUsersWithUpdates(Client $client, string $since): array
    {
        $teamFilter = $this->teamId ? "AND team_id = '{$this->teamId}'" : '';

        // Find distinct users with touchpoints after the last processing time
        $sql = "
            SELECT DISTINCT
                team_id,
                resolved_user_id
            FROM user_touchpoints
            WHERE touchpoint_timestamp > '{$since}'
            {$teamFilter}
            LIMIT 50000
        ";

        $result = $client->select($sql);
        return $result->rows();
    }

    private function processUserBatch(Client $client, array $users): void
    {
        $summaries = [];

        foreach ($users as $user) {
            $summary = $this->buildUserSummary($client, $user['team_id'], $user['resolved_user_id']);
            if ($summary) {
                $summaries[] = $summary;
            }
        }

        if (!empty($summaries)) {
            $this->insertSummaries($client, $summaries);
        }
    }

    private function buildUserSummary(Client $client, string $teamId, string $resolvedUserId): ?array
    {
        // Get aggregated journey data
        $journeyData = $this->getJourneyAggregates($client, $teamId, $resolvedUserId);

        if (empty($journeyData)) {
            return null;
        }

        // Get user profile data
        $profileData = $this->getUserProfile($client, $teamId, $resolvedUserId);

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

    private function getJourneyAggregates(Client $client, string $teamId, string $resolvedUserId): ?array
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

        $result = $client->select($sql);
        $rows = $result->rows();

        if (empty($rows) || empty($rows[0]['first_seen'])) {
            return null;
        }

        return $rows[0];
    }

    private function getUserProfile(Client $client, string $teamId, string $resolvedUserId): array
    {
        // Remove 'anon_' prefix if present to look up in user_profiles
        $canonicalId = $resolvedUserId;
        if (str_starts_with($resolvedUserId, 'anon_')) {
            $anonymousId = substr($resolvedUserId, 5);

            // Try to find mapped user_id
            $sql = "
                SELECT user_id
                FROM identity_mappings
                WHERE team_id = '{$teamId}'
                AND anonymous_id = '{$anonymousId}'
                ORDER BY last_seen_at DESC
                LIMIT 1
            ";

            $result = $client->select($sql);
            $rows = $result->rows();

            if (!empty($rows) && !empty($rows[0]['user_id'])) {
                $canonicalId = $rows[0]['user_id'];
            }
        }

        // Get profile data
        $sql = "
            SELECT
                email,
                phone,
                name,
                username
            FROM user_profiles
            WHERE team_id = '{$teamId}'
            AND canonical_user_id = '{$canonicalId}'
            ORDER BY updated_at DESC
            LIMIT 1
        ";

        $result = $client->select($sql);
        $rows = $result->rows();

        if (!empty($rows)) {
            return $rows[0];
        }

        return [];
    }

    private function calculateStatus(array $journeyData): string
    {
        // Converted: has at least one conversion
        if ((int) $journeyData['conversion_count'] > 0) {
            return self::STATUS_CONVERTED;
        }

        // Dormant: no activity in the threshold period
        if ((int) $journeyData['days_since_last_touch'] >= self::DORMANT_THRESHOLD_DAYS) {
            return self::STATUS_DORMANT;
        }

        // Active: recent activity but no conversion yet
        return self::STATUS_ACTIVE;
    }

    private function insertSummaries(Client $client, array $summaries): void
    {
        $rows = [];

        foreach ($summaries as $summary) {
            // Convert platforms and channels arrays to ClickHouse array format
            $platforms = $summary['platforms'];
            $channels = $summary['channels'];

            // Filter out empty values
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

        $client->insert(
            'user_journey_summary',
            $rows,
            [
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
            ]
        );
    }

    private function updateProcessingState(Client $client, array $users): void
    {
        if (empty($users)) {
            return;
        }

        $teamId = $this->teamId ?? 'all';
        $now = date('Y-m-d H:i:s');

        $client->insert(
            'attribution_processing_state',
            [[
                $teamId,
                self::JOB_TYPE,
                $now,
                $now,
                count($users),
                0,
            ]],
            [
                'team_id',
                'job_type',
                'last_processed_at',
                'last_event_timestamp',
                'events_processed',
                'conversions_processed',
            ]
        );
    }

    public function retryUntil(): \DateTime
    {
        return now()->addMinutes(30);
    }
}
