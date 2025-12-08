<?php

namespace App\Jobs;

use ClickHouseDB\Client;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\Log;
use Throwable;

class BuildUserJourneysJob implements ShouldQueue
{
    use Queueable;

    public int $tries = 3;
    public array $backoff = [30, 60, 120];

    private const JOB_TYPE = 'journey_builder';
    private const BATCH_SIZE = 1000;

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

            Log::info("BuildUserJourneysJob: Starting from {$lastProcessed}", [
                'team_id' => $this->teamId,
            ]);

            // Fetch new events from event_enriched
            $events = $this->fetchNewEvents($client, $lastProcessed);

            if (empty($events)) {
                Log::info('BuildUserJourneysJob: No new events to process');
                return;
            }

            Log::info('BuildUserJourneysJob: Processing ' . count($events) . ' events');

            // Group events by team_id and resolved_user_id
            $groupedEvents = $this->groupEventsByUser($client, $events);

            // Process each user's events
            $processedCount = 0;
            foreach ($groupedEvents as $teamId => $userEvents) {
                foreach ($userEvents as $resolvedUserId => $userEventList) {
                    $this->processUserEvents($client, $teamId, $resolvedUserId, $userEventList);
                    $processedCount++;
                }
            }

            // Update processing state
            $this->updateProcessingState($client, $events);

            Log::info("BuildUserJourneysJob: Completed. Processed {$processedCount} user journeys");

        } catch (Throwable $e) {
            Log::error('BuildUserJourneysJob: Error - ' . $e->getMessage(), [
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

    private function fetchNewEvents(Client $client, string $since): array
    {
        $teamFilter = $this->teamId ? "AND team_id = '{$this->teamId}'" : '';

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
            WHERE event_timestamp > '{$since}'
            {$teamFilter}
            ORDER BY event_timestamp ASC
            LIMIT 50000
        ";

        $result = $client->select($sql);
        return $result->rows();
    }

    private function groupEventsByUser(Client $client, array $events): array
    {
        $grouped = [];

        foreach ($events as $event) {
            $teamId = $event['team_id'];
            $userId = $event['user_id'] ?? '';
            $anonymousId = $event['anonymous_id'] ?? '';

            // Resolve identity
            $resolvedUserId = $this->resolveIdentity($client, $teamId, $userId, $anonymousId);

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

    private function resolveIdentity(Client $client, string $teamId, string $userId, string $anonymousId): string
    {
        // Priority 1: user_id if present
        if (!empty($userId)) {
            return $userId;
        }

        // Priority 2: Look up anonymous_id in identity_mappings
        if (!empty($anonymousId)) {
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
                return $rows[0]['user_id'];
            }

            // Priority 3: Use anonymous_id as identity
            return 'anon_' . $anonymousId;
        }

        return 'unknown_' . uniqid();
    }

    private function processUserEvents(Client $client, string $teamId, string $resolvedUserId, array $events): void
    {
        // Sort events by timestamp
        usort($events, fn($a, $b) => strtotime($a['event_timestamp']) <=> strtotime($b['event_timestamp']));

        // Get existing touchpoints for this user
        $existingTouchpoints = $this->getExistingTouchpoints($client, $teamId, $resolvedUserId);

        // Get first touch data
        $firstTouchData = $this->getFirstTouchData($existingTouchpoints, $events);

        // Determine starting touchpoint number
        $touchpointNumber = count($existingTouchpoints) + 1;

        // Check for conversions
        $conversionEvents = $this->getConversionEvents($client, $teamId);

        // Build touchpoints
        $touchpoints = [];
        $prevTimestamp = $this->getLastTouchpointTimestamp($existingTouchpoints);

        foreach ($events as $event) {
            // Check if this event is a conversion
            $isConversion = isset($conversionEvents[$event['event_name']]);
            $conversionScore = $isConversion ? ($conversionEvents[$event['event_name']]['score'] ?? 1.0) : 0.0;

            // Extract conversion value if it's a conversion (would need properties which we don't have here)
            $conversionValue = 0;
            $conversionRevenue = 0;

            $eventTimestamp = $event['event_timestamp'];
            $firstTouchAt = $firstTouchData['timestamp'] ?? $eventTimestamp;

            $daysSinceFirst = $this->calculateDaysBetween($firstTouchAt, $eventTimestamp);
            $hoursSinceFirst = $this->calculateHoursBetween($firstTouchAt, $eventTimestamp);
            $hoursSincePrev = $prevTimestamp ? $this->calculateHoursBetween($prevTimestamp, $eventTimestamp) : 0;

            $touchpoints[] = [
                substr($eventTimestamp, 0, 10),  // event_date
                $teamId,
                $resolvedUserId,
                $event['user_id'] ?? null,
                $event['anonymous_id'] ?? null,
                $event['session_id'] ?? null,
                $event['message_id'] ?? uniqid(),
                $touchpointNumber,
                $eventTimestamp,
                $firstTouchAt,
                $firstTouchData['source'] ?? $event['utm_source'],
                $firstTouchData['medium'] ?? $event['utm_medium'],
                $firstTouchData['campaign'] ?? $event['utm_campaign'],
                $firstTouchData['platform'] ?? $event['platform'],
                $firstTouchData['channel'] ?? $event['traffic_channel'],
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
                $conversionValue,
                $conversionRevenue,
                'USD',  // default currency
                '',     // order_id
                $daysSinceFirst,
                $hoursSinceFirst,
                $hoursSincePrev,
                $event['message_id'] ?? '',
            ];

            $prevTimestamp = $eventTimestamp;
            $touchpointNumber++;
        }

        // Insert touchpoints
        if (!empty($touchpoints)) {
            $client->insert(
                'user_touchpoints',
                $touchpoints,
                [
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
                ]
            );
        }
    }

    private function getExistingTouchpoints(Client $client, string $teamId, string $resolvedUserId): array
    {
        $sql = "
            SELECT *
            FROM user_touchpoints
            WHERE team_id = '{$teamId}'
            AND resolved_user_id = '{$resolvedUserId}'
            ORDER BY touchpoint_timestamp ASC
        ";

        $result = $client->select($sql);
        return $result->rows();
    }

    private function getFirstTouchData(array $existingTouchpoints, array $newEvents): array
    {
        // Use existing first touch if available
        if (!empty($existingTouchpoints)) {
            $first = $existingTouchpoints[0];
            return [
                'timestamp' => $first['first_touch_at'] ?? $first['touchpoint_timestamp'],
                'source' => $first['first_touch_source'] ?? $first['utm_source'],
                'medium' => $first['first_touch_medium'] ?? $first['utm_medium'],
                'campaign' => $first['first_touch_campaign'] ?? $first['utm_campaign'],
                'platform' => $first['first_touch_platform'] ?? $first['platform'],
                'channel' => $first['first_touch_channel'] ?? $first['traffic_channel'],
            ];
        }

        // Use first new event
        if (!empty($newEvents)) {
            $first = $newEvents[0];
            return [
                'timestamp' => $first['event_timestamp'],
                'source' => $first['utm_source'] ?? '',
                'medium' => $first['utm_medium'] ?? '',
                'campaign' => $first['utm_campaign'] ?? '',
                'platform' => $first['platform'] ?? '',
                'channel' => $first['traffic_channel'] ?? '',
            ];
        }

        return [];
    }

    private function getLastTouchpointTimestamp(array $existingTouchpoints): ?string
    {
        if (empty($existingTouchpoints)) {
            return null;
        }

        $last = end($existingTouchpoints);
        return $last['touchpoint_timestamp'] ?? null;
    }

    private function getConversionEvents(Client $client, string $teamId): array
    {
        // First try to get from team override (MySQL would be better but we're in edge)
        // For now, just get from ClickHouse defaults
        $sql = "SELECT event_name, score FROM conversion_events_config_default";

        try {
            $result = $client->select($sql);
            $rows = $result->rows();

            $events = [];
            foreach ($rows as $row) {
                $events[$row['event_name']] = [
                    'score' => (float) $row['score'],
                ];
            }
            return $events;
        } catch (Throwable $e) {
            // If table doesn't exist yet, return common defaults
            return [
                'purchase' => ['score' => 1.0],
                'order_completed' => ['score' => 1.0],
                'sign_up' => ['score' => 0.5],
                'lead' => ['score' => 0.3],
            ];
        }
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

    private function updateProcessingState(Client $client, array $events): void
    {
        if (empty($events)) {
            return;
        }

        // Find the latest event timestamp
        $latestTimestamp = '';
        foreach ($events as $event) {
            if ($event['event_timestamp'] > $latestTimestamp) {
                $latestTimestamp = $event['event_timestamp'];
            }
        }

        $teamId = $this->teamId ?? 'all';

        $client->insert(
            'attribution_processing_state',
            [[
                $teamId,
                self::JOB_TYPE,
                date('Y-m-d H:i:s'),
                $latestTimestamp,
                count($events),
                0,  // conversions_processed (filled by attribution job)
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
