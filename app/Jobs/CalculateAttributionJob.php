<?php

namespace App\Jobs;

use ClickHouseDB\Client;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\Log;
use Throwable;

class CalculateAttributionJob implements ShouldQueue
{
    use Queueable;

    public int $tries = 3;
    public array $backoff = [30, 60, 120];

    private const JOB_TYPE = 'attribution_calculator';

    // Attribution models
    private const MODEL_LAST_CLICK = 'last_click';
    private const MODEL_FIRST_CLICK = 'first_click';
    private const MODEL_LINEAR = 'linear';
    private const MODEL_POSITION_BASED = 'position_based';
    private const MODEL_TIME_DECAY = 'time_decay';
    private const MODEL_J_SHAPED = 'j_shaped';

    private const HALF_LIFE_DAYS = 7;

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

            Log::info("CalculateAttributionJob: Starting from {$lastProcessed}", [
                'team_id' => $this->teamId,
            ]);

            // Fetch new conversions from user_touchpoints
            $conversions = $this->fetchNewConversions($client, $lastProcessed);

            if (empty($conversions)) {
                Log::info('CalculateAttributionJob: No new conversions to process');
                return;
            }

            Log::info('CalculateAttributionJob: Processing ' . count($conversions) . ' conversions');

            $processedCount = 0;
            foreach ($conversions as $conversion) {
                $this->processConversion($client, $conversion);
                $processedCount++;
            }

            // Update processing state
            $this->updateProcessingState($client, $conversions);

            Log::info("CalculateAttributionJob: Completed. Processed {$processedCount} conversions");

        } catch (Throwable $e) {
            Log::error('CalculateAttributionJob: Error - ' . $e->getMessage(), [
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

    private function fetchNewConversions(Client $client, string $since): array
    {
        $teamFilter = $this->teamId ? "AND team_id = '{$this->teamId}'" : '';

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
            WHERE is_conversion = 1
            AND touchpoint_timestamp > '{$since}'
            {$teamFilter}
            ORDER BY touchpoint_timestamp ASC
            LIMIT 10000
        ";

        $result = $client->select($sql);
        return $result->rows();
    }

    private function processConversion(Client $client, array $conversion): void
    {
        $teamId = $conversion['team_id'];
        $resolvedUserId = $conversion['resolved_user_id'];
        $conversionTimestamp = $conversion['touchpoint_timestamp'];

        // Get all touchpoints for this user before conversion
        $touchpoints = $this->fetchUserTouchpoints($client, $teamId, $resolvedUserId, $conversionTimestamp);

        if (empty($touchpoints)) {
            return;
        }

        // Get platform configs (use defaults for now)
        $platformConfigs = $this->getPlatformConfigs($client);

        // Calculate platform view (each platform's own attribution)
        $platformResults = $this->calculatePlatformView($touchpoints, $conversion, $platformConfigs);

        // Calculate deduplicated view (priority resolves overlaps)
        $deduplicatedResults = $this->calculateDeduplicatedView($touchpoints, $conversion, $platformConfigs);

        // Insert results
        $this->insertAttributionResults($client, $platformResults, 'platform');
        $this->insertAttributionResults($client, $deduplicatedResults, 'deduplicated');
    }

    private function fetchUserTouchpoints(Client $client, string $teamId, string $resolvedUserId, string $beforeTimestamp): array
    {
        $sql = "
            SELECT *
            FROM user_touchpoints
            WHERE team_id = '{$teamId}'
            AND resolved_user_id = '{$resolvedUserId}'
            AND touchpoint_timestamp < '{$beforeTimestamp}'
            ORDER BY touchpoint_timestamp ASC
        ";

        $result = $client->select($sql);
        return $result->rows();
    }

    private function getPlatformConfigs(Client $client): array
    {
        $sql = "SELECT platform, click_window_days, view_window_days, priority, model FROM ad_platform_config_default";

        try {
            $result = $client->select($sql);
            $rows = $result->rows();

            $configs = [];
            foreach ($rows as $row) {
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
            // Return sensible defaults if table doesn't exist
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

            // Filter by window
            $validTouchpoints = $this->filterByWindow($platformTouchpoints, $conversion, $config['click_window_days']);

            if (empty($validTouchpoints)) {
                continue;
            }

            // Apply model
            $attributed = $this->applyModel($validTouchpoints, $conversion, $config['model']);

            // Build results
            foreach ($attributed as $tp) {
                $results[] = $this->buildAttributionResult($tp, $conversion, $config, count($validTouchpoints));
            }
        }

        return $results;
    }

    private function calculateDeduplicatedView(array $touchpoints, array $conversion, array $platformConfigs): array
    {
        // Find highest priority platform with valid touchpoints
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

        // Get highest priority platform
        arsort($platformPriorities);
        $winningPlatform = array_key_first($platformPriorities);
        $config = $platformConfigs[$winningPlatform] ?? $this->getDefaultConfig($winningPlatform);

        // Filter to winning platform's touchpoints within window
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

        // Apply model
        $attributed = $this->applyModel($validTouchpoints, $conversion, $config['model']);

        // Build results
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
        if (empty($touchpoints)) {
            return [];
        }

        $lastIndex = count($touchpoints) - 1;

        foreach ($touchpoints as $i => &$tp) {
            $tp['attribution_credit'] = ($i === $lastIndex) ? 1.0 : 0.0;
            $tp['is_assisted'] = ($i !== $lastIndex) ? 1 : 0;
        }

        return $touchpoints;
    }

    private function applyFirstClick(array $touchpoints): array
    {
        if (empty($touchpoints)) {
            return [];
        }

        foreach ($touchpoints as $i => &$tp) {
            $tp['attribution_credit'] = ($i === 0) ? 1.0 : 0.0;
            $tp['is_assisted'] = ($i !== 0) ? 1 : 0;
        }

        return $touchpoints;
    }

    private function applyLinear(array $touchpoints): array
    {
        if (empty($touchpoints)) {
            return [];
        }

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

        if ($count === 0) {
            return [];
        }

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
        if (empty($touchpoints)) {
            return [];
        }

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

        if ($count === 0) {
            return [];
        }

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

    private function insertAttributionResults(Client $client, array $results, string $attributionType): void
    {
        if (empty($results)) {
            return;
        }

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

        $client->insert(
            'attributed_conversions',
            $rows,
            [
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
            ]
        );
    }

    private function updateProcessingState(Client $client, array $conversions): void
    {
        if (empty($conversions)) {
            return;
        }

        $latestTimestamp = '';
        foreach ($conversions as $conversion) {
            if ($conversion['touchpoint_timestamp'] > $latestTimestamp) {
                $latestTimestamp = $conversion['touchpoint_timestamp'];
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
                0,
                count($conversions),
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
