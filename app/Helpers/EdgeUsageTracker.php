<?php

namespace App\Helpers;

use App\Models\Team;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

/**
 * Lightweight usage tracker for the edge app.
 *
 * This service directly manipulates Redis counters without
 * heavy dependencies like Stripe or Spark. The billing period
 * is expected to be pre-cached by the main app's WarmBillingPeriodCacheJob.
 */
class EdgeUsageTracker
{
    private const BILLING_PERIOD_TTL_BUFFER_DAYS = 7;

    protected array $orderEventNames = [
        'order completed',
        'order_completed',
        'purchase',
        'checkout completed',
        'checkout_completed',
    ];

    /**
     * Track if Redis is available (circuit breaker pattern)
     */
    protected static bool $redisAvailable = true;
    protected static int $redisFailureCount = 0;
    protected static ?int $redisLastFailure = null;

    /**
     * Increment usage counters in Redis with failure handling.
     * Returns the current counts for sampling decisions.
     */
    public function incrementUsage(Team $team, ?string $eventName): array
    {
        $billingPeriod = $this->getBillingPeriod($team);
        $periodKey = $billingPeriod['start'];
        $isOrderEvent = $this->isOrderEventName($eventName);

        // Check circuit breaker
        if (!$this->isRedisAvailable()) {
            return $this->handleRedisUnavailable($team, $billingPeriod, $isOrderEvent);
        }

        try {
            // Pipeline for efficiency
            $results = Redis::pipeline(function ($pipe) use ($team, $periodKey, $isOrderEvent) {
                $pipe->incr("team:{$team->id}:usage:{$periodKey}:events");
                if ($isOrderEvent) {
                    $pipe->incr("team:{$team->id}:usage:{$periodKey}:orders");
                } else {
                    $pipe->get("team:{$team->id}:usage:{$periodKey}:orders");
                }
            });

            $eventCount = (int) $results[0];
            $orderCount = (int) ($isOrderEvent ? $results[1] : ($results[1] ?? 0));

            // Set TTL on first increment
            if ($eventCount === 1) {
                $expireAt = strtotime($billingPeriod['end']) + (self::BILLING_PERIOD_TTL_BUFFER_DAYS * 86400);
                Redis::expireat("team:{$team->id}:usage:{$periodKey}:events", $expireAt);
                Redis::expireat("team:{$team->id}:usage:{$periodKey}:orders", $expireAt);
            }

            // Reset circuit breaker on success
            $this->markRedisSuccess();

            return [
                'events' => $eventCount,
                'orders' => $orderCount,
                'billing_period' => $billingPeriod,
                'is_order_event' => $isOrderEvent,
            ];
        } catch (\RedisException $e) {
            return $this->handleRedisFailure($team, $billingPeriod, $isOrderEvent, $e);
        }
    }

    /**
     * Check if Redis is available (circuit breaker)
     */
    protected function isRedisAvailable(): bool
    {
        // If marked unavailable, check if we should retry
        if (!self::$redisAvailable && self::$redisLastFailure) {
            // Retry after 30 seconds
            if (time() - self::$redisLastFailure > 30) {
                self::$redisAvailable = true;
                self::$redisFailureCount = 0;
            }
        }

        return self::$redisAvailable;
    }

    /**
     * Mark Redis as successful
     */
    protected function markRedisSuccess(): void
    {
        self::$redisAvailable = true;
        self::$redisFailureCount = 0;
        self::$redisLastFailure = null;
    }

    /**
     * Handle Redis failure with circuit breaker
     */
    protected function handleRedisFailure(Team $team, array $billingPeriod, bool $isOrderEvent, \RedisException $e): array
    {
        self::$redisFailureCount++;
        self::$redisLastFailure = time();

        // Open circuit after 3 consecutive failures
        if (self::$redisFailureCount >= 3) {
            self::$redisAvailable = false;
            Log::error('Redis circuit breaker opened', [
                'team_id' => $team->id,
                'failure_count' => self::$redisFailureCount,
                'error' => $e->getMessage(),
            ]);
        } else {
            Log::warning('Redis failure during usage tracking', [
                'team_id' => $team->id,
                'failure_count' => self::$redisFailureCount,
                'error' => $e->getMessage(),
            ]);
        }

        return $this->handleRedisUnavailable($team, $billingPeriod, $isOrderEvent);
    }

    /**
     * Handle case when Redis is unavailable
     * Events are still logged to ClickHouse, reconciliation will catch up
     */
    protected function handleRedisUnavailable(Team $team, array $billingPeriod, bool $isOrderEvent): array
    {
        // Return estimated values - events still go to ClickHouse via SeedEventUploadLogJob
        // Hourly reconciliation will sync accurate counts later
        return [
            'events' => 0, // Unknown, but events are logged to ClickHouse
            'orders' => 0,
            'billing_period' => $billingPeriod,
            'is_order_event' => $isOrderEvent,
            'redis_unavailable' => true,
        ];
    }

    /**
     * Get billing period from Redis cache.
     * Falls back to calendar month if not cached.
     */
    public function getBillingPeriod(Team $team): array
    {
        $cacheKey = "team:{$team->id}:billing_period";
        $cached = Redis::get($cacheKey);

        if ($cached) {
            return json_decode($cached, true);
        }

        // Fallback to calendar month if not cached
        // Main app's WarmBillingPeriodCacheJob should pre-populate this
        return [
            'start' => now()->startOfMonth()->format('Y-m-d'),
            'end' => now()->startOfMonth()->addMonth()->format('Y-m-d'),
            'type' => null,
            'max_events' => 50000,
            'max_orders' => 500,
            'is_free' => true,
        ];
    }

    /**
     * Check if threshold check should be dispatched (sampling).
     * Check every 1000 events OR every 10 orders.
     */
    public function shouldCheckThreshold(int $eventCount, int $orderCount): bool
    {
        return ($eventCount % 1000 === 0) || ($orderCount > 0 && $orderCount % 10 === 0);
    }

    /**
     * Check if the event name is an order event.
     */
    protected function isOrderEventName(?string $eventName): bool
    {
        if ($eventName === null) {
            return false;
        }
        return in_array(strtolower(trim($eventName)), $this->orderEventNames);
    }
}
