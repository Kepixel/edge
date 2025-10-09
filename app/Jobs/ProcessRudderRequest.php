<?php

namespace App\Jobs;

use App\Models\Source;
use App\Services\TeamEventUsageService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class ProcessRudderRequest implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * The number of times the job may be attempted.
     *
     * @var int
     */
    public $tries = 10;

    /**
     * The maximum number of seconds the job can run.
     *
     * @var int
     */
    public $timeout = 120;

    /**
     * Calculate the number of seconds to wait before retrying the job.
     *
     * @return array<int, int>
     */
    public function backoff(): array
    {
        return [1, 2, 5, 10, 15, 30, 60, 90, 120, 180];
    }

    protected $sourceKey;

    protected $data;

    protected $headers;

    protected $path;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct($sourceKey, $data, $headers, $path)
    {
        $this->sourceKey = $sourceKey;
        $this->data = $data;
        $this->headers = $headers;
        $this->path = $path;
//        $this->onConnection('database');
        $this->onQueue('high');
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        $source = Source::where('app_token', $this->sourceKey)->with('team')->first();
        if (! $source) {
            Log::emergency('Source not found for token: '.$this->sourceKey);
            return;
        }

        $team = $source->team;

        if (! $team) {
            Log::emergency('Team not found for source: '.$source->id);
            return;
        }

//        $usageService = app(TeamEventUsageService::class);
//        $currentMonth = now()->startOfMonth();
//
//        if ($usageService->limitReachedRecently($team, $currentMonth)) {
//            Log::emergency('Team already exceeded monthly events quota (cached); dropping Rudder request.', [
//                'team_id' => $team->id,
//                'source_id' => $source->id,
//                'path' => $this->path,
//                'attempt' => $this->attempts(),
//            ]);
//
//            return;
//        }
//
//        $usage = $usageService->currentMonthUsage($team);
//
//        if ($usage !== null && $usageService->hasReachedLimit($usage)) {
//            $usageService->storeTriggeredThreshold($team, $usage['month'], 100);
//
//            Log::emergency('Team reached the monthly events quota; dropping Rudder request.', [
//                'team_id' => $team->id,
//                'source_id' => $source->id,
//                'used_events' => $usage['used_events'],
//                'max_events' => $usage['max_events'],
//                'path' => $this->path,
//                'attempt' => $this->attempts(),
//            ]);
//
//            return;
//        }

        // Enhanced port validation: check port in database, env file, and container status
//        $validationResult = $this->validateTeamConfiguration($team, $source->id);
//        if ($validationResult['should_return']) {
//            return;
//        }
//        if ($validationResult['should_retry']) {
//            throw new \RuntimeException($validationResult['message']);
//        }

        $paths = [
            'v1/i' => 'v1/identify',
            'v1/t' => 'v1/track',
            'v1/p' => 'v1/page',
            'v1/s' => 'v1/screen',
            'v1/g' => 'v1/group',
            'v1/a' => 'v1/alias',
            'v1/b' => 'v1/batch',
        ];

        $path = $paths[$this->path] ?? $this->path;

        if ($this->isTrackEvent()) {
            // need to get event from data and validate it
            $event = $this->data['event'] ?? null;
            if (! $event) {
                // This is a non-retryable failure - track event is missing
                return;
            }
            // need to validate event properties data
            if (! $this->validateTrackEventProperties($event)) {
                // This is a non-retryable failure - track event properties are invalid
                return;
            }
            $this->injectUserTraits();
        }

//        $port = $validationResult['port'];
        $url = "http://localhost:8080/$path";
        $headers = $this->headers;
        $headers['authorization'] = 'Basic '.base64_encode($source->write_key.':');

        try {
            $response = Http::asJson()->acceptJson()->withoutVerifying()
                ->retry(3, 100)
                ->timeout(30)
                ->withHeaders($headers)
                ->post($url, $this->data);

            if ($response->ok()) {
                SeedEventUploadLogJob::dispatch($source, $this->data);
            }

            if ($response->failed()) {
                $statusCode = $response->status();
                $errorMessage = "Failed to send data to Rudder: HTTP {$statusCode}";

                Log::error($errorMessage, [
                    'source_key' => $this->sourceKey,
                    'status' => $statusCode,
                    'response' => $response->body(),
                    'url' => $url,
                    'attempt' => $this->attempts(),
                ]);

                // Determine if this is a retryable error
                if ($this->isRetryableHttpError($statusCode)) {
                    throw new \RuntimeException($errorMessage);
                }
            }
        } catch (ConnectionException $e) {
            // Network/connection issues are retryable
            Log::error('Connection error processing Rudder request: '.$e->getMessage(), [
                'source_key' => $this->sourceKey,
                'url' => $url,
                'attempt' => $this->attempts(),
                'exception' => $e,
            ]);

            throw $e;
        } catch (\Exception $e) {
            // For other exceptions, log and don't retry unless it's clearly retryable
            Log::error('Unexpected error processing Rudder request: '.$e->getMessage(), [
                'source_key' => $this->sourceKey,
                'url' => $url,
                'attempt' => $this->attempts(),
                'exception' => $e,
            ]);

            // Don't retry unexpected exceptions
            return;
        }
    }

    /**
     * Determine if an HTTP status code indicates a retryable error.
     */
    private function isRetryableHttpError(int $statusCode): bool
    {
        // Retry on server errors (5xx) and some client errors
        return $statusCode >= 500 ||
            in_array($statusCode, [408, 429, 502, 503, 504, 522, 524]);
    }

    /**
     * Check if the current request is a track event.
     */
    private function isTrackEvent(): bool
    {
        return $this->path === 'v1/t' || $this->path === 'v1/track';
    }

    /**
     * Validate track event properties.
     */
    private function validateTrackEventProperties(string $event): bool
    {
        $ecommerceEvents = [
            // Browsing
            'Products Searched',
            'Product List Viewed',
            'Product List Filtered',

            // Promotion
            'Promotion Viewed',
            'Promotion Clicked',

            // Ordering
            'Product Clicked',
            'Product Viewed',
            'Product Added',
            'Product Removed',
            'Cart Viewed',
            'Checkout Started',
            'Checkout Step Viewed',
            'Checkout Step Completed',
            'Payment Info Entered',
            'Order Updated',
            'Order Completed',
            'Order Refunded',
            'Order Cancelled',

            // Coupon
            'Coupon Entered',
            'Coupon Applied',
            'Coupon Denied',
            'Coupon Removed',

            // Wishlist
            'Product Added to Wishlist',
            'Product Removed from Wishlist',
            'Wishlist Product Added to Cart',

            // Sharing
            'Product Shared',
            'Cart Shared',

            // Review
            'Product Reviewed',
        ];

        // TODO: Implement validation logic for other track event properties
        return true;
    }

    private function injectUserTraits(): void
    {
        // TODO: Implement user traits injection logic from user identity from clickhouse
        //        $this->data['context']['traits'];
        //        $this->data['traits'];

        $anonymousId = $this->data['anonymousId'] ?? null;
        $userId = $this->data['userId'] ?? null;

        //        if ($anonymousId) {
        //            $rows = app(\ClickHouseDB\Client::class)->select(
        //                "SELECT *
        //                 FROM v_anonymous_clusters
        //                 WHERE anonymous_id = '{$anonymousId}'"
        //            );
        //            $clusters = $rows->rows();
        //
        //            Log::emergency('clusters', [
        //                '_clusters' => $clusters,
        //                'anonymous_id' => $anonymousId,
        //                'user_id' => $userId ?? 'not set',
        //                'traits' => [
        //                    'first' => $this->data['context']['traits'] ?? 'not set',
        //                    'second' => $this->data['traits'] ?? 'not set',
        //                ],
        //            ]);
        //
        //        }

    }
}
