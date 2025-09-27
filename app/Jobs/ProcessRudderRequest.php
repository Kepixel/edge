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
        $this->onQueue('high');
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        $source = Source::where('app_token', $this->sourceKey)->with('team')->first();
        if (! $source) {
            return;
        }

        $team = $source->team;

        if (! $team) {
            return;
        }

//        $usageService = app(TeamEventUsageService::class);
//        $currentMonth = now()->startOfMonth();
//
//        if ($usageService->limitReachedRecently($team, $currentMonth)) {
//            Log::warning('Team already exceeded monthly events quota (cached); dropping Rudder request.', [
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
//            Log::warning('Team reached the monthly events quota; dropping Rudder request.', [
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
        $validationResult = $this->validateTeamConfiguration($team, $source->id);
        if ($validationResult['should_return']) {
            return;
        }
        if ($validationResult['should_retry']) {
            throw new \RuntimeException($validationResult['message']);
        }

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

        $port = $validationResult['port'];
        $url = "http://localhost:$port/$path";
        $headers = $this->headers;
        $headers['authorization'] = 'Basic '.base64_encode($source->write_key.':');

        try {
            $response = Http::asJson()->acceptJson()->withoutVerifying()
                ->retry(3, 100)
                ->timeout(30)
                ->withHeaders($headers)
                ->post($url, $this->data);

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
     * Validate team configuration including port in DB, env file, and container status
     */
    private function validateTeamConfiguration($team, string $sourceId): array
    {
        $teamPort = $team->port;
        $envPort = $this->getPortFromEnvFile($team);
        $containersRunning = $this->isTeamContainerRunning($team->id);
        $portToUse = $teamPort;

        // Check if team has port in database
        if (! $teamPort) {
            Log::warning('Team port not found in database for source: '.$sourceId, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
            ]);

            return ['should_return' => true, 'should_retry' => false, 'message' => null, 'port' => null];
        }

        // Check if team has port in env file
        if (! $envPort) {
            Log::warning('Team port not found in env file for source: '.$sourceId, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'db_port' => $teamPort,
            ]);

            return ['should_return' => true, 'should_retry' => false, 'message' => null, 'port' => null];
        }

        // If ports are inconsistent, update team with env port and continue with env port
        if ($teamPort !== $envPort) {
            Log::info('Updating team port from database to match env file for source: '.$sourceId, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'old_db_port' => $teamPort,
                'new_db_port' => $envPort,
            ]);

            // Update team port in database
            $team->update(['port' => $envPort]);
            $portToUse = $envPort;
        }

        // Check if team docker containers are running
        if (! $containersRunning) {
            $message = "Team docker containers not running for source: {$sourceId}";
            Log::warning($message, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'port' => $portToUse,
                'attempt' => $this->attempts(),
            ]);

            // This is a retryable failure - infrastructure issue
            return ['should_return' => false, 'should_retry' => true, 'message' => $message, 'port' => $portToUse];
        }

        return ['should_return' => false, 'should_retry' => false, 'message' => null, 'port' => $portToUse];
    }

    /**
     * Get the port from team's .env file
     */
    private function getPortFromEnvFile($team): ?int
    {
        $base = base_path("../../teams/{$team->id}");
        $envFile = "{$base}/.env";

        if (! file_exists($envFile)) {
            return null;
        }

        $envContent = file_get_contents($envFile);
        if (preg_match('/^BACKEND_PORT=(\d+)$/m', $envContent, $matches)) {
            return (int) $matches[1];
        }

        return null;
    }

    /**
     * Check if Docker containers are running for a team
     */
    private function isTeamContainerRunning(string $teamId): bool
    {
        $cmd = sprintf('docker compose -p %s ps -q', escapeshellarg($teamId));
        $out = shell_exec($cmd) ?? '';

        return trim($out) !== '';
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
