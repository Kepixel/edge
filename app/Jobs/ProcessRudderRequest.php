<?php

namespace App\Jobs;

use App\Models\Source;
use App\Services\TeamEventUsageService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Symfony\Component\Process\Process;

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
        $this->onConnection('database');
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

        $teamsBase = config('services.teams.base_path');
        if (! $teamsBase) {
            throw new \RuntimeException('TEAMS_BASE_PATH environment variable not set');
        }

        $url = "http://localhost:8080/$path";
        $headers = $this->headers;
        $headers['authorization'] = 'Basic '.base64_encode($source->write_key.':');

        // Add a Content-Type header for JSON
//        $headers['Content-Type'] = 'application/json';
//        $headers['Accept'] = 'application/json';

        // Retry logic with exponential backoff
        $maxRetries = 3;
        $retryDelay = 100; // milliseconds

        for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
            $response = $this->executeCurlInDockerBackend($team->id, $url, $headers, $this->data);
            if ($response['success']) {
                // Request succeeded
                break;
            }

            $statusCode = $response['status_code'];
            $errorMessage = "Failed to send data to Rudder: HTTP {$statusCode}";

            Log::emergency($errorMessage, [
                'source_key' => $this->sourceKey,
                'status' => $statusCode,
                'response' => $response['response'],
                'url' => $url,
                'attempt' => $this->attempts(),
                'curl_attempt' => $attempt,
                'error' => $response['error'],
            ]);

            // If this is the last attempt or non-retryable error, handle accordingly
            if ($attempt === $maxRetries || !$this->isRetryableHttpError($statusCode)) {
                if ($this->isRetryableHttpError($statusCode)) {
                    throw new \RuntimeException($errorMessage);
                }
                // Non-retryable error, don't retry the job
                return;
            }

            // Wait before retrying
            usleep($retryDelay * 1000);
            $retryDelay *= 2; // Exponential backoff
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
            Log::emergency('Team port not found in database for source: '.$sourceId, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
            ]);

            return ['should_return' => true, 'should_retry' => false, 'message' => null, 'port' => null];
        }

        // Check if team has port in env file
        if (! $envPort) {
            Log::emergency('Team port not found in env file for source: '.$sourceId, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'db_port' => $teamPort,
            ]);

            return ['should_return' => true, 'should_retry' => false, 'message' => null, 'port' => null];
        }

        // If ports are inconsistent, update team with env port and continue with env port
        if ($teamPort !== $envPort) {
            Log::emergency('Updating team port from database to match env file for source: '.$sourceId, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'old_db_port' => $teamPort,
                'new_db_port' => $envPort,
            ]);

            // Update team port in database
            try {
                $team->update(['port' => $envPort]);
            } catch (\Exception $e) {
                $fallback = $this->recoverFromPortSyncFailure($team, $sourceId, $teamPort, $envPort, $e);

                if (! $fallback['success']) {
                    return $fallback['result'];
                }

                $envPort = $fallback['port'];
            }
            $portToUse = $envPort;
        }

        // Check if team docker containers are running
        if (! $containersRunning) {
            $message = "Team docker containers not running for source: {$sourceId}";
            Log::emergency($message, [
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
     * Attempt to recover from a database port update failure.
     */
    private function recoverFromPortSyncFailure($team, string $sourceId, ?int $teamPort, ?int $envPort, \Exception $exception): array
    {
        Log::warning('Failed to sync team port from env file; attempting automatic reassignment.', [
            'team_id' => $team->id,
            'source_id' => $sourceId,
            'database_port' => $teamPort,
            'env_port' => $envPort,
            'error' => $exception->getMessage(),
        ]);

        $newPort = $this->findAvailablePort();

        if ($newPort === null) {
            $message = 'Unable to allocate a replacement port for team after sync failure.';

            Log::emergency($message, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'database_port' => $teamPort,
                'env_port' => $envPort,
            ]);

            return [
                'success' => false,
                'result' => [
                    'should_return' => false,
                    'should_retry' => true,
                    'message' => $message,
                    'port' => $teamPort ?? $envPort,
                ],
            ];
        }

        if (! $this->updateTeamPortConfiguration($team, $newPort)) {
            $message = 'Failed to persist replacement port for team after sync failure.';

            Log::emergency($message, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'replacement_port' => $newPort,
            ]);

            return [
                'success' => false,
                'result' => [
                    'should_return' => false,
                    'should_retry' => true,
                    'message' => $message,
                    'port' => $teamPort ?? $envPort,
                ],
            ];
        }

        if (! $this->restartTeamDockerContainers($team->id)) {
            $message = 'Failed to restart docker containers after assigning replacement port.';

            Log::emergency($message, [
                'team_id' => $team->id,
                'source_id' => $sourceId,
                'replacement_port' => $newPort,
            ]);

            return [
                'success' => false,
                'result' => [
                    'should_return' => false,
                    'should_retry' => true,
                    'message' => $message,
                    'port' => $newPort,
                ],
            ];
        }

        Log::emergency('Successfully reassigned team port after sync failure.', [
            'team_id' => $team->id,
            'source_id' => $sourceId,
            'new_port' => $newPort,
        ]);

        return [
            'success' => true,
            'port' => $newPort,
        ];
    }

    /**
     * Update team database record and env file with the provided port.
     */
    private function updateTeamPortConfiguration($team, int $newPort): bool
    {
        $teamDirectory = $this->resolveTeamBaseDirectory($team->id);
        if (! $teamDirectory) {
            return false;
        }

        try {
            $team->update(['port' => $newPort]);
        } catch (\Exception $e) {
            Log::emergency('Failed to update team port in database during fallback.', [
                'team_id' => $team->id,
                'replacement_port' => $newPort,
                'error' => $e->getMessage(),
            ]);

            return false;
        }

        if (! $this->ensureTeamDirectoryExists($teamDirectory)) {
            return false;
        }

        $envFile = $teamDirectory.'/.env';

        if (file_exists($envFile)) {
            $envContent = file_get_contents($envFile);

            if ($envContent === false) {
                Log::emergency('Unable to read team env file for port update.', [
                    'team_id' => $team->id,
                    'path' => $envFile,
                ]);

                return false;
            }

            $updatedEnv = preg_replace('/^BACKEND_PORT=.*/m', 'BACKEND_PORT='.$newPort, $envContent, -1, $count);

            if ($count === 0) {
                $normalized = rtrim($envContent, "\r\n");
                $updatedEnv = $normalized.PHP_EOL.'BACKEND_PORT='.$newPort.PHP_EOL;
            }

            if (file_put_contents($envFile, $updatedEnv) === false) {
                Log::emergency('Unable to write updated port to team env file.', [
                    'team_id' => $team->id,
                    'path' => $envFile,
                ]);

                return false;
            }
        } else {
            $env = [
                'TEAM='.$team->id,
                'WORKSPACE_TOKEN='.$team->id,
                'WORKSPACE_CONFIG_PATH='.$teamDirectory.'/config.json',
                'BACKEND_PORT='.$newPort,
            ];

            if (file_put_contents($envFile, implode(PHP_EOL, $env).PHP_EOL) === false) {
                Log::emergency('Unable to create team env file during port update.', [
                    'team_id' => $team->id,
                    'path' => $envFile,
                ]);

                return false;
            }
        }

        return true;
    }

    /**
     * Restart docker containers for a team to apply the new port.
     */
    private function restartTeamDockerContainers(string $teamId): bool
    {
        $teamDirectory = $this->resolveTeamBaseDirectory($teamId);
        if (! $teamDirectory || ! is_dir($teamDirectory)) {
            Log::warning('Team directory missing when attempting docker restart.', [
                'team_id' => $teamId,
                'directory' => $teamDirectory,
            ]);

            return false;
        }

        $command = sprintf(
            'docker compose --project-directory %s --project-name %s up -d --remove-orphans --force-recreate',
            escapeshellarg($teamDirectory),
            escapeshellarg($teamId)
        );

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(280);

        try {
            $process->run();
        } catch (\Throwable $throwable) {
            Log::emergency('Docker restart command failed to execute.', [
                'team_id' => $teamId,
                'directory' => $teamDirectory,
                'error' => $throwable->getMessage(),
            ]);

            return false;
        }

        if (! $process->isSuccessful()) {
            Log::emergency('Docker restart command exited with failure.', [
                'team_id' => $teamId,
                'directory' => $teamDirectory,
                'exit_code' => $process->getExitCode(),
                'stderr' => $process->getErrorOutput(),
                'stdout' => $process->getOutput(),
            ]);

            return false;
        }

        Log::emergency('Docker containers restarted to apply new team port.', [
            'team_id' => $teamId,
            'directory' => $teamDirectory,
        ]);

        return true;
    }

    /**
     * Resolve the base directory for a team.
     */
    private function resolveTeamBaseDirectory(string $teamId): ?string
    {
        $base = config('services.teams.base_path');
        if (! $base) {
            Log::emergency('TEAMS_PATH environment variable not set');

            return null;
        }

        return rtrim($base, '/').'/'.$teamId;
    }

    /**
     * Ensure the team directory exists on disk.
     */
    private function ensureTeamDirectoryExists(string $directory): bool
    {
        if (is_dir($directory)) {
            return true;
        }

        if (@mkdir($directory, 0775, true) || is_dir($directory)) {
            return true;
        }

        Log::emergency('Unable to create team directory for port update.', [
            'directory' => $directory,
        ]);

        return false;
    }

    /**
     * Find an available port randomly within the default range.
     */
    private function findAvailablePort(int $start = 20000, int $end = 30000): ?int
    {
        if ($start > $end) {
            return null;
        }

        $checked = [];
        $total = ($end - $start) + 1;

        while (count($checked) < $total) {
            $port = random_int($start, $end);

            if (isset($checked[$port])) {
                continue;
            }

            $checked[$port] = true;

            if ($this->isPortAvailable($port)) {
                return $port;
            }
        }

        return null;
    }

    /**
     * Determine if a port is available for use.
     */
    private function isPortAvailable(int $port): bool
    {
        $socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if ($socket === false) {
            return false;
        }

        $result = @socket_bind($socket, '127.0.0.1', $port);
        @socket_close($socket);

        return $result;
    }

    /**
     * Get the port from team's .env file
     */
    private function getPortFromEnvFile($team): ?int
    {
        $base = $this->resolveTeamBaseDirectory($team->id);
        if (! $base) {
            return null;
        }

        $envFile = "{$base}/.env";

        if (! file_exists($envFile)) {
            Log::emergency("Env file not found for team: {$team->id} base: $base");
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

    /**
     * Execute curl command inside Docker backend service
     */
    private function executeCurlInDockerBackend(string $teamId, string $url, array $headers, array $data): array
    {
        // Prepare headers for curl
        $curlHeaders = [];
        foreach ($headers as $key => $value) {
            $curlHeaders[] = sprintf('%s: %s', $key, $value);
        }

        $jsonData = json_encode($data);

        // Create a temporary file to store the JSON data to avoid shell escaping issues
        $tempFile = tempnam(sys_get_temp_dir(), 'curl_data_');
        file_put_contents($tempFile, $jsonData);

        try {
            // First check if the Docker container exists and is running
            $checkCommand = sprintf('docker ps -q -f name=%s-backend-1', escapeshellarg($teamId));
            $checkProcess = Process::fromShellCommandline($checkCommand);
            $checkProcess->setTimeout(10);

            try {
                $checkProcess->run();
                if (!$checkProcess->isSuccessful() || trim($checkProcess->getOutput()) === '') {
                    Log::emergency('Docker container not found or not running.', [
                        'team_id' => $teamId,
                    ]);

                    return [
                        'success' => false,
                        'status_code' => 0,
                        'response' => '',
                        'error' => 'Docker container not available',
                        'time_total' => 0
                    ];
                }
            } catch (\Throwable $throwable) {
                Log::emergency('Failed to check Docker container status.', [
                    'team_id' => $teamId,
                    'error' => $throwable->getMessage(),
                ]);

                return [
                    'success' => false,
                    'status_code' => 0,
                    'response' => '',
                    'error' => 'Failed to check container status: ' . $throwable->getMessage(),
                    'time_total' => 0
                ];
            }

            // Build the docker exec command with file input instead of inline JSON
            $dockerArgs = [
                'docker', 'exec', '-i', $teamId . '-backend-1', 'sh', '-c',
                sprintf(
                    'curl -X POST %s --data-binary @- --connect-timeout 30 --max-time 30 -w "%%{http_code}|%%{time_total}" -s %s',
                    implode(' ', array_map(function($header) {
                        return '-H ' . escapeshellarg($header);
                    }, $curlHeaders)),
                    escapeshellarg($url)
                )
            ];

            // Execute the command with JSON data piped from the temp file
            $dockerCommand = sprintf(
                '%s < %s',
                implode(' ', array_map('escapeshellarg', $dockerArgs)),
                escapeshellarg($tempFile)
            );

            Log::emergency('Executing docker command', [
                'team_id' => $teamId,
                'command' => $dockerCommand,
            ]);

            $process = Process::fromShellCommandline($dockerCommand);
            $process->setTimeout(60);

            try {
                $process->run();
            } catch (\Throwable $throwable) {
                Log::emergency('Docker curl command failed to execute.', [
                    'team_id' => $teamId,
                    'url' => $url,
                    'error' => $throwable->getMessage(),
                ]);

                return [
                    'success' => false,
                    'status_code' => 0,
                    'response' => '',
                    'error' => $throwable->getMessage(),
                    'time_total' => 0
                ];
            }

            if (!$process->isSuccessful()) {
                Log::emergency('Docker curl command exited with failure.', [
                    'team_id' => $teamId,
                    'url' => $url,
                    'exit_code' => $process->getExitCode(),
                    'stderr' => $process->getErrorOutput(),
                    'stdout' => $process->getOutput(),
                ]);

                return [
                    'success' => false,
                    'status_code' => 0,
                    'response' => $process->getErrorOutput(),
                    'error' => 'Docker command failed',
                    'time_total' => 0
                ];
            }

            $output = trim($process->getOutput());

            // Parse the output - curl returns response body followed by status code and time
            $parts = explode('|', $output);
            if (count($parts) >= 2) {
                $statusAndTime = array_pop($parts);
                $statusTimeParts = explode('|', $statusAndTime);
                $statusCode = (int) ($statusTimeParts[0] ?? 0);
                $timeTotal = (float) ($statusTimeParts[1] ?? 0);
                $responseBody = implode('|', $parts);
            } else {
                // Fallback parsing
                $statusCode = 0;
                $timeTotal = 0;
                $responseBody = $output;
            }

            return [
                'success' => $statusCode >= 200 && $statusCode < 300,
                'status_code' => $statusCode,
                'response' => $responseBody,
                'error' => null,
                'time_total' => $timeTotal
            ];

        } finally {
            // Clean up the temporary file
            if (file_exists($tempFile)) {
                unlink($tempFile);
            }
        }
    }
}
