<?php

namespace App\Jobs;

use App\Models\Source;
use App\Services\TeamEventUsageService;
use ClickHouseDB\Client;
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
        if (!$source) {
            Log::emergency('Source not found for token: ' . $this->sourceKey);
            return;
        }

        $team = $source->team;

        if (!$team) {
            Log::emergency('Team not found for source: ' . $source->id);
            return;
        }


        if ($this->isTrackEvent($this->path)) {
            $sessionId = (string)($this->data['context']['sessionId'] ?? null);
            $anonymousId = (string)($this->data['anonymousId'] ?? null);

            $client = app(Client::class);

            $row = $client->select(
                '
    SELECT *
    FROM event_upload_logs
    WHERE session_id = :sessionId
      AND anonymous_id = :anonymousId
      AND event_name = :eventName
    ORDER BY event_timestamp ASC
    LIMIT 1
    ',
                [
                    'sessionId' => $sessionId,
                    'anonymousId' => $anonymousId,
                    'eventName' => 'page',
                ]
            )->fetchOne();




            if ($row) {
                $oldProperties = $row['properties'];
                $oldProperties = json_decode($oldProperties, true);
                $firstPageView = $oldProperties['properties'];

                $this->data["referrer"] = $firstPageView["referrer"] ?? null;
                $this->data["referring_domain"] = $firstPageView["referring_domain"] ?? null;
                $this->data["initial_referrer"] = $firstPageView["initial_referrer"] ?? null;
                $this->data["initial_referring_domain"] = $firstPageView["initial_referring_domain"] ?? null;


                $this->data['context']['campaign'] = $oldProperties['context']['campaign'];


                $contextPage = $this->data['context']['page'];

                $contextPage["referrer"] = $firstPageView["referrer"] ?? null;
                $contextPage["referring_domain"] = $firstPageView["referring_domain"] ?? null;
                $contextPage["initial_referrer"] = $firstPageView["initial_referrer"] ?? null;
                $contextPage["initial_referring_domain"] = $firstPageView["initial_referring_domain"] ?? null;

                $this->data['context']['page'] = $contextPage;
            }
        }

        if ($team->is_delivery_suspended) {
            SeedEventUploadLogJob::dispatch($source, $this->data);
            return;
        }

        $url = "http://localhost:8080/$this->path";
        $headers = $this->headers;
        $headers['authorization'] = 'Basic ' . base64_encode($source->write_key . ':');

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
            Log::error('Connection error processing Rudder request: ' . $e->getMessage(), [
                'source_key' => $this->sourceKey,
                'url' => $url,
                'attempt' => $this->attempts(),
                'exception' => $e,
            ]);

            throw $e;
        } catch (\Exception $e) {
            // For other exceptions, log and don't retry unless it's clearly retryable
            Log::error('Unexpected error processing Rudder request: ' . $e->getMessage(), [
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


    private function isTrackEvent($path): bool
    {
        return $path === 'v1/t' || $path === 'v1/track';
    }
}
