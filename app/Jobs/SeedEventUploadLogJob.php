<?php

namespace App\Jobs;

use Carbon\Carbon;
use ClickHouseDB\Client;
use ClickHouseDB\Exception\QueryException;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

class SeedEventUploadLogJob implements ShouldQueue
{
    use Queueable;

    /**
     * The number of times the job may be attempted.
     */
    public int $tries = 5;

    /**
     * The number of seconds to wait before retrying the job.
     */
    public array $backoff = [5, 15, 30, 60, 120];

    /**
     * Create a new job instance.
     */
    public function __construct(public $source, public array $data)
    {
        //
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        // Persist the event for reporting and debugging
        app(Client::class)->insert(
            'event_upload_logs',
            [
                [
                    $this->source->team_id,
                    $this->source->id,
                    $this->data['event'] ?? $this->data['type'] ?? 'unknown',
                    $this->data['type'] ?? 'track',
                    $this->data['userId'] ?? null,
                    $this->data['anonymousId'] ?? null,
                    $this->data['messageId'] ?? null,
                    $this->data['context']['sessionId'] ?? null,
                    $this->data['rudderId'] ?? null,
                    json_encode($this->data),
                    isset($item['sentAt']) ? Carbon::parse($item['sentAt'])->format('Y-m-d H:i:s') : now()->format('Y-m-d H:i:s'),
                    now()->toDateTimeString(),
                ],
            ],
            ['team_id', 'source_id', 'event_name', 'event_type', 'user_id', 'anonymous_id', 'message_id', 'rudder_id', 'session_id', 'properties', 'event_timestamp', 'created_at']
        );

        // Update source's last upload timestamp
        $this->source->update([
            'last_upload_at' => $item['sentAt'] ?? now(),
        ]);
    }

    /**
     * Determine if the job should retry on the given exception.
     */
    public function retryUntil(): \DateTime
    {
        return now()->addMinutes(10);
    }
}
