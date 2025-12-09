<?php

namespace App\Jobs;

use Carbon\Carbon;
use ClickHouseDB\Client;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

class SeedEventUploadLogJob implements ShouldQueue
{
    use Queueable;

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
        $client = app(Client::class);

        $eventTimestamp = isset($this->data['sentAt'])
            ? Carbon::parse($this->data['sentAt'])->format('Y-m-d H:i:s')
            : now()->format('Y-m-d H:i:s');

        // 1) insert raw event
        $client->insert(
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
                    $eventTimestamp,
                    now()->toDateTimeString(),
                ],
            ],
            [
                'team_id',
                'source_id',
                'event_name',
                'event_type',
                'user_id',
                'anonymous_id',
                'message_id',
                'session_id',
                'rudder_id',
                'properties',
                'event_timestamp',
                'created_at',
            ]
        );

        // 2) update source
        $this->source->update([
            'last_upload_at' => $this->data['sentAt'] ?? now(),
        ]);
    }

    /**
     * Determine if the job should retry on the given exception.
     */
    public function retryUntil(): \DateTime
    {
        return now()->addDays(2);
    }
}
