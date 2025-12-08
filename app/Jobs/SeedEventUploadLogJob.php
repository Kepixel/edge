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
     * The number of times the job may be attempted.
     */
    public int $tries = 20;

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

        // 3) dispatch enrichment for this single event
//        ProcessEventEnrichedJob::dispatch(
//            teamId: $this->source->team_id,
//            sourceId: $this->source->id,
//            eventName: $this->data['event'] ?? $this->data['type'] ?? 'unknown',
//            eventType: $this->data['type'] ?? 'track',
//            userId: $this->data['userId'] ?? null,
//            anonymousId: $this->data['anonymousId'] ?? null,
//            messageId: $this->data['messageId'] ?? null,
//            sessionId: $this->data['context']['sessionId'] ?? null,
//            rudderId: $this->data['rudderId'] ?? null,
//            properties: $this->data,
//            eventTimestamp: $eventTimestamp,
//        );
    }

    /**
     * Determine if the job should retry on the given exception.
     */
    public function retryUntil(): \DateTime
    {
        return now()->addMinutes(10);
    }
}
