<?php

namespace App\Jobs;

use App\Events\LiveEvent;
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
        $client->setTimeout(600 * 4);
        $client->setConnectTimeOut(60 * 4);

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

        $this->source->update(['last_upload_at' => now()]);

        if ($this->source->is_live_event_enabled) {

            $channelName = 'live-sources.' . $this->source->app_token;

            $eventData = [
                'id' => uniqid(),
                'timestamp' => $this->data['receivedAt'] ?? now()->toISOString(),
                'event' => $this->data['eventName'] ?? $item['eventType'] ?? 'unknown',
                'properties' => $this->data['properties'],
                'userId' => $this->data['userId'] ?? null,
                'anonymousId' => $this->data['anonymousId'] ?? null,
                'source_id' => $this->source->id,
                'write_key' => $this->source->app_token,
            ];

            // Broadcast the event
            broadcast(new LiveEvent($channelName, $eventData));
        }
    }

    /**
     * Determine if the job should retry on the given exception.
     */
    public function retryUntil(): \DateTime
    {
        return now()->addDays(2);
    }
}
