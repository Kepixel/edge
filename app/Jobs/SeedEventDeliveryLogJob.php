<?php

namespace App\Jobs;

use Carbon\Carbon;
use ClickHouseDB\Client;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Kepixel\Core\Models\Destination;

class SeedEventDeliveryLogJob implements ShouldQueue
{
    use Queueable;

    /**
     * Create a new job instance.
     */
    public function __construct(
        public string $teamId,
        public string $destinationId,
        public ?string $sourceId,
        public array $item
    ) {}

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        $item = $this->item;

        // Map jobState to our status format
        $status = match ($item['jobState'] ?? '') {
            'succeeded' => 'success',
            'aborted' => 'failed',
            'waiting' => 'dropped',
            default => 'failed'
        };

        // Handle payload conversion
        $payload = $item['payload'] ?? null;
        if (is_string($payload)) {
            $decodedPayload = json_decode($payload, true);
            $payload = $decodedPayload !== null ? $decodedPayload : $payload;
        } elseif (is_array($payload)) {
            $payload = json_decode(json_encode($payload), true);
        }
        $client = app(Client::class);
        $client->setTimeout(600 * 4);
        $client->setConnectTimeOut(60 * 4);
        $client->insert(
            'event_delivery_logs',
            [
                [
                    $this->teamId,
                    $this->destinationId,
                    $this->sourceId,
                    $item['eventName'] ?? $item['eventType'] ?? 'unknown',
                    $item['eventType'] ?? 'track',
                    $status,
                    $item['attemptNum'] ?? 1,
                    is_array($item['errorCode'] ?? null) ? json_encode($item['errorCode']) : ($item['errorCode'] ?? null),
                    is_array($item['errorResponse'] ?? null) ? json_encode($item['errorResponse']) : ($item['errorResponse'] ?? null),
                    $item['payload']['endpoint'] ?? null,
                    $item['payload']['method'] ?? null,
                    $item['payload']['userId'] ?? null,
                    $item['payload']['anonymousId'] ?? null,
                    $item['payload']['messageId'] ?? null,
                    $item['payload']['rudderId'] ?? null,
                    json_encode($payload),
                    isset($item['sentAt']) ? Carbon::parse($item['sentAt'])->format('Y-m-d H:i:s') : now()->format('Y-m-d H:i:s'),
                    now()->toDateTimeString(),
                ],
            ],
            ['team_id', 'destination_id', 'source_id', 'event_name', 'event_type', 'status', 'attempt_number', 'error_code', 'error_response', 'endpoint', 'method', 'user_id', 'anonymous_id', 'message_id', 'rudder_id', 'payload', 'event_timestamp', 'created_at']
        );

        Destination::where('id', $this->destinationId)->update(['last_delivery_at' => now()]);
    }

    /**
     * Determine if the job should retry on the given exception.
     */
    public function retryUntil(): \DateTime
    {
        return now()->addDays(10);
    }
}
