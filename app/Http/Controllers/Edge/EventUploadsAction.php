<?php

namespace App\Http\Controllers\Edge;

use App\Events\LiveEvent;
use App\Http\Controllers\Controller;
use App\Models\Source;
use ClickHouseDB\Client;
use Illuminate\Http\Request;

class EventUploadsAction extends Controller
{
    public function __invoke(Request $request)
    {
        $data = $request->all();

        foreach ($data as $writeKey => $items) {
            if ($writeKey === 'version') {
                continue;
            }
            $source = Source::where('write_key', $writeKey)->first(['id', 'team_id', 'write_key', 'app_token']);
            if (! $source) {
                continue;
            }

            // Broadcast each event to the channel based on app_token
            foreach ($items as $item) {
                $channelName = 'live-sources.'.$source->app_token;

                // Prepare event data for broadcasting
                $payload = $item['payload'] ?? [];

                if (is_string($payload)) {
                    $decodedPayload = json_decode($payload, true);
                    $payload = $decodedPayload !== null ? $decodedPayload : $payload;
                } elseif (is_array($payload)) {
                    $payload = json_decode(json_encode($payload), true);
                }

                $eventData = [
                    'id' => uniqid(),
                    'timestamp' => $item['receivedAt'] ?? now()->toISOString(),
                    'event' => $item['eventName'] ?? $item['eventType'] ?? 'unknown',
                    'properties' => $payload,
                    'userId' => $item['payload']['userId'] ?? null,
                    'anonymousId' => $item['payload']['anonymousId'] ?? null,
                    'source_id' => $source->id,
                    'write_key' => $writeKey,
                ];

                // Broadcast the event
                broadcast(new LiveEvent($channelName, $eventData));

                // Persist the event for reporting and debugging
                app(Client::class)->insert(
                    'event_upload_logs',
                    [
                        [
                            $source->team_id,
                            $source->id,
                            $item['eventName'] ?? $item['eventType'] ?? 'unknown',
                            $item['eventType'] ?? 'track',
                            $item['payload']['userId'] ?? null,
                            $item['payload']['anonymousId'] ?? null,
                            $item['payload']['messageId'] ?? null,
                            $item['payload']['rudderId'] ?? null,
                            json_encode($payload),
                            isset($item['receivedAt']) ? \Carbon\Carbon::parse($item['receivedAt'])->format('Y-m-d H:i:s') : now()->format('Y-m-d H:i:s'),
                            now()->toDateTimeString(),
                        ],
                    ],
                    ['team_id', 'source_id', 'event_name', 'event_type', 'user_id', 'anonymous_id', 'message_id', 'rudder_id', 'properties', 'event_timestamp', 'created_at']
                );

                // Update source's last upload timestamp
                $source->update([
                    'last_upload_at' => $item['receivedAt'] ?? now(),
                ]);
            }
        }

    }
}
