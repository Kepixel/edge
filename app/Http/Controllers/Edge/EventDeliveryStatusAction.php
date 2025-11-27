<?php

namespace App\Http\Controllers\Edge;

use App\Events\LiveEvent;
use App\Http\Controllers\Controller;
use App\Models\Destination;
use App\Models\Source;
use ClickHouseDB\Client;
use Illuminate\Http\Request;

class EventDeliveryStatusAction extends Controller
{
    public function __invoke(Request $request)
    {
        $data = $request->all();

        // Collect all unique destination and source IDs upfront to avoid N+1
        $destinationIds = [];
        $sourceIds = [];
        foreach ($data as $key => $items) {
            if ($key === 'version') {
                continue;
            }
            foreach ($items as $item) {
                if (isset($item['destinationId'])) {
                    $destinationIds[] = $item['destinationId'];
                }
                if (isset($item['sourceId'])) {
                    $sourceIds[] = $item['sourceId'];
                }
            }
        }

        // Eager load all destinations and sources in single queries
        $destinations = Destination::whereIn('id', array_unique($destinationIds))->get()->keyBy('id');
        $sources = Source::whereIn('id', array_unique($sourceIds))->get()->keyBy('id');

        // Track destinations to batch update at the end
        $destinationUpdates = [];

        foreach ($data as $key => $items) {
            if ($key === 'version') {
                continue;
            }

            // Process each delivery status item for this destination
            foreach ($items as $item) {
                $destination = $destinations->get($item['destinationId']);
                if (! $destination) {
                    continue;
                }

                // Map jobState to our status format
                $status = match ($item['jobState'] ?? '') {
                    'succeeded' => 'success',
                    'aborted' => 'failed',
                    'waiting' => 'dropped',
                    default => 'failed'
                };

                // Get source information
                $sourceId = $item['sourceId'] ?? null;
                $source = $sourceId ? $sources->get($sourceId) : null;

                // Organize event data for broadcasting
                $eventData = [
                    'id' => uniqid(),
                    'timestamp' => $item['sentAt'] ?? now()->toISOString(),
                    'event' => $item['eventName'] ?? 'unknown',
                    'properties' => [
                        'delivery_status' => [
                            'destination_id' => $destination->id,
                            'source_id' => $sourceId,
                            'attempt_number' => $item['attemptNum'] ?? 1,
                            'error_code' => $item['errorCode'] ?? null,
                            'error_response' => $item['errorResponse'] ?? null,
                            'endpoint' => $item['payload']['endpoint'] ?? null,
                            'method' => $item['payload']['method'] ?? null,
                        ],
                        'original_payload' => $item['payload'] ?? [],
                    ],
                    'userId' => $item['payload']['userId'] ?? null,
                    'anonymousId' => $item['payload']['anonymousId'] ?? null,
                    'status' => $status,
                    'source_id' => $sourceId,
                    'source_name' => $source?->name ?? 'Unknown Source',
                    'event_type' => $item['eventType'] ?? 'track',
                ];

                // Broadcast to destination-specific channel
                $channelName = 'live-destinations.'.$destination->id;
//                broadcast(new LiveEvent($channelName, $eventData));

                // Persist event delivery log for daily reporting
                // Ensure payload is properly handled to prevent array-to-string conversion errors
                $payload = $item['payload'] ?? null;

                // Handle payload conversion to ensure it's properly formatted for database storage
                if (is_string($payload)) {
                    // If payload is a JSON string, decode it first
                    $decodedPayload = json_decode($payload, true);
                    $payload = $decodedPayload !== null ? $decodedPayload : $payload;
                } elseif (is_array($payload)) {
                    // If payload is already an array, ensure it can be properly JSON encoded
                    // This handles complex nested arrays that might cause conversion issues
                    $payload = json_decode(json_encode($payload), true);
                }

                app(Client::class)->insert(
                    'event_delivery_logs',
                    [
                        [
                            $destination->team_id,
                            $destination->id,
                            $sourceId,
                            $item['eventName'] ?? $item['eventType'] ?? 'unknown',
                            $item['eventType'] ?? 'track',
                            $status,
                            $item['attemptNum'] ?? 1,
                            $item['errorCode'] ?? null,
                            $item['errorResponse'] ?? null,
                            $item['payload']['endpoint'] ?? null,
                            $item['payload']['method'] ?? null,
                            $item['payload']['userId'] ?? null,
                            $item['payload']['anonymousId'] ?? null,
                            $item['payload']['messageId'] ?? null,
                            $item['payload']['rudderId'] ?? null,
                            json_encode($payload),
                            isset($item['sentAt']) ? \Carbon\Carbon::parse($item['sentAt'])->format('Y-m-d H:i:s') : now()->format('Y-m-d H:i:s'),
                            now()->toDateTimeString(),
                        ],
                    ],
                    ['team_id', 'destination_id', 'source_id', 'event_name', 'event_type', 'status', 'attempt_number', 'error_code', 'error_response', 'endpoint', 'method', 'user_id', 'anonymous_id', 'message_id', 'rudder_id', 'payload', 'event_timestamp', 'created_at']
                );

                // Track destination update for batch processing
                $lastDeliveryAt = $item['sentAt'] ?? now();
                if (! isset($destinationUpdates[$destination->id]) || $lastDeliveryAt > $destinationUpdates[$destination->id]) {
                    $destinationUpdates[$destination->id] = $lastDeliveryAt;
                }
            }
        }

        // Batch update all destination last_delivery_at timestamps
        foreach ($destinationUpdates as $destinationId => $lastDeliveryAt) {
            Destination::where('id', $destinationId)->update(['last_delivery_at' => $lastDeliveryAt]);
        }
    }
}
