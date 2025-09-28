<?php

namespace App\Events;

use Illuminate\Broadcasting\InteractsWithSockets;
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcast;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

class LiveEvent implements ShouldBroadcast
{
    use Dispatchable, InteractsWithSockets, SerializesModels;

    public $channelName;

    public $eventData;

    /**
     * Create a new event instance.
     */
    public function __construct($channelName, $eventData)
    {
        $this->channelName = $channelName;
        $this->eventData = $eventData;
    }

    /**
     * Get the channels the event should broadcast on.
     *
     * @return array<int, \Illuminate\Broadcasting\Channel>
     */
    public function broadcastOn(): array
    {
        return [
            new PrivateChannel($this->channelName),
        ];
    }

    /**
     * The event's broadcast name.
     */
    //    public function broadcastAs(): string
    //    {
    //        return 'live-event';
    //    }

    /**
     * Get the data to broadcast.
     */
    public function broadcastWith(): array
    {
        return $this->eventData;
    }
}
