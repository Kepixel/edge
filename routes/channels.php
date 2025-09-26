<?php

use App\Broadcasting\LiveDestinationsChannel;
use App\Broadcasting\LiveSourcesChannel;
use App\Broadcasting\UserChannel;
use Illuminate\Support\Facades\Broadcast;

Broadcast::channel('App.Models.User.{id}', UserChannel::class);

// Live events channel authorization based on destination id
Broadcast::channel('live-destinations.{id}', LiveDestinationsChannel::class);

// Live events channel authorization based on source app_token
Broadcast::channel('live-sources.{app_token}', LiveSourcesChannel::class);
