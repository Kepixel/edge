<?php

use App\Http\Controllers\UpdateController;
use Carbon\CarbonImmutable;

Route::get('/wordpress-plugin/metadata.json', [UpdateController::class, 'pluginMetadata']);

require __DIR__.'/edges.php';
