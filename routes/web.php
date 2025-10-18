<?php

use App\Http\Controllers\UpdateController;

Route::get('/wordpress-plugin/metadata.json', [UpdateController::class, 'pluginMetadata']);

require __DIR__.'/edges.php';
