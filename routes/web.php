<?php

use App\Http\Controllers\UpdateController;

require __DIR__.'/edges.php';

Route::get('/wordpress-plugin/metadata.json', [UpdateController::class, 'pluginMetadata']);
