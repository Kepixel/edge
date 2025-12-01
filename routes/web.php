<?php

use App\Http\Controllers\UpdateController;

Route::get('events/{sourceKey}.csv', function (string $sourceKey) {

});

Route::get('/wordpress-plugin/metadata.json', [UpdateController::class, 'pluginMetadata']);

require __DIR__.'/edges.php';
