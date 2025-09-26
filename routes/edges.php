<?php

use App\Http\Controllers\Edge\AnubisIndexController;
use App\Http\Controllers\Edge\AnubisJsController;
use App\Http\Controllers\Edge\ConfigCatchAllController;
use App\Http\Controllers\Edge\ConfigIndexController;
use App\Http\Controllers\Edge\EdgeAction;
use App\Http\Controllers\Edge\EdgeOptionsController;
use App\Http\Controllers\Edge\EventDeliveryStatusAction;
use App\Http\Controllers\Edge\EventUploadsAction;
use App\Http\Controllers\Edge\SourceConfigController;
use Illuminate\Support\Facades\Route;

Route::domain('edge.kepixel.com')->group(function () {
    // Serve anubis.js with long cache duration for high traffic optimization
    Route::get('/anubis.js', AnubisJsController::class);

    Route::options('/{any}', EdgeOptionsController::class)->where('any', '.*');
    Route::any('/', EdgeAction::class)->name('edge');
    Route::any('/{path}', EdgeAction::class)->where('path', '.*')->name('edge.path');
});

Route::domain('source-config.kepixel.com')->group(function () {
    Route::get('sourceConfig', SourceConfigController::class);
});

Route::domain('config.kepixel.com')->group(function () {
    Route::post('/dataplane/v2/eventUploads', EventUploadsAction::class);
    Route::post('/dataplane/v2/eventDeliveryStatus', EventDeliveryStatusAction::class);

    Route::get('/', ConfigIndexController::class);

    Route::any('{any}', ConfigCatchAllController::class)->where('any', '.*');
});

Route::domain('anubis.kepixel.com')->group(function () {
    Route::get('/', AnubisIndexController::class)->name('anubis.index');
});
