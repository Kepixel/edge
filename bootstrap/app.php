<?php

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;
use Illuminate\Foundation\Configuration\Middleware;

return Application::configure(basePath: dirname(__DIR__))
    ->withRouting(
        web: __DIR__.'/../routes/web.php',
        commands: __DIR__.'/../routes/console.php',
        channels: __DIR__.'/../routes/channels.php',
        health: '/up',
    )
    ->withMiddleware(function (Middleware $middleware): void {
        $middleware->encryptCookies();

        $middleware->trustProxies(at: ['*']);

        $middleware->validateCsrfTokens(except: [
            'https://config.kepixel.com/*',
            'http://config.kepixel.com/*',
            'https://edge.kepixel.com/*',
            'http://edge.kepixel.com/*',
            'v1/*',
        ]);
    })
    ->withExceptions(function (Exceptions $exceptions): void {
        //
    })->create();
