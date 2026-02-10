<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use App\Jobs\ProcessRudderRequest;
use Illuminate\Http\Request;

class EdgeAction extends Controller
{
    private static ?array $eventSchemaCache = null;

    public function __invoke(Request $request, $path = '')
    {
        return redirect('https://kepixel.com');
        if ($path) {
            $authHeader = $request->header('Authorization');

            if ($authHeader && str_starts_with($authHeader, 'Basic ')) {
                $base64Credentials = substr($authHeader, 6);
                $decoded = base64_decode($base64Credentials);

                $ip = explode(',', $request->header('X-Forwarded-For'))[0] ?? $request->ip();

                $headers = [
                    'Accept' => $request->header('Accept'),
                    'Accept-Encoding' => $request->header('Accept-Encoding'),
                    'Accept-Language' => $request->header('Accept-Language'),
                    'Referer' => $request->header('Referer'),
                    'Origin' => $request->header('Origin'),
                    'Content-Type' => $request->header('Content-Type'),
                    'Content-Length' => $request->header('Content-Length'),
                    'Connection' => $request->header('Connection'),
                    'Cookie' => $request->header('Cookie'),
                    'x-client-ip' => $ip,
                    'X-Forwarded-For' => $ip,
                    'anonymousid' => $request->header('anonymousid'),
                    'User-Agent' => $request->header('User-Agent'),
                ];

                $paths = [
                    'v1/i' => 'v1/identify',
                    'v1/t' => 'v1/track',
                    'v1/p' => 'v1/page',
                    'v1/s' => 'v1/screen',
                    'v1/g' => 'v1/group',
                    'v1/a' => 'v1/alias',
                    'v1/b' => 'v1/batch',
                ];

                $path = $paths[$path] ?? $path;

                ProcessRudderRequest::dispatch(\Str::beforeLast($decoded, ':'), $request->all(), $headers, $path);
            }
        }

        return response(null, 204);
    }
}
