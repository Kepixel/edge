<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use App\Jobs\ProcessMatomoRequest;
use App\Jobs\ProcessRudderRequest;
use Illuminate\Http\Request;

class EdgeAction extends Controller
{
    public function __invoke(Request $request, $path = '')
    {
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

                ProcessRudderRequest::dispatch(\Str::beforeLast($decoded, ':'), $request->all(), $headers, $path);
            }

            return response()->json([
                'ok' => true,
            ], 202);
        }
        $code = 204;
        if ($request->has('appid')) {
            //            $ip = explode(',', $request->header('X-Forwarded-For'))[0] ?? $request->ip();
            //
            //            $data = $request->all();
            //            $data['cip'] = $ip;
            //            $data['ua'] = $request->header('User-Agent');
            //
            //            $headers = [
            //                'Accept' => $request->header('Accept'),
            //                'Accept-Encoding' => $request->header('Accept-Encoding'),
            //                'Accept-Language' => $request->header('Accept-Language'),
            //                'Referer' => $request->header('Referer'),
            //                'Origin' => $request->header('Origin'),
            //                'Content-Type' => $request->header('Content-Type'),
            //                'Content-Length' => $request->header('Content-Length'),
            //                'Connection' => $request->header('Connection'),
            //                'Cookie' => $request->header('Cookie'),
            //                'x-client-ip' => $ip,
            //                'X-Forwarded-For' => $ip,
            //            ];

            // Dispatch the job to process the request in the background
            //            ProcessMatomoRequest::dispatch(
            //                request('appid'),
            //                $data,
            //                $headers,
            //                $request->header('User-Agent')
            //            );

            // Return 202 Accepted status code to indicate the request has been accepted for processing
            $code = 202;
        }

        return response(null, $code);
    }
}
