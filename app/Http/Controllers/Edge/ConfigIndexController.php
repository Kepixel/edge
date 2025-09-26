<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use Illuminate\Http\JsonResponse;

class ConfigIndexController extends Controller
{
    public function __invoke(): JsonResponse
    {
        return response()->json([
            'components' => [
                [
                    'name' => 'server',
                    'features' => ['gzip-req-payload'],
                ],
            ],
        ]);
    }
}
