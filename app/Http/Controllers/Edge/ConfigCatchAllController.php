<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use Illuminate\Http\Response;
use Illuminate\Support\Facades\Log;

class ConfigCatchAllController extends Controller
{
    public function __invoke(string $any): Response
    {
        Log::emergency('Invalid request to config.kepixel.com: '.$any);

        return response(null, 204);
    }
}
