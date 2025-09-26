<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use Illuminate\Http\Response;
use Illuminate\Support\Facades\Cache;

class AnubisJsController extends Controller
{
    public function __invoke(): Response
    {
        $file = base_path('static/cdn/anubis.js');

        $content = Cache::remember('anubis_js_content', 86400, fn () => file_get_contents($file));

        return response($content)
            ->header('Content-Type', 'application/javascript')
            ->header('Cache-Control', 'public, max-age=2592000, immutable')
            ->header('Expires', gmdate('D, d M Y H:i:s', time() + 2592000).' GMT')
            ->header('X-Accel-Expires', '2592000');
    }
}
