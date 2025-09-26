<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use App\Models\Source;
use Illuminate\Support\Facades\Cache;
use MatthiasMullie\Minify;

class AnubisIndexController extends Controller
{
    public function __invoke()
    {
        $sourceKey = request()->query('appId', request()->query('writeKey'));

        $source = Cache::remember('source_data_'.$sourceKey, 86400, function () use ($sourceKey) {
            return Source::where('app_token', $sourceKey)->first(['id', 'app_token', 'tag_id', 'type']);
        });

        if (! $source) {
            return redirect('https://kepixel.com');
        }

        $tagId = is_object($source) ? $source->tag_id : null;

        $configJs = 'window.kepixelSourceType = '.json_encode($source->type).";\n";
        $configJs .= 'window.kepixelSourceKey = '.json_encode($sourceKey).";\n";
        if (! empty($tagId)) {
            $configJs .= 'window.kepixelTagId = '.json_encode($tagId).";\n";
        }

        $eventValidator = file_get_contents(base_path('static/cdn/event-validator.js'));
        $urlParams = file_get_contents(base_path('static/cdn/url-params.js'));

        $trackerContent = file_get_contents(base_path('static/cdn/tracker.js'));

        $js = $eventValidator;
        $js .= PHP_EOL;
        $js .= $urlParams;
        $js .= PHP_EOL;
        $js .= $configJs.$trackerContent;
        $js .= PHP_EOL;

        if ($source->type === 'salla') {
            $sallaJs = file_get_contents(base_path('static/cdn/salla.js'));
            $js .= $sallaJs;
            $js .= PHP_EOL;
        }

        if ($source->type === 'zid') {
            $zidJs = file_get_contents(base_path('static/cdn/zid.js'));
            $js .= $zidJs;
            $js .= PHP_EOL;
        }

        if ($source->type === 'tryorder') {
            $tryorderJs = file_get_contents(base_path('static/cdn/tryorder.js'));
            $js .= $tryorderJs;
            $js .= PHP_EOL;
        }

        if ($source->type === 'wordpress') {
            $wordpressJs = file_get_contents(base_path('static/cdn/wordpress.js'));
            $js .= $wordpressJs;
            $js .= PHP_EOL;
        }

        $minifier = new Minify\JS;
        $minifier->add($js);
        $minifiedCode = $minifier->minify();

        return response($minifiedCode, 200)
            ->header('Content-Type', 'application/javascript');
    }
}
