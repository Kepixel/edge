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

        /** @var Source $source */
        $source = Cache::remember('source_data_' . $sourceKey, 86400, function () use ($sourceKey) {
            return Source::where('app_token', $sourceKey)->with('destinations')->first(['id', 'app_token', 'tag_id', 'type',
                'use_custom_gtm',
                'own_gtm_container_id',
                'use_custom_google_analytics',
                'own_analytics_measurement_id',
                'own_analytics_api_secret',
            ]);
        });

        if (!$source) {
            return redirect('https://kepixel.com');
        }

        $configJs = 'window.kepixelSourceType = ' . json_encode($source->type) . ";\n";
        $configJs .= 'window.kepixelSourceKey = ' . json_encode($sourceKey) . ";\n";

        $eventValidator = file_get_contents(base_path('static/cdn/event-validator.js'));
        $urlParams = file_get_contents(base_path('static/cdn/url-params.js'));

        $trackerContent = file_get_contents(base_path('static/cdn/tracker.js'));

        $js = $eventValidator;
        $js .= PHP_EOL;
        $js .= $urlParams;
        $js .= PHP_EOL;
        $js .= $configJs . $trackerContent;
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

        if ($source->type === 'easy-orders') {
            $easyOrdersJs = file_get_contents(base_path('static/cdn/easy-orders.js'));
            $js .= $easyOrdersJs;
            $js .= PHP_EOL;
        }

        if ($source->type === 'tryorder') {
            $tryorderJs = file_get_contents(base_path('static/cdn/tryorder.js'));
            $js .= $tryorderJs;
            $js .= PHP_EOL;
            $tryorderNetworkJs = file_get_contents(base_path('static/cdn/tryorder-network.js'));
            $js .= $tryorderNetworkJs;
            $js .= PHP_EOL;
        }

        if ($source->type === 'wordpress') {
            $wordpressJs = file_get_contents(base_path('static/cdn/wordpress.js'));
            $js .= $wordpressJs;
            $js .= PHP_EOL;
        }

//        if ($sourceKey == '01KADKP3YX3BAW9GEG8XK1FVB7') {
//            $ga = $source->destinations->where('platform', 'google-analytics-4')->first();
//            if ($ga) {
//                dd($ga, $ga->config['measurementId']);
//            }
//        }

        if ($source->use_custom_gtm) {
            // own_gtm_container_id
        }

        $gaIds = [];
        $gas = $source->destinations->where('platform', 'google-analytics-4');
        foreach ($gas as $ga) {
            $gaIds[] = $ga->config['measurementId'];
        }

        if ($source->use_custom_google_analytics) {
            $gaIds[] = $source->own_analytics_measurement_id;
        }

        if (!empty($gaIds)) {
            // Sanitize, trim, remove empties and duplicates
            $gaIds = array_values(array_unique(array_filter(array_map(function ($v) {
                $s = is_string($v) ? trim($v) : (is_null($v) ? '' : trim((string)$v));
                return $s !== '' ? $s : null;
            }, $gaIds))));

            // Expose IDs to the GA loader so it can configure them on load
            if (!empty($gaIds)) {
                $gaGlobals = 'window.GA_MEASUREMENT_IDS = ' . json_encode($gaIds) . ';';
                // Also set a primary (legacy) ID for compatibility
                $gaGlobals .= 'window.GA_MEASUREMENT_ID = ' . json_encode($gaIds[0]) . ';';
                $js .= $gaGlobals . PHP_EOL;
            }

            $ga4Js = file_get_contents(base_path('static/sdk/ga.js'));
            $js .= $ga4Js;
            $js .= PHP_EOL;
        }

        $minifier = new Minify\JS;
        $minifier->add($js);
        $minifiedCode = $minifier->minify();

        return response($minifiedCode, 200)
            ->header('Content-Type', 'application/javascript');
    }
}
