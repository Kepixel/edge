<?php

namespace App\Http\Controllers;

class UpdateController extends Controller
{
    public function pluginMetadata()
    {
        $version = '1.0.3';
        $payload = [
            'name'          => 'Kepixel',
            'slug'          => 'kepixel',
            'version'       => $version,
            'download_url'  => 'https://cdn.kepixel.com/wordpress/plugin.zip?v=' . $version,
            'requires'      => '6.0',
            'tested'        => '6.6.2',
            'requires_php'  => '7.4',
            'last_updated'  => '2026-02-05 08:47:27',
            'homepage'      => 'https://www.kepixel.com/',
            'sections'      => [
                'description' => 'Bug fixes and improvements',
                'changelog'   => '1.0.2 Fix X. Improve Y.',
            ],
            'banners'       => [
                'low'  => 'https://cdn.kepixel.com/wordpress/banner-772x250.png',
                'high' => 'https://cdn.kepixel.com/wordpress/banner-1544x500.png',
            ],
        ];

        return response()->json($payload, 200, [
            'Cache-Control' => 'public, max-age=900',
            'Access-Control-Allow-Origin' => '*',
        ])->setEtag(sha1(json_encode($payload)));
    }
}
