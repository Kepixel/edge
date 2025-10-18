<?php

namespace App\Http\Controllers;

class UpdateController extends Controller
{
    public function pluginMetadata()
    {
        $version = '1.0.0';
        $payload = [
            'name'          => 'Kepixel',
            'slug'          => 'kepixel',
            'version'       => $version,
            'download_url'  => "https://github.com/Kepixel/wp/archive/refs/tags/v$version.zip",
            'requires'      => '6.0',
            'tested'        => '6.6.2',
            'requires_php'  => '7.4',
            'last_updated'  => '2025-10-18 09:00:00',
            'homepage'      => 'https://www.kepixel.com/',
            'sections'      => [
                'description' => 'Bug fixes and improvements',
                'changelog'   => '1.0.0 Fix X. Improve Y.',
            ],
            'banners'       => [
                'low'  => 'https://edge.kepixel.com/wordpress-plugin/banner-772x250.png',
                'high' => 'https://edge.kepixel.com/wordpress-plugin/banner-1544x500.png',
            ],
        ];

        return response()->json($payload, 200, [
            'Cache-Control' => 'public, max-age=900',
            'Access-Control-Allow-Origin' => '*',
        ])->setEtag(sha1(json_encode($payload)));
    }
}
