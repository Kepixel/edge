<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use App\Models\Destination;
use App\Models\Source;
use Illuminate\Support\Facades\Cache;

class SourceConfigController extends Controller
{
    public function __invoke()
    {
        $sourceKey = request()->query('writeKey');

        $sourceExists = Cache::remember('source_exists_' . $sourceKey, 86400, function () use ($sourceKey) {
            return $sourceKey && Source::where('app_token', $sourceKey)->exists();
        });

        if (!$sourceExists) {
            return redirect('https://kepixel.com');
        }

        $definitions = base_path('static/definitions/destinations.json');

        $definitions = json_decode(file_get_contents($definitions), true);

        $cacheKey = 'source_config_response_' . $sourceKey;
        $response = Cache::remember($cacheKey, 3600, function () use ($sourceKey, $definitions) {
            $source = Source::where('app_token', $sourceKey)->with('destinations')->first();

            $timestamp = now()->toIso8601String();

            return [
                'source' => [
                    'id' => $source->id,
                    'name' => $source->name,
                    'writeKey' => $sourceKey,
                    'config' => [
                        'statsCollection' => [
                            'errors' => ['enabled' => true],
                            'metrics' => ['enabled' => true],
                        ],
                    ],
                    'enabled' => true,
                    'workspaceId' => $source->team_id,
                    'destinations' => $source->destinations->map(function (Destination $destination) use ($definitions) {
                        $configDestination = collect($definitions)->firstWhere('slug', $destination->platform);

                        return [
                            'id' => $destination->id,
                            'name' => $destination->name,
                            'enabled' => true,
                            'destinationDefinitionId' => $configDestination['id'] ?? '',
                            'destinationDefinition' => [
                                'name' => $configDestination['name'],
                                'displayName' => $configDestination['displayName'],
                            ]
                        ];
                    }),
                    'updatedAt' => $timestamp,
                    'dataplanes' => (object)[],
                ],
                'updatedAt' => $timestamp,
                'consentManagementMetadata' => [
                    'providers' => [
                        ['provider' => 'oneTrust', 'resolutionStrategy' => 'and'],
                        ['provider' => 'ketch', 'resolutionStrategy' => 'or'],
                        ['provider' => 'iubenda', 'resolutionStrategy' => 'or'],
                    ],
                ],
            ];
        });

        return response()->json($response)
            ->header('Content-Type', 'application/json')
            ->header('Cache-Control', 'public, max-age=604800, immutable')
            ->header('Expires', gmdate('D, d M Y H:i:s', time() + 604800) . ' GMT')
            ->header('X-Accel-Expires', '604800')
            ->header('ETag', md5(json_encode($response)));
    }
}
