<?php

namespace App\Http\Controllers;

use App\Http\Controllers\Controller;
use App\Models\Destination;
use App\Models\DestinationSource;
use App\Models\Source;
use App\Models\Team;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Collection;
use stdClass;

class GetNameSpaceConfigAction extends Controller
{
    public function __invoke(): JsonResponse
    {
        $teams = Team::query()
            ->select(['id', 'name'])
            ->whereHas('destinationSources')
            ->orderBy('name')
            ->get();

        if ($teams->isEmpty()) {
            return response()->json([]);
        }

        $teamIds = $teams->pluck('id');

        $sources = Source::query()
            ->select(['id', 'team_id', 'name', 'write_key'])
            ->whereIn('team_id', $teamIds)
            ->orderBy('name')
            ->get()
            ->groupBy('team_id');

        $links = DestinationSource::query()
            ->select(['id', 'team_id', 'source_id', 'destination_id', 'event_mappings', 'status'])
            ->whereIn('team_id', $teamIds)
            ->where('status', 'configured')
            ->get();

        $destinationIds = $links->pluck('destination_id')->unique()->all();

        $destinations = empty($destinationIds)
            ? collect()
            : Destination::query()
                ->select(['id', 'name', 'platform', 'status', 'config'])
                ->whereIn('id', $destinationIds)
                ->get()
                ->keyBy('id');

        $linksBySource = $links->groupBy('source_id');

        $allConfig = [];

        foreach ($teams as $team) {
            $teamSources = $sources->get($team->id, collect());
            $allConfig[$team->id] = $this->buildConfig($team->id, $teamSources, $linksBySource, $destinations);
        }

        return response()->json($allConfig);
    }

    private function buildConfig(string $workspaceId, Collection $sources, Collection $linksBySource, Collection $destinations): array
    {
        if ($sources->isEmpty()) {
            return $this->baseConfig($workspaceId, [], []);
        }

        $sourceConfigs = [];
        $connections = [];

        foreach ($sources as $source) {
            $sourceConfig = $this->baseSource($workspaceId);
            $sourceConfig['name'] = $source->name;
            $sourceConfig['id'] = $source->id;
            $sourceConfig['writeKey'] = $source->write_key;

            if ($source->is_live_event_enabled) {
                $sourceConfig['config']['eventUpload'] = true;
                $sourceConfig['config']['eventUploadTS'] = (int) now()->valueOf();
                $sourceConfig['liveEventsConfig']['eventUpload'] = true;
                $sourceConfig['liveEventsConfig']['eventUploadTS'] = (int) now()->valueOf();
            }


            $destinationConfigs = [];

            foreach ($linksBySource->get($source->id, collect()) as $link) {
                $destination = $destinations->get($link->destination_id);

                if (! $destination || $destination->status !== 'configured') {
                    continue;
                }

                $payload = $this->buildDestinationPayload($destination, $link, $workspaceId);

                if (! $payload) {
                    continue;
                }

                $destinationConfigs[] = $payload;

                $connections[$source->id.$destination->id] = [
                    'sourceId' => $source->id,
                    'destinationId' => $destination->id,
                    'enabled' => true,
                    'processorEnabled' => true,
                ];
            }

            $sourceConfig['destinations'] = $destinationConfigs;
            $sourceConfigs[] = $sourceConfig;
        }

        return $this->baseConfig($workspaceId, $sourceConfigs, $connections);
    }

    private function baseConfig(string $workspaceId, array $sources, array $connections): array
    {
        return [
            'workspaceId' => $workspaceId,
            'sources' => $sources,
            'connections' => $connections === [] ? new stdClass : $connections,
        ];
    }

    private function baseSource(string $workspaceId): array
    {
        return [
            'config' => [
                'eventUpload' => false,
                'eventUploadTS' => 1752055686836,
            ],
            'liveEventsConfig' => [
                'eventUpload' => false,
                'eventUploadTS' => 1752055686836,
            ],
            'id' => '',
            'workspaceId' => $workspaceId,
            'destinations' => [],
            'sourceDefinition' => [
                'options' => [
                    'botEventManagement' => true,
                    'sdkExecutionEnvironment' => 'client',
                ],
                'config' => null,
                'configSchema' => [],
                'uiConfig' => [],
                'name' => 'Javascript',
                'id' => '1TW48i2bIzEl1HPf825cEznfIM8',
                'displayName' => 'JavaScript',
                'category' => null,
                'createdAt' => '2019-11-12T12:39:19.885Z',
                'updatedAt' => '2025-05-05T10:35:39.408Z',
                'type' => 'web',
            ],
            'name' => '',
            'writeKey' => '',
            'enabled' => true,
            'deleted' => false,
            'createdBy' => '2nEqgMFsN8z45NuJpqMZCB1bWZl',
            'transient' => false,
            'secretVersion' => null,
            'createdAt' => '2025-07-06T15:07:54.629Z',
            'updatedAt' => '2025-07-09T10:08:06.836Z',
            'geoEnrichment' => [
                'enabled' => false,
            ],
            'sourceDefinitionId' => '1TW48i2bIzEl1HPf825cEznfIM8',
        ];
    }

    private function buildDestinationPayload(Destination $destination, DestinationSource $link, string $workspaceId): ?array
    {
        $template = match ($destination->platform) {
            'facebook-conversions' => $this->getMetaDestination($workspaceId),
            'tiktok-ads' => $this->getTikTokDestination($workspaceId),
            'snapchat-conversion' => $this->getSnapchatDestination($workspaceId),
            'google-analytics-4' => $this->getGoogleAnalyticsDestination($workspaceId),
            default => null,
        };

        if (! $template) {
            return null;
        }

        $config = $destination->config ?? [];

        $template['id'] = $destination->id;
        $template['name'] = $destination->name ?? 'no name';

        switch ($destination->platform) {
            case 'facebook-conversions':
                $template['config']['datasetId'] = $config['pixel_id'] ?? $config['datasetId'] ?? '';
                $template['config']['accessToken'] = $config['access_token'] ?? $config['accessToken'] ?? '';
                $template['config']['eventsToEvents'] = $link->event_mappings ?? [];
                break;
            case 'tiktok-ads':
                $template['config']['accessToken'] = $config['access_token'] ?? $config['accessToken'] ?? '';
                $template['config']['pixelCode'] = $config['pixel_id'] ?? $config['pixelCode'] ?? '';
                $template['config']['eventsToStandard'] = $link->event_mappings ?? [];
                break;
            case 'snapchat-conversion':
                $template['config']['apiKey'] = $config['access_token'] ?? $config['apiKey'] ?? '';
                $template['config']['pixelId'] = $config['pixel_id'] ?? $config['pixelId'] ?? '';
                $template['config']['rudderEventsToSnapEvents'] = $link->event_mappings ?? [];
                break;
            case 'google-analytics-4':
                $template['config']['apiSecret'] = $config['api_secret'] ?? $config['apiSecret'] ?? '';
                $template['config']['measurementId'] = $config['measurement_id'] ?? $config['measurementId'] ?? '';
                break;
        }

        return $template;
    }

    private function getMetaDestination(string $workspaceId): array
    {
        return [
            'secretConfig' => [],
            'config' => [
                'blacklistPiiProperties' => [],
                'datasetId' => '', // This will be set later
                'eventsToEvents' => [], // This will be set later
                'whitelistPiiProperties' => [],
                'limitedDataUSage' => false,
                'actionSource' => 'website',
                'accessToken' => '', // This will be set later
                'testDestination' => false,
                'testEventCode' => '',
                'removeExternalId' => false,
                'connectionMode' => 'cloud',
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1752156858434, // todo make it dynamic
            ],
            'liveEventsConfig' => [
                'eventDelivery' => true,
                'eventDeliveryTS' => 1752156858434, // todo make it dynamic
            ],
            'id' => '', // This will be set later
            'workspaceId' => $workspaceId,
            'destinationDefinition' => [
                'config' => [
                    'destConfig' => [
                        'amp' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'ios' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'web' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cloud' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'unity' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'android' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cordova' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'flutter' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'shopify' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'warehouse' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'reactnative' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'defaultConfig' => [
                            'blacklistPiiProperties',
                            'datasetId',
                            'eventsToEvents',
                            'whitelistPiiProperties',
                            'limitedDataUSage',
                            'actionSource',
                            'accessToken',
                            'testDestination',
                            'testEventCode',
                            'removeExternalId',
                        ],
                    ],
                    'secretKeys' => [
                        'accessToken',
                    ],
                    'transformAtV1' => 'processor',
                    'supportedSourceTypes' => [
                        'android',
                        'ios',
                        'web',
                        'unity',
                        'amp',
                        'cloud',
                        'warehouse',
                        'reactnative',
                        'flutter',
                        'cordova',
                        'shopify',
                    ],
                    'saveDestinationResponse' => true,
                    'supportedConnectionModes' => [
                        'amp' => [
                            'cloud',
                        ],
                        'ios' => [
                            'cloud',
                        ],
                        'web' => [
                            'cloud',
                        ],
                        'cloud' => [
                            'cloud',
                        ],
                        'unity' => [
                            'cloud',
                        ],
                        'android' => [
                            'cloud',
                        ],
                        'cordova' => [
                            'cloud',
                        ],
                        'flutter' => [
                            'cloud',
                        ],
                        'shopify' => [
                            'cloud',
                        ],
                        'warehouse' => [
                            'cloud',
                        ],
                        'reactnative' => [
                            'cloud',
                        ],
                    ],
                    'supportedMessageTypes' => [
                        'page',
                        'screen',
                        'track',
                    ],
                ],
                'configSchema' => [],
                'connectionConfigSchema' => [],
                'responseRules' => new stdClass,
                'options' => [],
                'uiConfig' => [],
                'connectionUIConfig' => [],
                'id' => '2WvtWnchzworQ16cGl2QmRQ0QYj',
                'name' => 'FACEBOOK_CONVERSIONS',
                'displayName' => 'Facebook Conversions',
                'category' => null,
                'createdAt' => '2023-10-18T10:28:35.439Z',
                'updatedAt' => '2025-07-08T09:42:24.681Z',
            ],
            'transformations' => [],
            'isConnectionEnabled' => true,
            'isProcessorEnabled' => true,
            'name' => 'fb cn dev',
            'enabled' => true,
            'deleted' => false,
            'createdAt' => '2025-07-04T06:32:30.960Z',
            'updatedAt' => '2025-07-10T14:14:18.434Z',
            'revisionId' => '', // This will be set later
            'secretVersion' => 8,
        ];
    }

    private function getTikTokDestination(string $workspaceId): array
    {
        return [
            'secretConfig' => [],
            'config' => [
                'sendCustomEvents' => false,
                'accessToken' => '', // This will be set later
                'version' => 'v2',
                'pixelCode' => '',  // This will be set later
                'hashUserProperties' => true,
                'eventsToStandard' => [], // This will be set later
                'blacklistedEvents' => [],
                'whitelistedEvents' => [],
                'eventFilteringOption' => 'disable',
                'useNativeSDK' => false,
                'connectionMode' => 'cloud',
                'consentManagement' => [
                    [
                        'provider' => 'oneTrust',
                        'resolutionStrategy' => '',
                        'consents' => [],
                    ],
                ],
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1751960787404, // todo make it dynamic
            ],
            'liveEventsConfig' => [
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1751960787404, // todo make it dynamic
            ],
            'id' => '', // This will be set later
            'workspaceId' => $workspaceId,
            'destinationDefinition' => [
                'config' => [
                    'destConfig' => [
                        'amp' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'ios' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'web' => [
                            'useNativeSDK',
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cloud' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'unity' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'android' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cordova' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'flutter' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'shopify' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'warehouse' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'reactnative' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'defaultConfig' => [
                            'sendCustomEvents',
                            'accessToken',
                            'version',
                            'pixelCode',
                            'hashUserProperties',
                            'eventsToStandard',
                            'blacklistedEvents',
                            'whitelistedEvents',
                            'eventFilteringOption',
                        ],
                    ],
                    'secretKeys' => [
                        'accessToken',
                        'pixelCode',
                    ],
                    'excludeKeys' => [],
                    'includeKeys' => [
                        'pixelCode',
                        'version',
                        'sendCustomEvents',
                        'hashUserProperties',
                        'eventsToStandard',
                        'blacklistedEvents',
                        'whitelistedEvents',
                        'eventFilteringOption',
                        'oneTrustCookieCategories',
                        'ketchConsentPurposes',
                        'consentManagement',
                    ],
                    'transformAtV1' => 'router',
                    'supportedSourceTypes' => [
                        'web',
                        'cloud',
                        'ios',
                        'android',
                        'unity',
                        'amp',
                        'warehouse',
                        'reactnative',
                        'flutter',
                        'cordova',
                        'shopify',
                    ],
                    'saveDestinationResponse' => true,
                    'supportedConnectionModes' => [
                        'amp' => [
                            'cloud',
                        ],
                        'ios' => [
                            'cloud',
                        ],
                        'web' => [
                            'cloud',
                            'device',
                        ],
                        'cloud' => [
                            'cloud',
                        ],
                        'unity' => [
                            'cloud',
                        ],
                        'android' => [
                            'cloud',
                        ],
                        'cordova' => [
                            'cloud',
                        ],
                        'flutter' => [
                            'cloud',
                        ],
                        'shopify' => [
                            'cloud',
                        ],
                        'warehouse' => [
                            'cloud',
                        ],
                        'reactnative' => [
                            'cloud',
                        ],
                    ],
                    'supportedMessageTypes' => [
                        'track',
                    ],
                ],
                'configSchema' => [],
                'connectionConfigSchema' => [],
                'responseRules' => new stdClass,
                'options' => null,
                'uiConfig' => [],
                'connectionUIConfig' => [],
                'id' => '24Xc49td7cWPVLwuoUmh4tclXfa',
                'name' => 'TIKTOK_ADS',
                'displayName' => 'TikTok Ads',
                'category' => null,
                'createdAt' => '2022-02-02T05:36:17.488Z',
                'updatedAt' => '2025-07-08T09:42:23.734Z',
            ],
            'transformations' => [],
            'isConnectionEnabled' => true,
            'isProcessorEnabled' => true,
            'name' => 'tik ads dev',
            'enabled' => true,
            'deleted' => false,
            'createdAt' => '2025-07-04T06:59:27.974Z',
            'updatedAt' => '2025-07-08T07:46:27.405Z',
            'revisionId' => '', // This will be set later
            'secretVersion' => 5,
        ];
    }

    private function getSnapchatDestination(string $workspaceId): array
    {
        return [
            'secretConfig' => [],
            'config' => [
                'apiKey' => '', // This will be set later
                'pixelId' => '', // This will be set later
                'snapAppId' => '',
                'appId' => '',
                'rudderEventsToSnapEvents' => [], // This will be set later
                'enableDeduplication' => false,
                'apiVersion' => 'newApi',
                'connectionMode' => 'cloud',
                'consentManagement' => [
                    [
                        'provider' => 'oneTrust',
                        'resolutionStrategy' => '',
                        'consents' => [],
                    ],
                ],
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1751960701571, // todo make it dynamic
            ],
            'liveEventsConfig' => [
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1751960701571, // todo make it dynamic
            ],
            'id' => $workspaceId,
            'workspaceId' => '', // This will be set later
            'destinationDefinition' => [
                'config' => [
                    'destConfig' => [
                        'amp' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'ios' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'web' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cloud' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'unity' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'android' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cordova' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'flutter' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'shopify' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'warehouse' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'reactnative' => [
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'defaultConfig' => [
                            'apiKey',
                            'pixelId',
                            'snapAppId',
                            'appId',
                            'rudderEventsToSnapEvents',
                            'enableDeduplication',
                            'deduplicationKey',
                            'apiVersion',
                        ],
                    ],
                    'secretKeys' => [
                        'apiKey',
                    ],
                    'excludeKeys' => [],
                    'includeKeys' => [
                        'oneTrustCookieCategories',
                        'ketchConsentPurposes',
                        'consentManagement',
                    ],
                    'transformAtV1' => 'router',
                    'supportedSourceTypes' => [
                        'android',
                        'ios',
                        'web',
                        'unity',
                        'amp',
                        'cloud',
                        'warehouse',
                        'reactnative',
                        'flutter',
                        'cordova',
                        'shopify',
                    ],
                    'saveDestinationResponse' => true,
                    'supportedConnectionModes' => [
                        'amp' => [
                            'cloud',
                        ],
                        'ios' => [
                            'cloud',
                        ],
                        'web' => [
                            'cloud',
                        ],
                        'cloud' => [
                            'cloud',
                        ],
                        'unity' => [
                            'cloud',
                        ],
                        'android' => [
                            'cloud',
                        ],
                        'cordova' => [
                            'cloud',
                        ],
                        'flutter' => [
                            'cloud',
                        ],
                        'shopify' => [
                            'cloud',
                        ],
                        'warehouse' => [
                            'cloud',
                        ],
                        'reactnative' => [
                            'cloud',
                        ],
                    ],
                    'supportedMessageTypes' => [
                        'track',
                        'page',
                    ],
                ],
                'configSchema' => [],
                'connectionConfigSchema' => [],
                'responseRules' => new stdClass,
                'options' => [],
                'uiConfig' => [],
                'connectionUIConfig' => [],
                'id' => '29FISAjLpj2ebpi67CbcnFo6qye',
                'name' => 'SNAPCHAT_CONVERSION',
                'displayName' => 'Snapchat Conversion',
                'category' => null,
                'createdAt' => '2022-05-16T12:00:03.654Z',
                'updatedAt' => '2025-05-05T10:35:11.369Z',
            ],
            'transformations' => [],
            'isConnectionEnabled' => true,
            'isProcessorEnabled' => true,
            'name' => 'snap dev',
            'enabled' => true,
            'deleted' => false,
            'createdAt' => '2025-07-04T07:00:54.285Z',
            'updatedAt' => '2025-07-08T07:45:01.572Z',
            'revisionId' => '', // This will be set later
            'secretVersion' => 6,
        ];
    }

    private function getGoogleAnalyticsDestination(string $workspaceId): array
    {
        return [
            'secretConfig' => [],
            'config' => [
                'apiSecret' => '', // This will be set later
                'debugMode' => false,
                'typesOfClient' => 'gtag',
                'measurementId' => '', // This will be set later
                'firebaseAppId' => '',
                'whitelistedEvents' => [
                    [
                        'eventName' => '',
                    ],
                ],
                'blacklistedEvents' => [
                    [
                        'eventName' => '',
                    ],
                ],
                'eventFilteringOption' => 'disable',
                'piiPropertiesToIgnore' => [
                    [
                        'piiProperty' => '',
                    ],
                ],
                'sdkBaseUrl' => 'https://www.googletagmanager.com',
                'serverContainerUrl' => '',
                'debugView' => true,
                'useNativeSDK' => false,
                'connectionMode' => 'cloud',
                'capturePageView' => 'rs',
                'useNativeSDKToSend' => false,
                'extendPageViewParams' => false,
                'overrideClientAndSessionId' => false,
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1751656552920, // todo make it dynamic
            ],
            'liveEventsConfig' => [
                'eventDelivery' => true, // todo make it dynamic
                'eventDeliveryTS' => 1751656552920, // todo make it dynamic
            ],
            'id' => '', // This will be set later
            'workspaceId' => $workspaceId,
            'destinationDefinition' => [
                'config' => [
                    'destConfig' => [
                        'amp' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'ios' => [
                            'useNativeSDK',
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'web' => [
                            'debugView',
                            'useNativeSDK',
                            'connectionMode',
                            'capturePageView',
                            'useNativeSDKToSend',
                            'extendPageViewParams',
                            'overrideClientAndSessionId',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cloud' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'unity' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'android' => [
                            'useNativeSDK',
                            'connectionMode',
                            'consentManagement',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'cordova' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'flutter' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'shopify' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'warehouse' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'reactnative' => [
                            'consentManagement',
                            'connectionMode',
                            'oneTrustCookieCategories',
                            'ketchConsentPurposes',
                        ],
                        'defaultConfig' => [
                            'apiSecret',
                            'debugMode',
                            'typesOfClient',
                            'measurementId',
                            'firebaseAppId',
                            'whitelistedEvents',
                            'blacklistedEvents',
                            'eventFilteringOption',
                            'piiPropertiesToIgnore',
                            'sdkBaseUrl',
                            'serverContainerUrl',
                        ],
                    ],
                    'secretKeys' => [
                        'apiSecret',
                    ],
                    'excludeKeys' => [],
                    'includeKeys' => [
                        'debugView',
                        'measurementId',
                        'connectionMode',
                        'capturePageView',
                        'whitelistedEvents',
                        'blacklistedEvents',
                        'useNativeSDKToSend',
                        'eventFilteringOption',
                        'extendPageViewParams',
                        'piiPropertiesToIgnore',
                        'overrideClientAndSessionId',
                        'oneTrustCookieCategories',
                        'ketchConsentPurposes',
                        'consentManagement',
                        'sdkBaseUrl',
                        'serverContainerUrl',
                    ],
                    'transformAtV1' => 'processor',
                    'supportedSourceTypes' => [
                        'android',
                        'ios',
                        'web',
                        'unity',
                        'amp',
                        'cloud',
                        'reactnative',
                        'flutter',
                        'cordova',
                        'warehouse',
                        'shopify',
                    ],
                    'saveDestinationResponse' => false,
                    'supportedConnectionModes' => [
                        'amp' => [
                            'cloud',
                        ],
                        'ios' => [
                            'cloud',
                            'device',
                        ],
                        'web' => [
                            'cloud',
                            'device',
                            'hybrid',
                        ],
                        'cloud' => [
                            'cloud',
                        ],
                        'unity' => [
                            'cloud',
                        ],
                        'android' => [
                            'cloud',
                            'device',
                        ],
                        'cordova' => [
                            'cloud',
                        ],
                        'flutter' => [
                            'cloud',
                        ],
                        'shopify' => [
                            'cloud',
                        ],
                        'warehouse' => [
                            'cloud',
                        ],
                        'reactnative' => [
                            'cloud',
                        ],
                    ],
                    'hybridModeCloudEventsFilter' => [
                        'web' => [
                            'messageType' => [
                                'track',
                                'group',
                            ],
                        ],
                    ],
                    'supportedMessageTypes' => [
                        'track',
                        'group',
                        'page',
                    ],
                ],
                'configSchema' => [],
                'connectionConfigSchema' => [],
                'responseRules' => new stdClass,
                'options' => [
                    'isBeta' => false,
                ],
                'uiConfig' => [],
                'connectionUIConfig' => [],
                'id' => '1mQ0yXGAQM08MTdVxws7ENIPjYS',
                'name' => 'GA4',
                'displayName' => 'Google Analytics 4 (GA4)',
                'category' => null,
                'createdAt' => '2020-12-31T10:32:41.062Z',
                'updatedAt' => '2025-02-05T06:24:24.504Z',
            ],
            'transformations' => [],
            'isConnectionEnabled' => true,
            'isProcessorEnabled' => true,
            'name' => 'ga4 dev',
            'enabled' => true,
            'deleted' => false,
            'createdAt' => '2025-07-04T07:04:14.095Z',
            'updatedAt' => '2025-07-07T12:08:52.819Z',
            'revisionId' => '', // This will be set later
            'secretVersion' => 7,
        ];
    }
}
