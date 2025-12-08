<?php

namespace App\Jobs;

use ClickHouseDB\Client;
use DateTime;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Batch version of ProcessEventEnrichedJob for efficient backfilling.
 * Processes multiple events in a single job and batch inserts to ClickHouse.
 */
class ProcessEventEnrichedBatchJob implements ShouldQueue
{
    use Queueable;

    /**
     * The number of times the job may be attempted.
     */
    public int $tries = 0;

    /**
     * The number of seconds the job can run before timing out.
     */
    public int $timeout = 300; // 5 minutes

    /**
     * The number of seconds to wait before retrying the job.
     */
    public array $backoff = [10, 30, 60];

    // Source constants
    private const SEARCH_SOURCES = [
        'google', 'googleads', 'bing', 'bingads', 'yahoo', 'duckduckgo', 'baidu', 'yandex',
    ];

    private const SOCIAL_SOURCES = [
        'facebook', 'fb', 'instagram', 'ig', 'meta',
        'snap', 'snapchat', 'sc',
        'tiktok', 'tt',
        'twitter', 'x',
        'linkedin', 'li',
        'pinterest', 'pin',
        'reddit',
        'youtube', 'yt',
        'threads',
        'whatsapp',
    ];

    private const PAID_SOURCES = [
        'facebook', 'fb', 'instagram', 'ig', 'meta',
        'tiktok', 'tt',
        'snap', 'snapchat', 'sc',
        'google', 'googleads', 'bing', 'bingads', 'yahoo', 'microsoft',
        'linkedin', 'li',
        'twitter', 'x',
        'pinterest', 'pin',
        'reddit',
        'amazon', 'amzn',
        'criteo', 'outbrain', 'taboola',
    ];

    private const PAID_MEDIUMS = [
        'paid', 'cpc', 'ppc', 'cpv', 'cpa', 'cpm',
        'display', 'paid_social', 'remarketing', 'retargeting',
        'banner', 'native', 'sponsored', 'promoted',
        'shopping', 'pla',
    ];

    private const EMAIL_SOURCES = [
        'email', 'newsletter', 'mailchimp', 'klaviyo', 'sendgrid',
        'hubspot', 'marketo', 'salesforce', 'braze', 'iterable',
        'customer.io', 'drip', 'activecampaign', 'constantcontact',
    ];

    private const EMAIL_MEDIUMS = [
        'email', 'e-mail', 'newsletter', 'mail', 'edm',
    ];

    private const AFFILIATE_MEDIUMS = [
        'affiliate', 'partner', 'referral_partner', 'influencer',
        'ambassador', 'cps', 'commission',
    ];

    // Traffic channel constants
    private const CHANNEL_PAID_SEARCH = 'paid_search';
    private const CHANNEL_PAID_SOCIAL = 'paid_social';
    private const CHANNEL_PAID_VIDEO = 'paid_video';
    private const CHANNEL_PAID_SHOPPING = 'paid_shopping';
    private const CHANNEL_PAID_NATIVE = 'paid_native';
    private const CHANNEL_DIRECT = 'direct';
    private const CHANNEL_ORGANIC_SEARCH = 'organic_search';
    private const CHANNEL_ORGANIC_SOCIAL = 'organic_social';
    private const CHANNEL_ORGANIC_VIDEO = 'organic_video';
    private const CHANNEL_EMAIL = 'email';
    private const CHANNEL_AFFILIATE = 'affiliate';
    private const CHANNEL_REFERRAL = 'referral';
    private const CHANNEL_OTHER = 'other';

    // Platform constants
    private const PLATFORM_INSTAGRAM_REELS_ADS = 'instagram_reels_ads';
    private const PLATFORM_FACEBOOK_REELS_ADS = 'facebook_reels_ads';
    private const PLATFORM_INSTAGRAM_ADS = 'instagram_ads';
    private const PLATFORM_META_ADS = 'meta_ads';
    private const PLATFORM_META_ADVANTAGE_PLUS = 'meta_advantage_plus';
    private const PLATFORM_FACEBOOK_ADS = 'facebook_ads';
    private const PLATFORM_TIKTOK_SPARK_ADS = 'tiktok_spark_ads';
    private const PLATFORM_TIKTOK_CREATOR_ADS = 'tiktok_creator_ads';
    private const PLATFORM_TIKTOK_TOPVIEW = 'tiktok_topview';
    private const PLATFORM_TIKTOK_ADS = 'tiktok_ads';
    private const PLATFORM_SNAPCHAT_ADS = 'snapchat_ads';
    private const PLATFORM_GOOGLE_PERFORMANCE_MAX = 'google_performance_max';
    private const PLATFORM_GOOGLE_DISPLAY_ADS = 'google_display_ads';
    private const PLATFORM_GOOGLE_YOUTUBE_ADS = 'google_youtube_ads';
    private const PLATFORM_GOOGLE_SEARCH_ADS = 'google_search_ads';
    private const PLATFORM_GOOGLE_SHOPPING_ADS = 'google_shopping_ads';
    private const PLATFORM_GOOGLE_DISCOVERY_ADS = 'google_discovery_ads';
    private const PLATFORM_GOOGLE_APP_ADS = 'google_app_ads';
    private const PLATFORM_MICROSOFT_ADS = 'microsoft_ads';
    private const PLATFORM_BING_SHOPPING_ADS = 'bing_shopping_ads';
    private const PLATFORM_LINKEDIN_ADS = 'linkedin_ads';
    private const PLATFORM_LINKEDIN_SPONSORED_CONTENT = 'linkedin_sponsored_content';
    private const PLATFORM_LINKEDIN_INMAIL = 'linkedin_inmail';
    private const PLATFORM_TWITTER_ADS = 'twitter_ads';
    private const PLATFORM_PINTEREST_ADS = 'pinterest_ads';
    private const PLATFORM_REDDIT_ADS = 'reddit_ads';
    private const PLATFORM_AMAZON_ADS = 'amazon_ads';
    private const PLATFORM_CRITEO_ADS = 'criteo_ads';
    private const PLATFORM_OUTBRAIN_ADS = 'outbrain_ads';
    private const PLATFORM_TABOOLA_ADS = 'taboola_ads';
    private const PLATFORM_OTHER_PAID = 'other_paid';
    private const PLATFORM_DIRECT = 'direct';
    private const PLATFORM_ORGANIC_SEARCH = 'organic_search';
    private const PLATFORM_ORGANIC_SOCIAL = 'organic_social';
    private const PLATFORM_ORGANIC_VIDEO = 'organic_video';
    private const PLATFORM_EMAIL = 'email';
    private const PLATFORM_AFFILIATE = 'affiliate';
    private const PLATFORM_REFERRAL = 'referral';
    private const PLATFORM_OTHER = 'other';

    // Click ID to platform mapping
    private const CLICK_ID_PLATFORMS = [
        'fbclid' => self::PLATFORM_FACEBOOK_ADS,
        'ttclid' => self::PLATFORM_TIKTOK_ADS,
        'gclid' => self::PLATFORM_GOOGLE_SEARCH_ADS,
        'gbraid' => self::PLATFORM_GOOGLE_SEARCH_ADS,
        'wbraid' => self::PLATFORM_GOOGLE_SEARCH_ADS,
        'msclkid' => self::PLATFORM_MICROSOFT_ADS,
        'li_fat_id' => self::PLATFORM_LINKEDIN_ADS,
        'twclid' => self::PLATFORM_TWITTER_ADS,
        'epik' => self::PLATFORM_PINTEREST_ADS,
        'ScCid' => self::PLATFORM_SNAPCHAT_ADS,
        'scid' => self::PLATFORM_SNAPCHAT_ADS,
        'rdt_cid' => self::PLATFORM_REDDIT_ADS,
        'crto_pid' => self::PLATFORM_CRITEO_ADS,
        'obOrigUrl' => self::PLATFORM_OUTBRAIN_ADS,
        'tblci' => self::PLATFORM_TABOOLA_ADS,
    ];

    // Click ID to inferred UTM data
    private const CLICK_ID_ATTRIBUTION = [
        'fbclid' => ['source' => 'facebook', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'ttclid' => ['source' => 'tiktok', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'gclid' => ['source' => 'google', 'medium' => 'cpc', 'channel' => self::CHANNEL_PAID_SEARCH],
        'gbraid' => ['source' => 'google', 'medium' => 'cpc', 'channel' => self::CHANNEL_PAID_SEARCH],
        'wbraid' => ['source' => 'google', 'medium' => 'cpc', 'channel' => self::CHANNEL_PAID_SEARCH],
        'msclkid' => ['source' => 'bing', 'medium' => 'cpc', 'channel' => self::CHANNEL_PAID_SEARCH],
        'li_fat_id' => ['source' => 'linkedin', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'twclid' => ['source' => 'twitter', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'epik' => ['source' => 'pinterest', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'ScCid' => ['source' => 'snapchat', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'scid' => ['source' => 'snapchat', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'rdt_cid' => ['source' => 'reddit', 'medium' => 'paid', 'channel' => self::CHANNEL_PAID_SOCIAL],
        'crto_pid' => ['source' => 'criteo', 'medium' => 'retargeting', 'channel' => self::CHANNEL_PAID_NATIVE],
        'obOrigUrl' => ['source' => 'outbrain', 'medium' => 'native', 'channel' => self::CHANNEL_PAID_NATIVE],
        'tblci' => ['source' => 'taboola', 'medium' => 'native', 'channel' => self::CHANNEL_PAID_NATIVE],
    ];

    /**
     * Create a new job instance.
     *
     * @param array $events Array of event rows from event_upload_logs
     */
    public function __construct(
        public array $events
    ) {
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        if (empty($this->events)) {
            return;
        }

        $client = app(Client::class);

        $enrichedRows = [];
        $identityMappings = [];
        $userProfiles = [];
        $processedCount = 0;
        $skippedCount = 0;

        foreach ($this->events as $event) {
            try {
                $processed = $this->processEvent($event, $enrichedRows, $identityMappings, $userProfiles);
                if ($processed) {
                    $processedCount++;
                } else {
                    $skippedCount++;
                }
            } catch (Throwable $e) {
                $skippedCount++;
                Log::warning('ProcessEventEnrichedBatchJob: Failed to process event', [
                    'message_id' => $event['message_id'] ?? 'unknown',
                    'error' => $e->getMessage(),
                ]);
            }
        }

        // Batch insert enriched events
        if (!empty($enrichedRows)) {
            $this->batchInsertEnriched($client, $enrichedRows);
        }

        // Batch insert identity mappings
        if (!empty($identityMappings)) {
            $this->batchInsertIdentityMappings($client, $identityMappings);
        }

        // Batch insert user profiles
        if (!empty($userProfiles)) {
            $this->batchInsertUserProfiles($client, $userProfiles);
        }

        Log::info('ProcessEventEnrichedBatchJob: Completed', [
            'processed' => $processedCount,
            'skipped' => $skippedCount,
            'enriched_rows' => count($enrichedRows),
            'identity_mappings' => count($identityMappings),
            'user_profiles' => count($userProfiles),
        ]);
    }

    /**
     * Process a single event and add to batch arrays.
     */
    private function processEvent(array $event, array &$enrichedRows, array &$identityMappings, array &$userProfiles): bool
    {
        $teamId = $event['team_id'] ?? null;
        $sourceId = $event['source_id'] ?? null;
        $eventTimestamp = $event['event_timestamp'] ?? null;

        // Skip invalid rows
        if (empty($teamId) || empty($sourceId) || empty($eventTimestamp)) {
            return false;
        }

        $properties = is_string($event['properties'] ?? null)
            ? (json_decode($event['properties'], true) ?: [])
            : ($event['properties'] ?? []);

        $eventName = $event['event_name'] ?? '';
        $eventType = $event['event_type'] ?? '';
        $userId = $event['user_id'] ?? '';
        $anonymousId = $event['anonymous_id'] ?? '';
        $messageId = $event['message_id'] ?? '';
        $sessionId = $event['session_id'] ?? '';
        $rudderId = $event['rudder_id'] ?? '';

        // Extract and enrich data
        $context = is_array($properties['context'] ?? null) ? $properties['context'] : [];
        $page = is_array($context['page'] ?? null) ? $context['page'] : [];
        $campaign = is_array($context['campaign'] ?? null) ? $context['campaign'] : [];

        $pageData = $this->extractPageData($page);
        $utmData = $this->parseUtmParameters($page, $campaign);
        $clickIdData = $this->resolveClickId($utmData['queryParams'], $context);
        $clickId = $clickIdData['clickId'];

        $utmData = $this->enrichUtmFromClickId($utmData, $clickIdData);

        $isDirect = $this->determineIsDirect($pageData, $utmData, $clickId);
        $isPaid = $this->determineIsPaid($utmData, $clickId);
        $trafficChannel = $this->determineTrafficChannel($utmData, $pageData, $clickIdData, $isDirect, $isPaid);
        $platform = $this->determinePlatform($utmData, $pageData, $clickIdData, $isPaid, $trafficChannel);

        // Add to enriched rows batch
        $enrichedRows[] = [
            substr($eventTimestamp, 0, 10),
            $eventTimestamp,
            $teamId,
            $sourceId,
            $eventName,
            $eventType,
            $userId,
            $anonymousId,
            $messageId,
            $sessionId,
            $rudderId,
            $pageData['pagePath'],
            $pageData['pageUrl'],
            $pageData['pageTitle'],
            $pageData['pageDomain'],
            $pageData['pageQuery'],
            $pageData['landingReferrer'],
            $pageData['landingReferringDomain'],
            $pageData['lastReferrer'],
            $pageData['lastReferringDomain'],
            $utmData['utmSource'],
            $utmData['utmMedium'],
            $utmData['utmCampaign'],
            $utmData['utmTerm'],
            $utmData['utmContent'],
            $utmData['utmId'],
            $utmData['utmSourcePlatform'],
            $utmData['utmContentType'],
            $utmData['snapCampaignId'],
            $clickId,
            $isDirect,
            $isPaid,
            $trafficChannel,
            $platform,
        ];

        // Process identify events
        if ($eventType === 'identify') {
            // Identity mapping
            if (!empty($userId) && !empty($anonymousId)) {
                $identityMappings[] = [
                    $teamId,
                    $anonymousId,
                    $userId,
                    $eventTimestamp,
                    $eventTimestamp,
                ];
            }

            // User profile
            $traits = $properties['traits'] ?? $properties;
            if (!empty($userId) || !empty($anonymousId)) {
                $canonicalUserId = $userId ?: 'anon_' . $anonymousId;
                $userProfiles[] = [
                    $teamId,
                    $canonicalUserId,
                    $traits['email'] ?? null,
                    $traits['phone'] ?? null,
                    $traits['name'] ?? null,
                    $traits['username'] ?? null,
                    $traits['firstName'] ?? $traits['first_name'] ?? null,
                    $traits['lastName'] ?? $traits['last_name'] ?? null,
                    $traits['avatar'] ?? null,
                    $eventTimestamp,
                    $eventTimestamp,
                    json_encode($traits),
                ];
            }
        }

        return true;
    }

    /**
     * Batch insert enriched events.
     */
    private function batchInsertEnriched(Client $client, array $rows): void
    {
        $client->insert(
            'event_enriched',
            $rows,
            [
                'event_date',
                'event_timestamp',
                'team_id',
                'source_id',
                'event_name',
                'event_type',
                'user_id',
                'anonymous_id',
                'message_id',
                'session_id',
                'rudder_id',
                'page_path',
                'page_url',
                'page_title',
                'page_domain',
                'page_query',
                'landing_referrer',
                'landing_referring_domain',
                'last_referrer',
                'last_referring_domain',
                'utm_source',
                'utm_medium',
                'utm_campaign',
                'utm_term',
                'utm_content',
                'utm_id',
                'utm_source_platform',
                'utm_content_type',
                'ad_campaign_native_id',
                'click_id',
                'is_direct',
                'is_paid',
                'traffic_channel',
                'platform',
            ]
        );
    }

    /**
     * Batch insert identity mappings.
     */
    private function batchInsertIdentityMappings(Client $client, array $rows): void
    {
        try {
            $client->insert(
                'identity_mappings',
                $rows,
                ['team_id', 'anonymous_id', 'user_id', 'first_seen_at', 'last_seen_at']
            );
        } catch (Throwable $e) {
            Log::warning('ProcessEventEnrichedBatchJob: Failed to insert identity mappings', [
                'count' => count($rows),
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * Batch insert user profiles.
     */
    private function batchInsertUserProfiles(Client $client, array $rows): void
    {
        try {
            $client->insert(
                'user_profiles',
                $rows,
                [
                    'team_id',
                    'canonical_user_id',
                    'email',
                    'phone',
                    'name',
                    'username',
                    'first_name',
                    'last_name',
                    'avatar',
                    'first_seen',
                    'last_seen',
                    'traits',
                ]
            );
        } catch (Throwable $e) {
            Log::warning('ProcessEventEnrichedBatchJob: Failed to insert user profiles', [
                'count' => count($rows),
                'error' => $e->getMessage(),
            ]);
        }
    }

    // ============================================================
    // HELPER METHODS (same as ProcessEventEnrichedJob)
    // ============================================================

    private function extractPageData(array $page): array
    {
        $pageUrl = $page['url'] ?? '';
        $pagePath = $page['path'] ?? '';
        $pageTitle = $page['title'] ?? '';
        $pageQuery = $page['search'] ?? '';

        $pageDomain = '';
        if (!empty($pageUrl)) {
            $host = parse_url($pageUrl, PHP_URL_HOST);
            $pageDomain = $host ?: '';
        }

        return [
            'pageUrl' => $pageUrl,
            'pagePath' => $pagePath,
            'pageTitle' => $pageTitle,
            'pageQuery' => $pageQuery,
            'pageDomain' => $pageDomain,
            'landingReferrer' => $page['initial_referrer'] ?? '',
            'landingReferringDomain' => $page['initial_referring_domain'] ?? '',
            'lastReferrer' => $page['referrer'] ?? '',
            'lastReferringDomain' => $page['referring_domain'] ?? '',
        ];
    }

    private function parseUtmParameters(array $page, array $campaign): array
    {
        $pageQuery = $page['search'] ?? '';
        $queryString = ltrim($pageQuery, '?');
        parse_str($queryString, $q);

        return [
            'utmSource' => $q['utm_source'] ?? $campaign['source'] ?? '',
            'utmMedium' => $q['utm_medium'] ?? $campaign['medium'] ?? '',
            'utmCampaign' => $q['utm_campaign'] ?? $campaign['campaign'] ?? '',
            'utmTerm' => $q['utm_term'] ?? $campaign['term'] ?? '',
            'utmContent' => $q['utm_content'] ?? $campaign['content'] ?? '',
            'utmId' => $q['utm_id'] ?? $campaign['id'] ?? '',
            'utmSourcePlatform' => $q['utm_source_platform'] ?? $campaign['source_platform'] ?? '',
            'utmContentType' => $q['utm_content_type'] ?? $campaign['content_type'] ?? '',
            'snapCampaignId' => $q['snap_campaign_id'] ?? $campaign['campaign_id'] ?? '',
            'queryParams' => $q,
        ];
    }

    private function resolveClickId(array $queryParams, array $context): array
    {
        $clickIdParams = [
            'fbclid' => $queryParams['fbclid'] ?? $context['facebook_click_id'] ?? '',
            'ttclid' => $queryParams['ttclid'] ?? $context['tiktok_click_id'] ?? '',
            'gclid' => $queryParams['gclid'] ?? $context['google_click_id'] ?? '',
            'gbraid' => $queryParams['gbraid'] ?? '',
            'wbraid' => $queryParams['wbraid'] ?? $context['meta_click_id'] ?? '',
            'msclkid' => $queryParams['msclkid'] ?? $context['microsoft_click_id'] ?? '',
            'li_fat_id' => $queryParams['li_fat_id'] ?? $context['linkedin_click_id'] ?? '',
            'twclid' => $queryParams['twclid'] ?? $context['twitter_click_id'] ?? '',
            'epik' => $queryParams['epik'] ?? $context['pinterest_click_id'] ?? '',
            'ScCid' => $queryParams['ScCid'] ?? $context['snapchat_click_id'] ?? '',
            'scid' => $queryParams['scid'] ?? $context['snapchat_scid'] ?? '',
            'rdt_cid' => $queryParams['rdt_cid'] ?? $context['reddit_click_id'] ?? '',
            'crto_pid' => $queryParams['crto_pid'] ?? '',
            'obOrigUrl' => $queryParams['obOrigUrl'] ?? '',
            'tblci' => $queryParams['tblci'] ?? '',
        ];

        foreach ($clickIdParams as $paramName => $value) {
            if (!empty($value)) {
                return [
                    'clickId' => $value,
                    'clickIdParam' => $paramName,
                ];
            }
        }

        return ['clickId' => '', 'clickIdParam' => ''];
    }

    private function enrichUtmFromClickId(array $utmData, array $clickIdData): array
    {
        $clickIdParam = $clickIdData['clickIdParam'];

        if (empty($clickIdParam) || !empty($utmData['utmSource'])) {
            return $utmData;
        }

        if (!isset(self::CLICK_ID_ATTRIBUTION[$clickIdParam])) {
            return $utmData;
        }

        $attribution = self::CLICK_ID_ATTRIBUTION[$clickIdParam];

        if (empty($utmData['utmSource'])) {
            $utmData['utmSource'] = $attribution['source'];
        }

        if (empty($utmData['utmMedium'])) {
            $utmData['utmMedium'] = $attribution['medium'];
        }

        return $utmData;
    }

    private function determineIsPaid(array $utmData, string $clickId): int
    {
        $utmSourceLower = mb_strtolower($utmData['utmSource']);
        $utmMediumLower = mb_strtolower($utmData['utmMedium']);

        if (in_array($utmMediumLower, self::PAID_MEDIUMS, true)) {
            return 1;
        }

        $paidMediumKeywords = ['cpc', 'ppc', 'cpv', 'cpa', 'cpm', 'paid', 'remarketing', 'retargeting', 'sponsored'];
        foreach ($paidMediumKeywords as $keyword) {
            if ($this->contains($utmMediumLower, $keyword)) {
                return 1;
            }
        }

        if (!empty($clickId)) {
            return 1;
        }

        if (in_array($utmSourceLower, self::PAID_SOURCES, true) && $utmData['utmCampaign'] !== '') {
            return 1;
        }

        return 0;
    }

    private function determineIsDirect(array $pageData, array $utmData, string $clickId = ''): int
    {
        if (!$this->hasNoUtmParameters($utmData)) {
            return 0;
        }

        if (!empty($clickId)) {
            return 0;
        }

        $landingReferrer = $pageData['landingReferrer'];
        $landingReferringDomain = $pageData['landingReferringDomain'];
        $pageDomain = $pageData['pageDomain'];

        if ($landingReferrer === '$direct') {
            return 1;
        }

        if ($landingReferrer === '' || $landingReferrer === null) {
            return 1;
        }

        if (!empty($pageDomain) && !empty($landingReferringDomain)) {
            $pageDomainClean = $this->extractRootDomain($pageDomain);
            $referrerDomainClean = $this->extractRootDomain($landingReferringDomain);

            if ($pageDomainClean === $referrerDomainClean) {
                return 1;
            }
        }

        $directIndicators = [
            '$direct', '(direct)', 'direct', '(none)', 'none', '(not set)',
            'bookmark', 'typed', 'url_typed', 'android-app://com.google.android.gm',
        ];

        $referrerLower = mb_strtolower($landingReferrer);
        if (in_array($referrerLower, $directIndicators, true)) {
            return 1;
        }

        return 0;
    }

    private function hasNoUtmParameters(array $utmData): bool
    {
        return empty($utmData['utmSource'])
            && empty($utmData['utmMedium'])
            && empty($utmData['utmCampaign']);
    }

    private function extractRootDomain(string $domain): string
    {
        $domain = mb_strtolower($domain);

        if (str_starts_with($domain, 'www.')) {
            $domain = substr($domain, 4);
        }

        $parts = explode('.', $domain);
        $count = count($parts);

        if ($count > 2) {
            $tld = $parts[$count - 1];
            $sld = $parts[$count - 2];

            $compoundTlds = ['co', 'com', 'net', 'org', 'edu', 'gov', 'ac'];
            if (in_array($sld, $compoundTlds, true) && strlen($tld) === 2) {
                if ($count > 3) {
                    return implode('.', array_slice($parts, -3));
                }
            } else {
                return implode('.', array_slice($parts, -2));
            }
        }

        return $domain;
    }

    private function determineTrafficChannel(array $utmData, array $pageData, array $clickIdData, int $isDirect, int $isPaid): string
    {
        $utmSourceLower = mb_strtolower($utmData['utmSource']);
        $utmMediumLower = mb_strtolower($utmData['utmMedium']);
        $utmSourcePlatformLower = mb_strtolower($utmData['utmSourcePlatform']);
        $landingReferringDomain = $pageData['landingReferringDomain'];
        $landingReferrer = $pageData['landingReferrer'];
        $clickIdParam = $clickIdData['clickIdParam'] ?? '';

        if (in_array($utmSourceLower, self::EMAIL_SOURCES, true)
            || in_array($utmMediumLower, self::EMAIL_MEDIUMS, true)) {
            return self::CHANNEL_EMAIL;
        }

        if (in_array($utmMediumLower, self::AFFILIATE_MEDIUMS, true)) {
            return self::CHANNEL_AFFILIATE;
        }

        if ($isPaid === 1) {
            if ($this->contains($utmMediumLower, 'shopping')
                || $this->contains($utmMediumLower, 'pla')
                || $utmSourcePlatformLower === 'google_shopping') {
                return self::CHANNEL_PAID_SHOPPING;
            }

            if ($utmSourcePlatformLower === 'youtube'
                || $this->contains($utmSourceLower, 'youtube')
                || $this->contains($utmMediumLower, 'video')) {
                return self::CHANNEL_PAID_VIDEO;
            }

            if (in_array($utmSourceLower, ['criteo', 'outbrain', 'taboola'], true)
                || $this->contains($utmMediumLower, 'native')) {
                return self::CHANNEL_PAID_NATIVE;
            }

            if (in_array($utmSourceLower, self::SEARCH_SOURCES, true)) {
                return self::CHANNEL_PAID_SEARCH;
            }

            if (in_array($utmSourceLower, self::SOCIAL_SOURCES, true)) {
                return self::CHANNEL_PAID_SOCIAL;
            }

            if (!empty($clickIdParam) && isset(self::CLICK_ID_ATTRIBUTION[$clickIdParam])) {
                return self::CLICK_ID_ATTRIBUTION[$clickIdParam]['channel'];
            }

            return self::CHANNEL_OTHER;
        }

        if ($isDirect === 1) {
            return self::CHANNEL_DIRECT;
        }

        if ($utmSourceLower === 'youtube' || $utmSourceLower === 'yt'
            || $this->contains($landingReferringDomain, 'youtube')) {
            return self::CHANNEL_ORGANIC_VIDEO;
        }

        if (in_array($utmSourceLower, self::SEARCH_SOURCES, true)) {
            return self::CHANNEL_ORGANIC_SEARCH;
        }

        if (in_array($utmSourceLower, self::SOCIAL_SOURCES, true)) {
            return self::CHANNEL_ORGANIC_SOCIAL;
        }

        $searchDomains = ['google', 'bing', 'yahoo', 'duckduckgo', 'baidu', 'yandex'];
        foreach ($searchDomains as $domain) {
            if ($this->contains($landingReferringDomain, $domain)) {
                return self::CHANNEL_ORGANIC_SEARCH;
            }
        }

        $socialDomains = ['facebook', 'instagram', 'twitter', 'linkedin', 'pinterest', 'reddit', 'tiktok', 'snapchat'];
        foreach ($socialDomains as $domain) {
            if ($this->contains($landingReferringDomain, $domain)) {
                return self::CHANNEL_ORGANIC_SOCIAL;
            }
        }

        if ($landingReferrer !== ''
            && $landingReferrer !== '$direct'
            && !in_array($landingReferringDomain, ['', 'localhost'], true)
        ) {
            return self::CHANNEL_REFERRAL;
        }

        return self::CHANNEL_OTHER;
    }

    private function determinePlatform(array $utmData, array $pageData, array $clickIdData, int $isPaid, string $trafficChannel): string
    {
        if ($isPaid !== 1) {
            return $this->determineOrganicPlatform($trafficChannel);
        }

        $utmSourceLower = mb_strtolower($utmData['utmSource']);
        $utmMediumLower = mb_strtolower($utmData['utmMedium']);
        $utmSourcePlatformLower = mb_strtolower($utmData['utmSourcePlatform']);
        $utmContentTypeLower = mb_strtolower($utmData['utmContentType']);
        $utmCampaignLower = mb_strtolower($utmData['utmCampaign']);
        $landingDomainLower = mb_strtolower($pageData['landingReferringDomain']);

        $platform = $this->determinePlatformFromUtm(
            $utmSourceLower,
            $utmMediumLower,
            $utmSourcePlatformLower,
            $utmContentTypeLower,
            $utmCampaignLower
        );

        if ($platform !== null) {
            return $platform;
        }

        return $this->determinePlatformFromClickId($clickIdData, $landingDomainLower);
    }

    private function determinePlatformFromUtm(
        string $utmSourceLower,
        string $utmMediumLower,
        string $utmSourcePlatformLower,
        string $utmContentTypeLower,
        string $utmCampaignLower
    ): ?string {
        $isReel = $this->contains($utmContentTypeLower, 'reel');
        $isSpark = $this->contains($utmContentTypeLower, 'spark');
        $isCreator = $this->contains($utmContentTypeLower, 'creator');
        $isTopview = $this->contains($utmContentTypeLower, 'topview') || $this->contains($utmContentTypeLower, 'brand_takeover');
        $isAdvantagePlus = $this->contains($utmCampaignLower, 'advantage') || $this->contains($utmCampaignLower, 'asc');
        $isTiktok = str_starts_with($utmSourceLower, 'tiktok') || $utmSourceLower === 'tt';
        $isYoutube = $utmSourcePlatformLower === 'youtube' || $this->contains($utmSourceLower, 'youtube');
        $isDisplay = $this->contains($utmMediumLower, 'display') || in_array($utmSourcePlatformLower, ['google_display', 'gdn'], true);
        $isShopping = $this->contains($utmMediumLower, 'shopping') || $this->contains($utmMediumLower, 'pla') || $utmSourcePlatformLower === 'google_shopping';
        $isDiscovery = $this->contains($utmCampaignLower, 'discovery') || $utmSourcePlatformLower === 'google_discovery';
        $isPmax = $this->contains($utmCampaignLower, 'pmax') || $this->contains($utmCampaignLower, 'performance_max');
        $isApp = $this->contains($utmCampaignLower, 'app') && $this->contains($utmSourceLower, 'google');

        // Meta/Facebook/Instagram
        if (in_array($utmSourceLower, ['facebook', 'fb', 'meta', 'instagram', 'ig'], true)) {
            if ($isAdvantagePlus) return self::PLATFORM_META_ADVANTAGE_PLUS;
            if ($isReel && $this->contains($utmSourcePlatformLower, 'instagram')) return self::PLATFORM_INSTAGRAM_REELS_ADS;
            if ($isReel) return self::PLATFORM_FACEBOOK_REELS_ADS;
            if (in_array($utmSourceLower, ['instagram', 'ig'], true)) return self::PLATFORM_INSTAGRAM_ADS;
            if ($utmSourceLower === 'meta') return self::PLATFORM_META_ADS;
            return self::PLATFORM_FACEBOOK_ADS;
        }

        // TikTok
        if ($isTiktok) {
            if ($isTopview) return self::PLATFORM_TIKTOK_TOPVIEW;
            if ($isSpark) return self::PLATFORM_TIKTOK_SPARK_ADS;
            if ($isCreator) return self::PLATFORM_TIKTOK_CREATOR_ADS;
            return self::PLATFORM_TIKTOK_ADS;
        }

        // Snapchat
        if (in_array($utmSourceLower, ['snap', 'snapchat', 'sc'], true)) {
            return self::PLATFORM_SNAPCHAT_ADS;
        }

        // LinkedIn
        if (in_array($utmSourceLower, ['linkedin', 'li'], true)) {
            if ($this->contains($utmMediumLower, 'inmail') || $this->contains($utmContentTypeLower, 'inmail')) {
                return self::PLATFORM_LINKEDIN_INMAIL;
            }
            if ($this->contains($utmMediumLower, 'sponsored_content') || $this->contains($utmContentTypeLower, 'sponsored')) {
                return self::PLATFORM_LINKEDIN_SPONSORED_CONTENT;
            }
            return self::PLATFORM_LINKEDIN_ADS;
        }

        // Twitter/X
        if (in_array($utmSourceLower, ['twitter', 'x'], true)) {
            return self::PLATFORM_TWITTER_ADS;
        }

        // Pinterest
        if (in_array($utmSourceLower, ['pinterest', 'pin'], true)) {
            return self::PLATFORM_PINTEREST_ADS;
        }

        // Reddit
        if ($utmSourceLower === 'reddit') {
            return self::PLATFORM_REDDIT_ADS;
        }

        // Amazon
        if (in_array($utmSourceLower, ['amazon', 'amzn'], true)) {
            return self::PLATFORM_AMAZON_ADS;
        }

        // Native advertising
        if ($utmSourceLower === 'criteo') return self::PLATFORM_CRITEO_ADS;
        if ($utmSourceLower === 'outbrain') return self::PLATFORM_OUTBRAIN_ADS;
        if ($utmSourceLower === 'taboola') return self::PLATFORM_TABOOLA_ADS;

        // Microsoft/Bing
        if (in_array($utmSourceLower, ['bing', 'bingads', 'microsoft'], true)) {
            if ($isShopping) return self::PLATFORM_BING_SHOPPING_ADS;
            return self::PLATFORM_MICROSOFT_ADS;
        }

        // Google platforms
        if (in_array($utmSourceLower, ['google', 'googleads'], true)) {
            if ($isPmax) return self::PLATFORM_GOOGLE_PERFORMANCE_MAX;
            if ($isShopping) return self::PLATFORM_GOOGLE_SHOPPING_ADS;
            if ($isDiscovery) return self::PLATFORM_GOOGLE_DISCOVERY_ADS;
            if ($isApp) return self::PLATFORM_GOOGLE_APP_ADS;
            if ($isDisplay) return self::PLATFORM_GOOGLE_DISPLAY_ADS;
            if ($isYoutube) return self::PLATFORM_GOOGLE_YOUTUBE_ADS;
            return self::PLATFORM_GOOGLE_SEARCH_ADS;
        }

        // YouTube standalone
        if ($isYoutube) {
            return self::PLATFORM_GOOGLE_YOUTUBE_ADS;
        }

        return null;
    }

    private function determineOrganicPlatform(string $trafficChannel): string
    {
        return match ($trafficChannel) {
            self::CHANNEL_DIRECT => self::PLATFORM_DIRECT,
            self::CHANNEL_ORGANIC_SEARCH => self::PLATFORM_ORGANIC_SEARCH,
            self::CHANNEL_ORGANIC_SOCIAL => self::PLATFORM_ORGANIC_SOCIAL,
            self::CHANNEL_ORGANIC_VIDEO => self::PLATFORM_ORGANIC_VIDEO,
            self::CHANNEL_EMAIL => self::PLATFORM_EMAIL,
            self::CHANNEL_AFFILIATE => self::PLATFORM_AFFILIATE,
            self::CHANNEL_REFERRAL => self::PLATFORM_REFERRAL,
            default => self::PLATFORM_OTHER,
        };
    }

    private function determinePlatformFromClickId(array $clickIdData, string $landingDomainLower): string
    {
        $clickId = $clickIdData['clickId'];
        $clickIdParam = $clickIdData['clickIdParam'];

        if (!empty($clickIdParam) && isset(self::CLICK_ID_PLATFORMS[$clickIdParam])) {
            return self::CLICK_ID_PLATFORMS[$clickIdParam];
        }

        if (empty($clickId)) {
            return self::PLATFORM_OTHER_PAID;
        }

        $domainPlatformMap = [
            'tiktok' => self::PLATFORM_TIKTOK_ADS,
            'facebook' => self::PLATFORM_FACEBOOK_ADS,
            'fb.com' => self::PLATFORM_FACEBOOK_ADS,
            'instagram' => self::PLATFORM_INSTAGRAM_ADS,
            'snapchat' => self::PLATFORM_SNAPCHAT_ADS,
            'snap.com' => self::PLATFORM_SNAPCHAT_ADS,
            'linkedin' => self::PLATFORM_LINKEDIN_ADS,
            'twitter' => self::PLATFORM_TWITTER_ADS,
            'x.com' => self::PLATFORM_TWITTER_ADS,
            'pinterest' => self::PLATFORM_PINTEREST_ADS,
            'reddit' => self::PLATFORM_REDDIT_ADS,
            'google' => self::PLATFORM_GOOGLE_SEARCH_ADS,
            'bing' => self::PLATFORM_MICROSOFT_ADS,
            'amazon' => self::PLATFORM_AMAZON_ADS,
        ];

        foreach ($domainPlatformMap as $domain => $platform) {
            if ($this->contains($landingDomainLower, $domain)) {
                return $platform;
            }
        }

        return self::PLATFORM_OTHER_PAID;
    }

    private function contains(string $haystack, string $needle): bool
    {
        if ($needle === '') {
            return false;
        }
        return mb_stripos($haystack, $needle) !== false;
    }

    public function retryUntil(): DateTime
    {
        return now()->addMinutes(30);
    }
}
