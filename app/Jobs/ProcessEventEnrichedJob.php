<?php

namespace App\Jobs;

use ClickHouseDB\Client;
use DateTime;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

class ProcessEventEnrichedJob implements ShouldQueue
{
    use Queueable;

    /**
     * The number of times the job may be attempted.
     */
    public int $tries = 10;

    /**
     * The number of seconds the job can run before timing out.
     */
    public int $timeout = 500;

    /**
     * The number of seconds to wait before retrying the job.
     */
    public array $backoff = [5, 15, 30, 60, 120];

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
        // Meta
        'facebook', 'fb', 'instagram', 'ig', 'meta',
        // TikTok
        'tiktok', 'tt',
        // Snapchat
        'snap', 'snapchat', 'sc',
        // Search
        'google', 'googleads', 'bing', 'bingads', 'yahoo', 'microsoft',
        // LinkedIn
        'linkedin', 'li',
        // Twitter/X
        'twitter', 'x',
        // Pinterest
        'pinterest', 'pin',
        // Reddit
        'reddit',
        // Amazon
        'amazon', 'amzn',
        // Native ads
        'criteo', 'outbrain', 'taboola',
    ];

    private const PAID_MEDIUMS = [
        'paid', 'cpc', 'ppc', 'cpv', 'cpa', 'cpm',
        'display', 'paid_social', 'remarketing', 'retargeting',
        'banner', 'native', 'sponsored', 'promoted',
        'shopping', 'pla', // Product Listing Ads
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

    // Platform constants - Meta
    private const PLATFORM_INSTAGRAM_REELS_ADS = 'instagram_reels_ads';
    private const PLATFORM_FACEBOOK_REELS_ADS = 'facebook_reels_ads';
    private const PLATFORM_INSTAGRAM_ADS = 'instagram_ads';
    private const PLATFORM_META_ADS = 'meta_ads';
    private const PLATFORM_META_ADVANTAGE_PLUS = 'meta_advantage_plus';
    private const PLATFORM_FACEBOOK_ADS = 'facebook_ads';

    // Platform constants - TikTok
    private const PLATFORM_TIKTOK_SPARK_ADS = 'tiktok_spark_ads';
    private const PLATFORM_TIKTOK_CREATOR_ADS = 'tiktok_creator_ads';
    private const PLATFORM_TIKTOK_TOPVIEW = 'tiktok_topview';
    private const PLATFORM_TIKTOK_ADS = 'tiktok_ads';

    // Platform constants - Snapchat
    private const PLATFORM_SNAPCHAT_ADS = 'snapchat_ads';

    // Platform constants - Google
    private const PLATFORM_GOOGLE_PERFORMANCE_MAX = 'google_performance_max';
    private const PLATFORM_GOOGLE_DISPLAY_ADS = 'google_display_ads';
    private const PLATFORM_GOOGLE_YOUTUBE_ADS = 'google_youtube_ads';
    private const PLATFORM_GOOGLE_SEARCH_ADS = 'google_search_ads';
    private const PLATFORM_GOOGLE_SHOPPING_ADS = 'google_shopping_ads';
    private const PLATFORM_GOOGLE_DISCOVERY_ADS = 'google_discovery_ads';
    private const PLATFORM_GOOGLE_APP_ADS = 'google_app_ads';

    // Platform constants - Microsoft/Bing
    private const PLATFORM_MICROSOFT_ADS = 'microsoft_ads';
    private const PLATFORM_BING_SHOPPING_ADS = 'bing_shopping_ads';

    // Platform constants - LinkedIn
    private const PLATFORM_LINKEDIN_ADS = 'linkedin_ads';
    private const PLATFORM_LINKEDIN_SPONSORED_CONTENT = 'linkedin_sponsored_content';
    private const PLATFORM_LINKEDIN_INMAIL = 'linkedin_inmail';

    // Platform constants - Twitter/X
    private const PLATFORM_TWITTER_ADS = 'twitter_ads';

    // Platform constants - Pinterest
    private const PLATFORM_PINTEREST_ADS = 'pinterest_ads';

    // Platform constants - Reddit
    private const PLATFORM_REDDIT_ADS = 'reddit_ads';

    // Platform constants - Amazon
    private const PLATFORM_AMAZON_ADS = 'amazon_ads';

    // Platform constants - Native advertising
    private const PLATFORM_CRITEO_ADS = 'criteo_ads';
    private const PLATFORM_OUTBRAIN_ADS = 'outbrain_ads';
    private const PLATFORM_TABOOLA_ADS = 'taboola_ads';

    // Platform constants - Other paid
    private const PLATFORM_OTHER_PAID = 'other_paid';

    // Platform constants - Organic/Non-paid
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

    // Click ID to inferred UTM data (source, medium, channel)
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
     */
    public function __construct(
        public string  $teamId,
        public string  $sourceId,
        public ?string $eventName,
        public ?string $eventType,
        public ?string $userId,
        public ?string $anonymousId,
        public ?string $messageId,
        public ?string $sessionId,
        public ?string $rudderId,
        public array   $properties,
        public string  $eventTimestamp,
    )
    {
    }

    /**
     * Execute the job.
     * @throws Throwable
     */
    public function handle(): void
    {
        $client = app(Client::class);

        // Validate and extract context data
        $context = is_array($this->properties['context'] ?? null) ? $this->properties['context'] : [];
        $page = is_array($context['page'] ?? null) ? $context['page'] : [];
        $campaign = is_array($context['campaign'] ?? null) ? $context['campaign'] : [];

        $pageData = $this->extractPageData($page);
        $utmData = $this->parseUtmParameters($page, $campaign);
        $clickIdData = $this->resolveClickId($utmData['queryParams'], $context);
        $clickId = $clickIdData['clickId'];

        // Enrich UTM data from click ID if UTM params are missing
        $utmData = $this->enrichUtmFromClickId($utmData, $clickIdData);

        $isDirect = $this->determineIsDirect($pageData, $utmData, $clickId);
        $isPaid = $this->determineIsPaid($utmData, $clickId);
        $trafficChannel = $this->determineTrafficChannel($utmData, $pageData, $clickIdData, $isDirect, $isPaid);
        $platform = $this->determinePlatform($utmData, $pageData, $clickIdData, $isPaid, $trafficChannel);

        try {
            $client->insert(
            'event_enriched',
            [
                [
                    substr($this->eventTimestamp, 0, 10),
                    $this->eventTimestamp,
                    $this->teamId,
                    $this->sourceId,
                    $this->eventName ?? '',
                    $this->eventType ?? '',
                    $this->userId ?? '',
                    $this->anonymousId ?? '',
                    $this->messageId ?? '',
                    $this->sessionId ?? '',
                    $this->rudderId ?? '',
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
                ],
            ],
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

            // Process identify events for identity mapping and user profiles
            if ($this->eventType === 'identify') {
                $this->processIdentifyEvent($client);
            }
        } catch (Throwable $e) {
            report($e);
            throw $e;
        }
    }

    /**
     * Process identify event to extract identity mappings and user profiles.
     */
    private function processIdentifyEvent(Client $client): void
    {
        // 1. Insert identity mapping if both user_id and anonymous_id are present
        if (!empty($this->userId) && !empty($this->anonymousId)) {
            try {
                $client->insert(
                    'identity_mappings',
                    [[
                        $this->teamId,
                        $this->anonymousId,
                        $this->userId,
                        $this->eventTimestamp,  // first_seen_at
                        $this->eventTimestamp,  // last_seen_at
                    ]],
                    ['team_id', 'anonymous_id', 'user_id', 'first_seen_at', 'last_seen_at']
                );
            } catch (Throwable $e) {
                // Log but don't fail the main job
                report($e);
            }
        }

        // 2. Extract and insert user profile from traits
        $traits = $this->properties['traits'] ?? $this->properties;

        // Only create profile if we have some identity
        if (empty($this->userId) && empty($this->anonymousId)) {
            return;
        }

        $canonicalUserId = $this->userId ?: 'anon_' . $this->anonymousId;

        try {
            $client->insert(
                'user_profiles',
                [[
                    $this->teamId,
                    $canonicalUserId,
                    $traits['email'] ?? null,
                    $traits['phone'] ?? null,
                    $traits['name'] ?? null,
                    $traits['username'] ?? null,
                    $traits['firstName'] ?? $traits['first_name'] ?? null,
                    $traits['lastName'] ?? $traits['last_name'] ?? null,
                    $traits['avatar'] ?? null,
                    $this->eventTimestamp,  // first_seen
                    $this->eventTimestamp,  // last_seen
                    json_encode($traits),   // all traits as JSON
                ]],
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
            // Log but don't fail the main job
            report($e);
        }
    }

    /**
     * Extract page-related data from the page context.
     */
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

    /**
     * Parse UTM parameters from query string and campaign context.
     */
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

    /**
     * Resolve the click ID from various platform-specific parameters.
     * Returns both the click ID value and the parameter name for platform detection.
     */
    private function resolveClickId(array $queryParams, array $context): array
    {
        $clickIdParams = [
            // Meta
            'fbclid' => $queryParams['fbclid'] ?? $context['facebook_click_id'] ?? '',
            // TikTok
            'ttclid' => $queryParams['ttclid'] ?? $context['tiktok_click_id'] ?? '',
            // Google
            'gclid' => $queryParams['gclid'] ?? $context['google_click_id'] ?? '',
            'gbraid' => $queryParams['gbraid'] ?? '',
            'wbraid' => $queryParams['wbraid'] ?? $context['meta_click_id'] ?? '',
            // Microsoft/Bing
            'msclkid' => $queryParams['msclkid'] ?? $context['microsoft_click_id'] ?? '',
            // LinkedIn
            'li_fat_id' => $queryParams['li_fat_id'] ?? $context['linkedin_click_id'] ?? '',
            // Twitter/X
            'twclid' => $queryParams['twclid'] ?? $context['twitter_click_id'] ?? '',
            // Pinterest
            'epik' => $queryParams['epik'] ?? $context['pinterest_click_id'] ?? '',
            // Snapchat
            'ScCid' => $queryParams['ScCid'] ?? $context['snapchat_click_id'] ?? '',
            'scid' => $queryParams['scid'] ?? $context['snapchat_scid'] ?? '',
            // Reddit
            'rdt_cid' => $queryParams['rdt_cid'] ?? $context['reddit_click_id'] ?? '',
            // Criteo
            'crto_pid' => $queryParams['crto_pid'] ?? '',
            // Outbrain
            'obOrigUrl' => $queryParams['obOrigUrl'] ?? '',
            // Taboola
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

        return [
            'clickId' => '',
            'clickIdParam' => '',
        ];
    }

    /**
     * Enrich UTM data from click ID when UTM parameters are missing.
     * This ensures proper attribution even when marketers forget to add UTM tags.
     */
    private function enrichUtmFromClickId(array $utmData, array $clickIdData): array
    {
        $clickIdParam = $clickIdData['clickIdParam'];

        // Only enrich if we have a click ID and UTM source is missing
        if (empty($clickIdParam) || !empty($utmData['utmSource'])) {
            return $utmData;
        }

        // Get attribution data for this click ID
        if (!isset(self::CLICK_ID_ATTRIBUTION[$clickIdParam])) {
            return $utmData;
        }

        $attribution = self::CLICK_ID_ATTRIBUTION[$clickIdParam];

        // Enrich missing UTM fields (don't overwrite existing values)
        if (empty($utmData['utmSource'])) {
            $utmData['utmSource'] = $attribution['source'];
        }

        if (empty($utmData['utmMedium'])) {
            $utmData['utmMedium'] = $attribution['medium'];
        }

        return $utmData;
    }

    /**
     * Determine if the traffic is paid based on UTM parameters and click IDs.
     */
    private function determineIsPaid(array $utmData, string $clickId): int
    {
        $utmSourceLower = mb_strtolower($utmData['utmSource']);
        $utmMediumLower = mb_strtolower($utmData['utmMedium']);

        // Check exact medium match
        if (in_array($utmMediumLower, self::PAID_MEDIUMS, true)) {
            return 1;
        }

        // Check medium contains paid keywords
        $paidMediumKeywords = ['cpc', 'ppc', 'cpv', 'cpa', 'cpm', 'paid', 'remarketing', 'retargeting', 'sponsored'];
        foreach ($paidMediumKeywords as $keyword) {
            if ($this->contains($utmMediumLower, $keyword)) {
                return 1;
            }
        }

        // Any click ID means paid
        if (!empty($clickId)) {
            return 1;
        }

        // Paid source with campaign
        if (in_array($utmSourceLower, self::PAID_SOURCES, true) && $utmData['utmCampaign'] !== '') {
            return 1;
        }

        return 0;
    }

    /**
     * Determine if the traffic is direct (no referrer, no UTM, no click ID - typed URL or bookmark).
     */
    private function determineIsDirect(array $pageData, array $utmData, string $clickId = ''): int
    {
        // If there are UTM parameters, it's NOT direct (it's a campaign)
        if (!$this->hasNoUtmParameters($utmData)) {
            return 0;
        }

        // If there's a click ID, it's NOT direct (it's paid traffic)
        if (!empty($clickId)) {
            return 0;
        }

        $landingReferrer = $pageData['landingReferrer'];
        $landingReferringDomain = $pageData['landingReferringDomain'];
        $pageDomain = $pageData['pageDomain'];

        // Explicit direct marker from analytics SDK
        if ($landingReferrer === '$direct') {
            return 1;
        }

        // Empty referrer indicates direct traffic
        if ($landingReferrer === '' || $landingReferrer === null) {
            return 1;
        }

        // Self-referral (same domain) is treated as direct
        if (!empty($pageDomain) && !empty($landingReferringDomain)) {
            $pageDomainClean = $this->extractRootDomain($pageDomain);
            $referrerDomainClean = $this->extractRootDomain($landingReferringDomain);

            if ($pageDomainClean === $referrerDomainClean) {
                return 1;
            }
        }

        // Common direct indicators from various analytics platforms
        $directIndicators = [
            '$direct',
            '(direct)',
            'direct',
            '(none)',
            'none',
            '(not set)',
            'bookmark',
            'typed',
            'url_typed',
            'android-app://com.google.android.gm', // Gmail app (no referrer context)
        ];

        $referrerLower = mb_strtolower($landingReferrer);
        if (in_array($referrerLower, $directIndicators, true)) {
            return 1;
        }

        return 0;
    }

    /**
     * Check if there are no UTM parameters set.
     */
    private function hasNoUtmParameters(array $utmData): bool
    {
        return empty($utmData['utmSource'])
            && empty($utmData['utmMedium'])
            && empty($utmData['utmCampaign']);
    }

    /**
     * Extract root domain from a hostname (e.g., www.example.com -> example.com).
     */
    private function extractRootDomain(string $domain): string
    {
        $domain = mb_strtolower($domain);

        // Remove www. prefix
        if (str_starts_with($domain, 'www.')) {
            $domain = substr($domain, 4);
        }

        // Handle common subdomains
        $parts = explode('.', $domain);
        $count = count($parts);

        // If we have more than 2 parts, try to get the root domain
        // e.g., blog.example.com -> example.com
        // But keep things like co.uk intact
        if ($count > 2) {
            $tld = $parts[$count - 1];
            $sld = $parts[$count - 2];

            // Check for compound TLDs like co.uk, com.au, etc.
            $compoundTlds = ['co', 'com', 'net', 'org', 'edu', 'gov', 'ac'];
            if (in_array($sld, $compoundTlds, true) && strlen($tld) === 2) {
                // It's a compound TLD, get three parts
                if ($count > 3) {
                    return implode('.', array_slice($parts, -3));
                }
            } else {
                // Standard domain, get last two parts
                return implode('.', array_slice($parts, -2));
            }
        }

        return $domain;
    }

    /**
     * Determine the traffic channel based on source, paid status, and referrer.
     */
    private function determineTrafficChannel(array $utmData, array $pageData, array $clickIdData, int $isDirect, int $isPaid): string
    {
        $utmSourceLower = mb_strtolower($utmData['utmSource']);
        $utmMediumLower = mb_strtolower($utmData['utmMedium']);
        $utmSourcePlatformLower = mb_strtolower($utmData['utmSourcePlatform']);
        $landingReferrer = $pageData['landingReferrer'];
        $landingReferringDomain = $pageData['landingReferringDomain'];
        $clickIdParam = $clickIdData['clickIdParam'] ?? '';

        // Email channel (regardless of paid status)
        if (in_array($utmSourceLower, self::EMAIL_SOURCES, true)
            || in_array($utmMediumLower, self::EMAIL_MEDIUMS, true)) {
            return self::CHANNEL_EMAIL;
        }

        // Affiliate channel
        if (in_array($utmMediumLower, self::AFFILIATE_MEDIUMS, true)) {
            return self::CHANNEL_AFFILIATE;
        }

        // Paid channels
        if ($isPaid === 1) {
            // Shopping ads
            if ($this->contains($utmMediumLower, 'shopping')
                || $this->contains($utmMediumLower, 'pla')
                || $utmSourcePlatformLower === 'google_shopping') {
                return self::CHANNEL_PAID_SHOPPING;
            }

            // Video ads
            if ($utmSourcePlatformLower === 'youtube'
                || $this->contains($utmSourceLower, 'youtube')
                || $this->contains($utmMediumLower, 'video')) {
                return self::CHANNEL_PAID_VIDEO;
            }

            // Native ads
            if (in_array($utmSourceLower, ['criteo', 'outbrain', 'taboola'], true)
                || $this->contains($utmMediumLower, 'native')) {
                return self::CHANNEL_PAID_NATIVE;
            }

            // Paid search
            if (in_array($utmSourceLower, self::SEARCH_SOURCES, true)) {
                return self::CHANNEL_PAID_SEARCH;
            }

            // Paid social
            if (in_array($utmSourceLower, self::SOCIAL_SOURCES, true)) {
                return self::CHANNEL_PAID_SOCIAL;
            }

            // Fallback: Use click ID attribution if UTM source is still empty/unknown
            if (!empty($clickIdParam) && isset(self::CLICK_ID_ATTRIBUTION[$clickIdParam])) {
                return self::CLICK_ID_ATTRIBUTION[$clickIdParam]['channel'];
            }

            // Generic paid
            return self::CHANNEL_OTHER;
        }

        // Organic channels
        if ($isDirect === 1) {
            return self::CHANNEL_DIRECT;
        }

        // Organic video (YouTube without paid indicators)
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

        // Check referrer domain for organic search
        $searchDomains = ['google', 'bing', 'yahoo', 'duckduckgo', 'baidu', 'yandex'];
        foreach ($searchDomains as $domain) {
            if ($this->contains($landingReferringDomain, $domain)) {
                return self::CHANNEL_ORGANIC_SEARCH;
            }
        }

        // Check referrer domain for organic social
        $socialDomains = ['facebook', 'instagram', 'twitter', 'linkedin', 'pinterest', 'reddit', 'tiktok', 'snapchat'];
        foreach ($socialDomains as $domain) {
            if ($this->contains($landingReferringDomain, $domain)) {
                return self::CHANNEL_ORGANIC_SOCIAL;
            }
        }

        // Referral
        if ($landingReferrer !== ''
            && $landingReferrer !== '$direct'
            && !in_array($landingReferringDomain, ['', 'localhost'], true)
        ) {
            return self::CHANNEL_REFERRAL;
        }

        return self::CHANNEL_OTHER;
    }

    /**
     * Determine the advertising platform based on UTM parameters and click IDs.
     */
    private function determinePlatform(
        array  $utmData,
        array  $pageData,
        array  $clickIdData,
        int    $isPaid,
        string $trafficChannel
    ): string
    {
        // Non-paid traffic
        if ($isPaid !== 1) {
            return $this->determineOrganicPlatform($trafficChannel);
        }

        $utmSourceLower = mb_strtolower($utmData['utmSource']);
        $utmMediumLower = mb_strtolower($utmData['utmMedium']);
        $utmSourcePlatformLower = mb_strtolower($utmData['utmSourcePlatform']);
        $utmContentTypeLower = mb_strtolower($utmData['utmContentType']);
        $utmCampaignLower = mb_strtolower($utmData['utmCampaign']);
        $landingDomainLower = mb_strtolower($pageData['landingReferringDomain']);

        // Try to detect from UTM source first
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

        // Fallback to click ID based detection
        return $this->determinePlatformFromClickId($clickIdData, $landingDomainLower);
    }

    /**
     * Determine platform from UTM parameters.
     */
    private function determinePlatformFromUtm(
        string $utmSourceLower,
        string $utmMediumLower,
        string $utmSourcePlatformLower,
        string $utmContentTypeLower,
        string $utmCampaignLower
    ): ?string
    {
        $isReel = $this->contains($utmContentTypeLower, 'reel');
        $isSpark = $this->contains($utmContentTypeLower, 'spark');
        $isCreator = $this->contains($utmContentTypeLower, 'creator');
        $isTopview = $this->contains($utmContentTypeLower, 'topview') || $this->contains($utmContentTypeLower, 'brand_takeover');
        $isAdvantagePlus = $this->contains($utmCampaignLower, 'advantage') || $this->contains($utmCampaignLower, 'asc');
        $isTiktok = str_starts_with($utmSourceLower, 'tiktok') || $utmSourceLower === 'tt';
        $isYoutube = $utmSourcePlatformLower === 'youtube' || $this->contains($utmSourceLower, 'youtube');
        $isDisplay = $this->contains($utmMediumLower, 'display')
            || in_array($utmSourcePlatformLower, ['google_display', 'gdn'], true);
        $isShopping = $this->contains($utmMediumLower, 'shopping')
            || $this->contains($utmMediumLower, 'pla')
            || $utmSourcePlatformLower === 'google_shopping';
        $isDiscovery = $this->contains($utmCampaignLower, 'discovery')
            || $utmSourcePlatformLower === 'google_discovery';
        $isPmax = $this->contains($utmCampaignLower, 'pmax') || $this->contains($utmCampaignLower, 'performance_max');
        $isApp = $this->contains($utmCampaignLower, 'app') && $this->contains($utmSourceLower, 'google');

        // Meta/Facebook/Instagram platforms
        if (in_array($utmSourceLower, ['facebook', 'fb', 'meta', 'instagram', 'ig'], true)) {
            if ($isAdvantagePlus) {
                return self::PLATFORM_META_ADVANTAGE_PLUS;
            }
            if ($isReel && $this->contains($utmSourcePlatformLower, 'instagram')) {
                return self::PLATFORM_INSTAGRAM_REELS_ADS;
            }
            if ($isReel) {
                return self::PLATFORM_FACEBOOK_REELS_ADS;
            }
            if (in_array($utmSourceLower, ['instagram', 'ig'], true)) {
                return self::PLATFORM_INSTAGRAM_ADS;
            }
            if ($utmSourceLower === 'meta') {
                return self::PLATFORM_META_ADS;
            }
            return self::PLATFORM_FACEBOOK_ADS;
        }

        // TikTok platforms
        if ($isTiktok) {
            if ($isTopview) {
                return self::PLATFORM_TIKTOK_TOPVIEW;
            }
            if ($isSpark) {
                return self::PLATFORM_TIKTOK_SPARK_ADS;
            }
            if ($isCreator) {
                return self::PLATFORM_TIKTOK_CREATOR_ADS;
            }
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
        if ($utmSourceLower === 'criteo') {
            return self::PLATFORM_CRITEO_ADS;
        }
        if ($utmSourceLower === 'outbrain') {
            return self::PLATFORM_OUTBRAIN_ADS;
        }
        if ($utmSourceLower === 'taboola') {
            return self::PLATFORM_TABOOLA_ADS;
        }

        // Microsoft/Bing
        if (in_array($utmSourceLower, ['bing', 'bingads', 'microsoft'], true)) {
            if ($isShopping) {
                return self::PLATFORM_BING_SHOPPING_ADS;
            }
            return self::PLATFORM_MICROSOFT_ADS;
        }

        // Google platforms
        if (in_array($utmSourceLower, ['google', 'googleads'], true)) {
            if ($isPmax) {
                return self::PLATFORM_GOOGLE_PERFORMANCE_MAX;
            }
            if ($isShopping) {
                return self::PLATFORM_GOOGLE_SHOPPING_ADS;
            }
            if ($isDiscovery) {
                return self::PLATFORM_GOOGLE_DISCOVERY_ADS;
            }
            if ($isApp) {
                return self::PLATFORM_GOOGLE_APP_ADS;
            }
            if ($isDisplay) {
                return self::PLATFORM_GOOGLE_DISPLAY_ADS;
            }
            if ($isYoutube) {
                return self::PLATFORM_GOOGLE_YOUTUBE_ADS;
            }
            return self::PLATFORM_GOOGLE_SEARCH_ADS;
        }

        // YouTube (standalone)
        if ($isYoutube) {
            return self::PLATFORM_GOOGLE_YOUTUBE_ADS;
        }

        return null;
    }

    /**
     * Determine platform for non-paid traffic.
     */
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

    /**
     * Determine platform based on click ID and landing domain.
     */
    private function determinePlatformFromClickId(array $clickIdData, string $landingDomainLower): string
    {
        $clickId = $clickIdData['clickId'];
        $clickIdParam = $clickIdData['clickIdParam'];

        // First try exact click ID param match
        if (!empty($clickIdParam) && isset(self::CLICK_ID_PLATFORMS[$clickIdParam])) {
            return self::CLICK_ID_PLATFORMS[$clickIdParam];
        }

        // No click ID at all
        if (empty($clickId)) {
            return self::PLATFORM_OTHER_PAID;
        }

        // Fallback to domain-based detection
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

    /**
     * Check if a string contains a substring (case-insensitive).
     */
    private function contains(string $haystack, string $needle): bool
    {
        if ($needle === '') {
            return false;
        }
        return mb_stripos($haystack, $needle) !== false;
    }

    /**
     * Determine if the job should retry on the given exception.
     */
    public function retryUntil(): DateTime
    {
        return now()->addMinutes(10);
    }
}
