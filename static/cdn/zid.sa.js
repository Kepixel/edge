(function () {
    'use strict';

    // ============================================
    // CONFIGURATION
    // ============================================

    var CONFIG = {
        PROCESS_DELAY_MS: 2000,
        DEBUG: false
    };

    // ============================================
    // STATE
    // ============================================

    var state = {
        formsStarted: {},
        formsViewed: {},
        formsSubmitted: {},
        isReady: false,
        queuedEvents: []
    };

    // ============================================
    // UTILITY FUNCTIONS
    // ============================================

    function log() {
        if (CONFIG.DEBUG && console && console.log) {
            console.log.apply(console, ['[Kepixel Zid.sa]'].concat(Array.prototype.slice.call(arguments)));
        }
    }

    function getLanguage() {
        var path = window.location.pathname;
        if (path.indexOf('/ar') === 0 || path.indexOf('/ar/') !== -1) {
            return 'ar';
        }
        return 'en';
    }

    // ============================================
    // PAGE TYPE DETECTION
    // ============================================

    function detectPageType() {
        var path = window.location.pathname;
        var normalizedPath = path.replace(/^\/(en|ar)/, '');

        if (normalizedPath === '' || normalizedPath === '/') {
            return 'homepage';
        }
        if (/^\/pricing\/?$/.test(normalizedPath)) {
            return 'pricing';
        }
        if (/^\/solutions(\/|$)/.test(normalizedPath)) {
            return 'solutions';
        }
        if (/^\/customers(\/|$)/.test(normalizedPath)) {
            return 'customers';
        }
        if (/^\/switchers(\/|$)/.test(normalizedPath)) {
            return 'switchers';
        }
        if (/^\/enterprise\/?$/.test(normalizedPath)) {
            return 'enterprise';
        }
        if (/^\/why-zid(\/|$)/.test(normalizedPath)) {
            return 'why_zid';
        }
        if (/^\/partners\/?$/.test(normalizedPath)) {
            return 'partners';
        }
        return 'other';
    }

    // ============================================
    // ANALYTICS QUEUE
    // ============================================

    function ensureAnalyticsQueue() {
        if (!window.kepixelAnalytics) {
            window.kepixelAnalytics = [];
        }
        return window.kepixelAnalytics;
    }

    function queueAnalyticsCommand(command) {
        ensureAnalyticsQueue().push(command);
    }

    function track(eventName, payload) {
        log('Track:', eventName, payload);
        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
            window.kepixelAnalytics.track(eventName, payload);
        } else {
            queueAnalyticsCommand(['track', eventName, payload]);
        }
    }

    function page(category, name, properties) {
        log('Page:', category, name, properties);
        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.page === 'function') {
            if (category) {
                window.kepixelAnalytics.page(category, name, properties);
            } else {
                window.kepixelAnalytics.page();
            }
        } else {
            if (category) {
                queueAnalyticsCommand(['page', category, name, properties]);
            } else {
                queueAnalyticsCommand(['page']);
            }
        }
    }

    // ============================================
    // PAGE VIEW TRACKING
    // ============================================

    function trackPageView() {
        var pageType = detectPageType();
        var language = getLanguage();

        page(pageType, document.title, {
            page_type: pageType,
            page_url: window.location.href,
            page_path: window.location.pathname,
            referrer: document.referrer || null,
            language: language
        });
    }

    function handlePageView() {
        trackPageView();
        setupFormViewedTracking();
    }

    // ============================================
    // LINK CLICK TRACKING
    // ============================================

    function classifyLinkType(element, href) {
        var classes = (element.className || '').toLowerCase();
        var text = (element.textContent || element.innerText || '').toLowerCase().trim();

        if (classes.indexOf('cta') !== -1 ||
            classes.indexOf('btn-primary') !== -1 ||
            text.indexOf('get started') !== -1 ||
            text.indexOf('sign up') !== -1 ||
            text.indexOf('free trial') !== -1 ||
            text.indexOf('start') !== -1 ||
            text.indexOf('try') !== -1) {
            return 'cta';
        }

        if (/twitter|facebook|linkedin|instagram|youtube|tiktok/.test(href.toLowerCase())) {
            return 'social';
        }

        var footer = element.closest('footer');
        if (footer) {
            return 'footer';
        }

        var nav = element.closest('nav, header, [role="navigation"]');
        if (nav) {
            return 'navigation';
        }

        try {
            var linkUrl = new URL(href, window.location.origin);
            if (linkUrl.hostname !== window.location.hostname) {
                return 'external';
            }
        } catch (e) {}

        return 'internal';
    }

    function trackLinkClick(event) {
        var target = event.target;
        var link = target.closest('a, button[data-href]');

        if (!link) return;

        var href = link.href || link.getAttribute('data-href') || '';
        if (!href) return;

        if (href.indexOf('javascript:') === 0 || href === '#') return;

        var linkText = (link.textContent || link.innerText || '').trim().substring(0, 100);
        var linkType = classifyLinkType(link, href);

        track('Link Clicked', {
            link_url: href,
            link_text: linkText,
            link_type: linkType,
            link_id: link.id || null,
            link_class: link.className || null,
            page_url: window.location.href
        });
    }

    // ============================================
    // FORM TRACKING
    // ============================================

    function getFormIdentifier(form) {
        return form.id || form.name || form.action || 'form_' + Array.prototype.indexOf.call(document.forms, form);
    }

    function getFormName(form) {
        return form.getAttribute('data-form-name') ||
               form.getAttribute('name') ||
               form.id ||
               null;
    }

    function getFormType(form) {
        var action = (form.action || '').toLowerCase();
        var classes = (form.className || '').toLowerCase();
        var formName = (getFormName(form) || '').toLowerCase();

        if (/contact|inquiry|question/.test(formName + classes + action)) {
            return 'contact';
        }
        if (/newsletter|subscribe|email/.test(formName + classes + action)) {
            return 'newsletter';
        }
        if (/demo|trial|signup|register/.test(formName + classes + action)) {
            return 'signup';
        }
        if (/lead|download|whitepaper|ebook/.test(formName + classes + action)) {
            return 'lead';
        }
        return 'other';
    }

    function trackFormViewed(form) {
        var formId = getFormIdentifier(form);
        if (state.formsViewed[formId]) return;
        state.formsViewed[formId] = true;

        track('Form Viewed', {
            form_id: form.id || null,
            form_name: getFormName(form),
            form_type: getFormType(form),
            page_url: window.location.href
        });
    }

    function trackFormStarted(event) {
        var input = event.target;
        var form = input.closest('form');
        if (!form) return;

        var formId = getFormIdentifier(form);
        if (state.formsStarted[formId]) return;
        state.formsStarted[formId] = true;

        track('Form Started', {
            form_id: form.id || null,
            form_name: getFormName(form),
            form_type: getFormType(form),
            page_url: window.location.href,
            first_field: input.name || input.id || null
        });
    }

    function getFormFields(form) {
        var fields = {};
        var inputs = form.querySelectorAll('input, select, textarea');
        inputs.forEach(function(input) {
            var name = input.name || input.id;
            if (!name) return;
            // Skip sensitive fields
            if (/password|card|cvv|ssn|secret/i.test(name)) return;
            if (input.type === 'password') return;

            if (input.type === 'checkbox' || input.type === 'radio') {
                if (input.checked) {
                    fields[name] = input.value;
                }
            } else {
                fields[name] = input.value || '';
            }
        });
        return fields;
    }

    function trackFormSubmitted(event) {
        var form = event.target;
        if (!form || form.tagName !== 'FORM') return;

        var formId = getFormIdentifier(form);

        // Get form field values (excluding sensitive data)
        var fields = getFormFields(form);

        track('Form Submitted', {
            form_id: form.id || null,
            form_name: getFormName(form),
            form_type: getFormType(form),
            page_url: window.location.href,
            fields: fields
        });

        // Mark as submitted to avoid duplicates
        state.formsSubmitted[formId] = true;
    }

    function setupFormViewedTracking() {
        var forms = document.querySelectorAll('form');
        if (!forms.length) return;

        if ('IntersectionObserver' in window) {
            var observer = new IntersectionObserver(function(entries) {
                entries.forEach(function(entry) {
                    if (entry.isIntersecting) {
                        trackFormViewed(entry.target);
                    }
                });
            }, { threshold: 0.5 });

            forms.forEach(function(form) {
                observer.observe(form);
            });
        } else {
            forms.forEach(function(form) {
                trackFormViewed(form);
            });
        }
    }

    // ============================================
    // DATALAYER PROCESSING (GTM Integration)
    // ============================================

    function extractPayload(eventData) {
        if (eventData[0] !== undefined) {
            return eventData[0];
        }
        var payload = Object.assign({}, eventData);
        delete payload.event;
        return payload;
    }

    function processDataLayerEvent(eventData) {
        if (!eventData) return;

        // Handle GTM page view events
        if (eventData.event === 'gtm.dom' || eventData.event === 'gtm.historyChange-v2') {
            handlePageView();
        }
    }

    function flushQueuedEvents() {
        state.isReady = true;

        if (!state.queuedEvents.length) return;

        var snapshot = state.queuedEvents.splice(0, state.queuedEvents.length);
        snapshot.forEach(processDataLayerEvent);
    }

    function enqueueOrProcessEvent(eventData) {
        if (!state.isReady) {
            if (eventData) {
                flushQueuedEvents();
            }
            state.queuedEvents.push(eventData);
            return;
        }
        processDataLayerEvent(eventData);
    }

    function setupDataLayerIntegration() {
        if (!window.dataLayer || !Array.isArray(window.dataLayer)) {
            log('No dataLayer found, skipping GTM integration');
            return;
        }

        log('Setting up dataLayer integration');

        // Process existing dataLayer events
        window.dataLayer.forEach(function(eventData) {
            enqueueOrProcessEvent(eventData);
        });

        // Intercept future dataLayer.push calls
        var originalPush = window.dataLayer.push;
        window.dataLayer.push = function() {
            Array.prototype.forEach.call(arguments, function(eventData) {
                try {
                    enqueueOrProcessEvent(eventData);
                } catch (error) {
                    console.error('Error processing event:', error, 'Event:', eventData);
                }
            });
            return originalPush.apply(this, arguments);
        };

        // Delayed flush for queued events
        setTimeout(function() {
            flushQueuedEvents();
        }, CONFIG.PROCESS_DELAY_MS);
    }

    // ============================================
    // SPA NAVIGATION SUPPORT
    // ============================================

    function setupSpaSupport() {
        var lastUrl = window.location.href;

        var originalPushState = history.pushState;
        var originalReplaceState = history.replaceState;

        history.pushState = function() {
            originalPushState.apply(this, arguments);
            handleUrlChange();
        };

        history.replaceState = function() {
            originalReplaceState.apply(this, arguments);
            handleUrlChange();
        };

        window.addEventListener('popstate', handleUrlChange);

        function handleUrlChange() {
            var newUrl = window.location.href;
            if (newUrl !== lastUrl) {
                lastUrl = newUrl;
                // Only track if no dataLayer (GTM handles its own history changes)
                if (!window.dataLayer || !Array.isArray(window.dataLayer)) {
                    handlePageView();
                }
            }
        }
    }

    // ============================================
    // INITIALIZATION
    // ============================================

    function init() {
        log('Initializing Kepixel Zid.sa Tracker');

        // Track initial page view (if no dataLayer, we track immediately)
        if (!window.dataLayer || !Array.isArray(window.dataLayer)) {
            handlePageView();
        }

        // Setup dataLayer integration for GTM
        setupDataLayerIntegration();

        // Setup link click tracking
        document.addEventListener('click', trackLinkClick, true);

        // Setup form started tracking
        document.addEventListener('focus', trackFormStarted, true);

        // Setup form submitted tracking
        document.addEventListener('submit', trackFormSubmitted, true);

        // Setup form viewed tracking
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', function() {
                setupFormViewedTracking();
            });
        } else {
            setupFormViewedTracking();
        }

        // Setup SPA navigation support
        setupSpaSupport();

        log('Kepixel Zid.sa Tracker initialized');
    }

    // Start initialization
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Expose for debugging
    window._kepixelZidSa = {
        config: CONFIG,
        state: state,
        track: track,
        page: page,
        detectPageType: detectPageType,
        version: '2.1.0'
    };

})();
