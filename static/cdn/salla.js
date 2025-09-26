(function () {
    if (!window.dataLayer || !Array.isArray(window.dataLayer)) {
        return;
    }

    const TRACKED_EVENTS = [
        // 'Product Reviewed',
        // 'Product Shared',
        // 'Cart Shared',
        'Product Added to Wishlist',
        'Product Removed from Wishlist',
        'Wishlist Product Added to Cart',
        // 'Coupon Entered',
        // 'Coupon Applied',
        // 'Coupon Denied',
        // 'Coupon Removed',
        'Product Clicked',
        'Product Viewed',
        // 'Product Added',
        'Product Removed',
        'Cart Viewed',
        // 'Cart Updated',
        'Checkout Started',
        // 'Checkout Step Viewed',
        // 'Checkout Step Completed',
        // 'Payment Info Entered',
        // 'Payment Failed',
        // 'Order Updated',
        // 'Order Completed',
        // 'Order Refunded',
        // 'Order Cancelled',
        // 'Products Searched',
        'Product List Viewed',
        'Product List Filtered',
        // 'Product List Sorted'
    ];
    const CURRENCY_COOKIE_KEY = 'kepixel_currency';
    const CURRENCY_COOKIE_MAX_AGE = 60 * 60 * 24 * 30; // 30 days
    const PROCESS_DELAY_MS = 2000;
    const DEDUP_KEYS = ['client_dedup_id', 'position'];
    const DEFAULT_CONTENT_TYPE = 'product';

    function setCurrencyCookie(value) {
        if (!value || typeof document === 'undefined') {
            return;
        }

        document.cookie = CURRENCY_COOKIE_KEY + '=' + encodeURIComponent(value)
            + ';path=/'
            + ';max-age=' + CURRENCY_COOKIE_MAX_AGE
            + ';SameSite=Lax';
    }

    function getCurrencyCookie() {
        if (typeof document === 'undefined' || !document.cookie) {
            return null;
        }

        const match = document.cookie.match(new RegExp('(?:^|; )' + CURRENCY_COOKIE_KEY + '=([^;]*)'));
        return match ? decodeURIComponent(match[1]) : null;
    }

    function captureCurrencyFromEvent(eventData) {
        const ecommerceCurrency = eventData?.ecommerce?.currencyCode;
        if (ecommerceCurrency) {
            setCurrencyCookie(ecommerceCurrency);
            return ecommerceCurrency;
        }

        return getCurrencyCookie();
    }

    function applyCurrencyFromPayload(payload, currentCurrency) {
        if (!payload || typeof payload !== 'object') {
            return currentCurrency;
        }

        const payloadCurrency = payload.currency || payload?.properties?.currency;
        if (!payloadCurrency) {
            return currentCurrency;
        }

        if (payloadCurrency !== currentCurrency) {
            setCurrencyCookie(payloadCurrency);
        }

        return payloadCurrency;
    }

    function ensurePayloadDefaults(payload, currency) {
        if (!payload || typeof payload !== 'object') {
            return;
        }

        if (currency != null && !('currency' in payload)) {
            payload.currency = currency;
        }

        if (!('content_type' in payload)) {
            payload.content_type = DEFAULT_CONTENT_TYPE;
        }
    }

    function getUserProperties() {
        const salla = window.Salla || window.salla || {};
        if (!salla.config) {
            return null;
        }

        return {
            id: salla.config.get('user.id'),
            email: salla.config.get('user.email'),
            phone: salla.config.get('user.mobile'),
            firstname: salla.config.get('user.first_name'),
            lastname: salla.config.get('user.last_name'),
            country: salla.config.get('user.country_code')
        };
    }

    function extractPayload(eventData) {
        if (eventData[0] !== undefined) {
            return eventData[0];
        }

        const payload = Object.assign({}, eventData);
        delete payload.event;
        return payload;
    }

    function hasDeduplicationMarkers(payload) {
        if (!payload || typeof payload !== 'object') {
            return false;
        }

        if (DEDUP_KEYS.some((key) => payload[key] != null)) {
            return true;
        }

        const properties = payload.properties;
        if (properties && typeof properties === 'object' && DEDUP_KEYS.some((key) => properties[key] != null)) {
            return true;
        }

        return DEDUP_KEYS.some((key) => Object.prototype.hasOwnProperty.call(payload, key));
    }

    function ensureAnalyticsQueue() {
        if (!window.kepixelAnalytics) {
            window.kepixelAnalytics = [];
        }

        return window.kepixelAnalytics;
    }

    function queueAnalyticsCommand(command) {
        ensureAnalyticsQueue().push(command);
    }

    function aliasUser(userId) {
        if (!userId) {
            return;
        }

        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.alias === 'function') {
            window.kepixelAnalytics.alias(userId);
        } else {
            queueAnalyticsCommand(['alias', userId]);
        }
    }

    function identifyUser(user) {
        if (!user || !user.id) {
            return;
        }

        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.identify === 'function') {
            window.kepixelAnalytics.identify(user.id, user);
        } else {
            queueAnalyticsCommand(['identify', user.id, user]);
        }
    }

    function trackEvent(eventName, payload, user) {
        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
            if (user && user.id) {
                window.kepixelAnalytics.identify(user.id, user);
            }
            window.kepixelAnalytics.track(eventName, payload);
        } else {
            if (user && user.id) {
                queueAnalyticsCommand(['identify', user.id, user]);
            }
            queueAnalyticsCommand(['track', eventName, payload]);
        }
    }

    function handleAuthEvent(user) {
        if (!user || !user.id) {
            return;
        }

        aliasUser(user.id);
        identifyUser(user);
    }

    function handlePageView(user) {
        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.page === 'function') {
            window.kepixelAnalytics.page();
            if (user && user.id) {
                window.kepixelAnalytics.identify(user.id, user);
            }
        } else {
            queueAnalyticsCommand(['page']);
            if (user && user.id) {
                queueAnalyticsCommand(['identify', user.id, user]);
            }
        }
    }

    function buildAddToCartPayload(eventData, currency) {
        const ecommerce = eventData?.ecommerce || {};
        const product = ecommerce.add.products[0];

        return {
            currency: currency,
            value: product.price,
            content_id: product.id,
            content_type: 'product',
            items: [{
                item_id: product.id.toString(),
                item_name: product.name,
                item_category: product.category,
                item_brand: product.brand,
                quantity: product.quantity,
                price: product.price
            }]
        };
    }

    function handleAddToCart(eventData, user, currency) {
        const finalCurrency = currency != null ? currency : eventData?.ecommerce?.currencyCode;
        const payload = buildAddToCartPayload(eventData, finalCurrency);

        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
            if (user && user.id) {
                window.kepixelAnalytics.identify(user.id, user);
            }
            window.kepixelAnalytics.track('Product Added', payload);
        } else {
            if (user && user.id) {
                queueAnalyticsCommand(['alias', user.id]);
                queueAnalyticsCommand(['identify', user.id, user]);
            }
            queueAnalyticsCommand(['Product Added', payload]);
        }
    }

    function buildPurchasePayload(eventData, currency) {
        const ecommerce = eventData?.ecommerce || {};
        const purchase = ecommerce.purchase || {};
        const actionField = purchase.actionField || {};
        const products = Array.isArray(purchase.products) ? purchase.products : [];

        return {
            currency: currency,
            value: actionField.total,
            transaction_id: ecommerce.event_id,
            order_id: ecommerce.event_id,
            items: products.map(function (item) {
                return {
                    item_id: item.id,
                    item_name: item.name,
                    item_category: item.category,
                    quantity: item.quantity,
                    price: item.price,
                    discount: item.discount
                };
            })
        };
    }

    function handlePurchase(eventData, user, currency) {
        const finalCurrency = currency != null ? currency : eventData?.ecommerce?.currencyCode;
        const payload = buildPurchasePayload(eventData, finalCurrency);

        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
            if (user && user.id) {
                window.kepixelAnalytics.identify(user.id, user);
            }
            window.kepixelAnalytics.track('Order Completed', payload);
        } else {
            if (user && user.id) {
                queueAnalyticsCommand(['alias', user.id]);
                queueAnalyticsCommand(['identify', user.id, user]);
            }
            queueAnalyticsCommand(['Order Completed', payload]);
        }
    }

    function isTrackedEvent(eventName) {
        return !!eventName && TRACKED_EVENTS.includes(eventName);
    }

    function processTrackedEvent(eventData, user, currency) {
        const payload = extractPayload(eventData);

        if (hasDeduplicationMarkers(payload)) {
            return currency;
        }

        const updatedCurrency = applyCurrencyFromPayload(payload, currency);
        ensurePayloadDefaults(payload, updatedCurrency);
        trackEvent(eventData.event, payload, user);

        return updatedCurrency;
    }

    function processDataLayerEvent(eventData) {
        if (!eventData) {
            return;
        }

        const user = getUserProperties();
        let currency = captureCurrencyFromEvent(eventData);

        if (isTrackedEvent(eventData.event)) {
            currency = processTrackedEvent(eventData, user, currency);
        }

        if (eventData.event === 'auth::logged.in' || eventData.event === 'auth::registration.success') {
            handleAuthEvent(user);
        }

        if (eventData.event === 'page.view') {
            handlePageView(user);
        }

        if (eventData.event === 'purchase') {
            handlePurchase(eventData, user, currency || getCurrencyCookie());
        }
        if (eventData.event === 'addToCart') {
            handleAddToCart(eventData, user, currency || getCurrencyCookie());
        }
    }

    const queuedEvents = [];
    let isReady = false;

    function flushQueuedEvents() {
        isReady = true;

        if (!queuedEvents.length) {
            return;
        }

        const snapshot = queuedEvents.splice(0, queuedEvents.length);
        snapshot.forEach(processDataLayerEvent);
    }

    function enqueueOrProcessEvent(eventData) {
        if (!isReady) {
            if (eventData && eventData.route === 'auth.token.session') {
                flushQueuedEvents();
            }

            queuedEvents.push(eventData);
            return;
        }

        processDataLayerEvent(eventData);
    }

    setTimeout(function () {
        flushQueuedEvents();
    }, PROCESS_DELAY_MS);

    window.dataLayer.forEach(function (eventData) {
        enqueueOrProcessEvent(eventData);
    });

    const originalPush = window.dataLayer.push;
    window.dataLayer.push = function () {
        Array.prototype.forEach.call(arguments, function (eventData) {
            try {
                enqueueOrProcessEvent(eventData);
            } catch (error) {
                console.error('Error processing event:', error, 'Event:', eventData);
            }
        });

        return originalPush.apply(this, arguments);
    };
})();
