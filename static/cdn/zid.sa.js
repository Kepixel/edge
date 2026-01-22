(function () {
    if (!window.dataLayer || !Array.isArray(window.dataLayer)) {
        return;
    }

    const PROCESS_DELAY_MS = 2000;


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

    function trackEvent(eventName, payload, user) {
        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
            window.kepixelAnalytics.track(eventName, payload);
        } else {
            queueAnalyticsCommand(['track', eventName, payload]);
        }
    }

    function handlePageView(user) {
        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.page === 'function') {
            window.kepixelAnalytics.page();
        } else {
            queueAnalyticsCommand(['page']);
        }
    }
    function processDataLayerEvent(eventData) {
        console.log(eventData)
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

        // if (!isReady) {
        //     if (eventData) {
        //         flushQueuedEvents();
        //     }
        //
        //     queuedEvents.push(eventData);
        //     return;
        // }

        processDataLayerEvent(eventData);
    }

    // setTimeout(function () {
    //     flushQueuedEvents();
    // }, PROCESS_DELAY_MS);

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
