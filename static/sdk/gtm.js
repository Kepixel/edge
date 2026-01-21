// Google Tag Manager Loader
// Reads container IDs from window.GTM_CONTAINER_IDS and loads each GTM container
(function() {
    var containerIds = window.GTM_CONTAINER_IDS || [];

    if (!containerIds.length) return;

    // Initialize dataLayer if not exists
    window.dataLayer = window.dataLayer || [];

    // Track which containers are already loaded to avoid duplicates
    window.__kepLoadedGTM = window.__kepLoadedGTM || {};

    // Check if a GTM container is already loaded on the page
    function isContainerLoaded(containerId) {
        // Check our own tracking
        if (window.__kepLoadedGTM[containerId]) return true;

        // Check for existing GTM script tags (from user's own installation)
        var scripts = document.getElementsByTagName('script');
        for (var i = 0; i < scripts.length; i++) {
            var src = scripts[i].src || '';
            if (src.indexOf('gtm.js') !== -1 && src.indexOf(containerId) !== -1) {
                return true;
            }
        }
        return false;
    }

    // Check if gtm.start has already been pushed to dataLayer
    function hasGtmStart() {
        for (var i = 0; i < window.dataLayer.length; i++) {
            if (window.dataLayer[i] && window.dataLayer[i]['gtm.start']) {
                return true;
            }
        }
        return false;
    }

    // Push gtm.start event only if not already present
    if (!hasGtmStart()) {
        window.dataLayer.push({
            'gtm.start': new Date().getTime(),
            event: 'gtm.js'
        });
    }

    // Load each GTM container
    containerIds.forEach(function(containerId) {
        // Skip if this container is already loaded
        if (isContainerLoaded(containerId)) {
            return;
        }

        // Mark as loaded
        window.__kepLoadedGTM[containerId] = true;

        // Create and inject script tag for this container
        var f = document.getElementsByTagName('script')[0];
        var j = document.createElement('script');
        j.async = true;
        j.src = 'https://edge.kepixel.com/gtm/gtm.js?id=' + containerId;
        f.parentNode.insertBefore(j, f);
    });
})();
