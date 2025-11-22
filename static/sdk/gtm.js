// Google Tag Manager Loader
// Reads container IDs from window.GTM_CONTAINER_IDS and loads each GTM container
(function() {
  var containerIds = window.GTM_CONTAINER_IDS || [];

  if (!containerIds.length) return;

  // Initialize dataLayer if not exists
  window.dataLayer = window.dataLayer || [];

  // Load each GTM container
  containerIds.forEach(function(containerId, index) {
    // Push gtm.start event (only once for the first container)
    if (index === 0) {
      window.dataLayer.push({
        'gtm.start': new Date().getTime(),
        event: 'gtm.js'
      });
    }

    // Create and inject script tag for this container
    var f = document.getElementsByTagName('script')[0];
    var j = document.createElement('script');
    j.async = true;
    j.src = 'https://edge.kepixel.com/gtm/gtm.js?id=' + containerId;
    f.parentNode.insertBefore(j, f);
  });
})();
