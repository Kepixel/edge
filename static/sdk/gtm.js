// Google Tag Manager Loader
// Reads container IDs from window.GTM_CONTAINER_IDS and loads each GTM container
(function() {
  if (typeof window === 'undefined') return;

  // Internal state (shared across multiple inclusions of this file)
  var state = window.__kep_gtm_state = window.__kep_gtm_state || {
    baseUrl: 'https://edge.kepixel.com/gtm/gtm.js',
    dataLayerInitialized: false,
    loadedContainers: {} // map of containerId -> true
  };

  function normalizeIds(input) {
    if (!input) return [];
    var arr = [];
    if (Array.isArray(input)) {
      arr = input;
    } else if (typeof input === 'string') {
      // Allow comma/space separated strings
      arr = input.split(/[\s,]+/);
    } else {
      return [];
    }
    var out = [];
    for (var i = 0; i < arr.length; i++) {
      var id = (arr[i] || '').toString().trim();
      if (!id) continue;
      // Basic sanity: GTM IDs typically start with 'GTM-'
      if (id.indexOf('GTM-') === 0) {
        out.push(id);
      }
    }
    return out;
  }

  function ensureDataLayerInit() {
    if (state.dataLayerInitialized) return;
    // Initialize dataLayer once
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
      'gtm.start': new Date().getTime(),
      event: 'gtm.js'
    });
    state.dataLayerInitialized = true;
  }

  function findExistingGtmScript(containerId) {
    var scripts = document.getElementsByTagName('script');
    for (var i = 0; i < scripts.length; i++) {
      var s = scripts[i];
      var src = s && s.getAttribute && s.getAttribute('src');
      if (!src) continue;
      // Check if this specific container is already loaded
      if (src.indexOf(state.baseUrl + '?id=' + containerId) === 0) return s;
      // Also check canonical Google GTM host
      if (src.indexOf('https://www.googletagmanager.com/gtm.js?id=' + containerId) === 0) return s;
    }
    return null;
  }

  function loadContainer(containerId) {
    if (!containerId) return;
    if (state.loadedContainers[containerId]) return; // already loaded
    if (findExistingGtmScript(containerId)) {
      state.loadedContainers[containerId] = true;
      return;
    }

    try {
      // Ensure dataLayer is initialized before loading any container
      ensureDataLayerInit();

      // Create and inject script tag for this container
      var gtmScript = document.createElement('script');
      gtmScript.async = true;
      gtmScript.src = state.baseUrl + '?id=' + encodeURIComponent(containerId);

      // Append to head or fallback to documentElement
      (document.head || document.getElementsByTagName('head')[0] || document.documentElement).appendChild(gtmScript);

      state.loadedContainers[containerId] = true;
    } catch (e) {
      // Swallow to avoid breaking page; loading can be retried
    }
  }

  function unique(arr) {
    var map = {};
    var out = [];
    for (var i = 0; i < arr.length; i++) {
      var v = arr[i];
      if (map[v]) continue;
      map[v] = true;
      out.push(v);
    }
    return out;
  }

  function loadContainers(ids) {
    if (!ids || !ids.length) return;
    for (var i = 0; i < ids.length; i++) {
      loadContainer(ids[i]);
    }
  }

  // Public API to allow loading containers later or multiple times
  window.kepGTM = window.kepGTM || {};
  window.kepGTM.load = function(idOrIds) {
    var ids = normalizeIds(idOrIds);
    ids = unique(ids);
    if (!ids.length) return;
    loadContainers(ids);
  };
  window.kepGTM.isDataLayerInitialized = function() { return !!state.dataLayerInitialized; };
  window.kepGTM.loadedContainers = function() {
    var list = [];
    for (var k in state.loadedContainers) if (state.loadedContainers.hasOwnProperty(k)) list.push(k);
    return list;
  };

  // Collect IDs from global GTM_CONTAINER_IDS and load them
  var initialIds = normalizeIds(window.GTM_CONTAINER_IDS);
  initialIds = unique(initialIds);

  if (initialIds.length) {
    loadContainers(initialIds);
  }
})();
