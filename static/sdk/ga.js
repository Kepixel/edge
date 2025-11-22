(function () {
  if (typeof window === 'undefined') return;

  // Internal state (shared across multiple inclusions of this file)
  var state = window.__kep_ga_state = window.__kep_ga_state || {
    baseUrl: 'https://edge.kepixel.com/ga/gtag/js',
    scriptInjected: false,
    jsInitialized: false,
    configuredIds: {}, // map of id -> true
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
      // Basic sanity: GA4 IDs typically start with 'G-'
      out.push(id);
    }
    return out;
  }

  function ensureJsInit() {
    if (state.jsInitialized) return;
    // Initialize dataLayer and gtag once
    window.dataLayer = window.dataLayer || [];
    function gtag(){ window.dataLayer.push(arguments); }
    window.gtag = window.gtag || gtag;
    // Call 'js' only once
    window.gtag('js', new Date());
    state.jsInitialized = true;
  }

  function findExistingGtagScript() {
    var scripts = document.getElementsByTagName('script');
    for (var i = 0; i < scripts.length; i++) {
      var s = scripts[i];
      var src = s && s.getAttribute && s.getAttribute('src');
      if (!src) continue;
      if (src.indexOf(state.baseUrl) === 0) return s;
      // Allow for the canonical Google host too, just in case something else injected it
      if (src.indexOf('https://www.googletagmanager.com/gtag/js') === 0) return s;
    }
    return null;
  }

  function ensureScriptInjected(idForSrc) {
    if (state.scriptInjected) return;
    if (findExistingGtagScript()) {
      state.scriptInjected = true;
      return;
    }
    if (!idForSrc) return; // defer loading until we have at least one ID
    var gaScript = document.createElement('script');
    gaScript.async = true;
    gaScript.src = state.baseUrl + '?id=' + encodeURIComponent(idForSrc);
    (document.head || document.getElementsByTagName('head')[0] || document.documentElement).appendChild(gaScript);
    state.scriptInjected = true;
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

  function configureIds(ids) {
    if (!ids || !ids.length) return;
    ensureJsInit();
    // Ensure the external script is injected using the first ID we see
    ensureScriptInjected(ids[0]);
    for (var i = 0; i < ids.length; i++) {
      var id = ids[i];
      if (state.configuredIds[id]) continue; // already configured
      try {
        window.gtag('config', id);
        state.configuredIds[id] = true;
      } catch (e) {
        // Swallow to avoid breaking page; configuration can be retried in later inclusions
      }
    }
  }

  // Public API to allow configuring later or multiple times
  window.kepGA = window.kepGA || {};
  window.kepGA.config = function (idOrIds) {
    var ids = normalizeIds(idOrIds);
    ids = unique(ids);
    if (!ids.length) return;
    configureIds(ids);
  };
  window.kepGA.isInitialized = function () { return !!state.jsInitialized; };
  window.kepGA.isScriptInjected = function () { return !!state.scriptInjected || !!findExistingGtagScript(); };
  window.kepGA.configuredIds = function () {
    var list = [];
    for (var k in state.configuredIds) if (state.configuredIds.hasOwnProperty(k)) list.push(k);
    return list;
  };

  // Collect IDs from globals (legacy behavior) and configure them.
  // Supports: GA_MEASUREMENT_ID (string) and GA_MEASUREMENT_IDS (array or comma-separated string)
  var initialIds = [];
  if (typeof window.GA_MEASUREMENT_ID === 'string' && window.GA_MEASUREMENT_ID) {
    initialIds.push(window.GA_MEASUREMENT_ID);
  }
  var more = normalizeIds(window.GA_MEASUREMENT_IDS);
  if (more.length) {
    for (var mi = 0; mi < more.length; mi++) initialIds.push(more[mi]);
  }
  initialIds = unique(initialIds);

  if (initialIds.length) {
    configureIds(initialIds);
  }
})();
