(function () {
  if (typeof window === 'undefined') return;

  // Internal state (shared across multiple inclusions)
  var state = window.__kep_gtm_state = window.__kep_gtm_state || {
    baseUrl: 'https://www.googletagmanager.com/gtm.js',
    jsInitialized: false, // whether we pushed the initial gtm.js event
    injected: {}, // map of containerId -> true
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
      out.push(id);
    }
    return out;
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

  function ensureJsInit() {
    if (state.jsInitialized) return;
    window.dataLayer = window.dataLayer || [];
    // Standard GTM bootstrap event
    window.dataLayer.push({ 'gtm.start': new Date().getTime(), event: 'gtm.js' });
    state.jsInitialized = true;
  }

  function scriptMatchesId(src, id) {
    if (!src || !id) return false;
    if (src.indexOf(state.baseUrl) !== 0) return false;
    // crude check for id parameter
    return src.indexOf('id=' + encodeURIComponent(id)) !== -1;
  }

  function findExistingScriptFor(id) {
    var scripts = document.getElementsByTagName('script');
    for (var i = 0; i < scripts.length; i++) {
      var s = scripts[i];
      var src = s && s.getAttribute && s.getAttribute('src');
      if (scriptMatchesId(src, id)) return s;
    }
    return null;
  }

  function inject(id) {
    if (!id) return;
    if (state.injected[id]) return;
    if (findExistingScriptFor(id)) { state.injected[id] = true; return; }
    ensureJsInit();
    var gtmScript = document.createElement('script');
    gtmScript.async = true;
    gtmScript.src = state.baseUrl + '?id=' + encodeURIComponent(id);
    (document.head || document.getElementsByTagName('head')[0] || document.documentElement).appendChild(gtmScript);
    state.injected[id] = true;
  }

  function loadMany(ids) {
    if (!ids || !ids.length) return;
    for (var i = 0; i < ids.length; i++) inject(ids[i]);
  }

  // Public API
  window.kepGTM = window.kepGTM || {};
  window.kepGTM.load = function (idOrIds) {
    var ids = normalizeIds(idOrIds);
    ids = unique(ids);
    if (!ids.length) return;
    loadMany(ids);
  };
  window.kepGTM.isInitialized = function () { return !!state.jsInitialized; };
  window.kepGTM.injectedIds = function () {
    var list = [];
    for (var k in state.injected) if (state.injected.hasOwnProperty(k)) list.push(k);
    return list;
  };
  window.kepGTM.isInjected = function (id) { return !!state.injected[id]; };

  // Auto-load from globals (array or comma-separated string)
  var initialIds = unique(normalizeIds(window.GTM_CONTAINER_IDS));
  if (initialIds.length) loadMany(initialIds);
})();
