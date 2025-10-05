(function () {
    "use strict";
    window.KepixelSnippetVersion = "3.0.60";
    var e = "kepixelAnalytics";
    window[e] || (window[e] = []);
    var kepixelAnalytics = window[e];

    if (Array.isArray(kepixelAnalytics)) {
        if (true === kepixelAnalytics.snippetExecuted && window.console && console.error) {
            // console.error("KePixel JavaScript SDK snippet included more than once.");
        } else {
            kepixelAnalytics.snippetExecuted = true;
            window.kepixelAnalyticsBuildType = "legacy";
            var scriptLoadingMode = "async";
            var r = [
                "setDefaultInstanceKey", "load", "ready", "page", "track", "identify", "alias", "group",
                "reset", "setAnonymousId", "startSession", "endSession", "consent"
            ];

            for (var n = 0; n < r.length; n++) {
                var t = r[n];
                kepixelAnalytics[t] = function (r) {
                    return function () {
                        var n;
                        Array.isArray(window[e])
                            ? kepixelAnalytics.push([r].concat(Array.prototype.slice.call(arguments)))
                            : (n = window[e][r]) && n.apply(window[e], arguments);
                    };
                }(t);
            }

            try {
                new Function('class Test{field=()=>{};test({prop=[]}={}){return prop?(prop?.property??[...prop]):import("");}}');
                window.kepixelAnalyticsBuildType = "modern";
            } catch (i) {}

            var d = document.head || document.getElementsByTagName("head")[0];
            var o = document.body || document.getElementsByTagName("body")[0];

            window.kepixelAnalyticsAddScript = function (e, r, n) {
                var t = document.createElement("script");
                t.src = e;
                t.setAttribute("data-loader", "KE_JS_SDK");
                r && n && t.setAttribute(r, n);
                "async" === scriptLoadingMode ? t.async = true : "defer" === scriptLoadingMode && (t.defer = true);
                d ? d.insertBefore(t, d.firstChild) : o.insertBefore(t, o.firstChild);
            };

            window.kepixelAnalyticsMount = function () {
                !function () {
                    if ("undefined" == typeof globalThis) {
                        var r = function getGlobal() {
                            return "undefined" != typeof self ? self : "undefined" != typeof window ? window : null;
                        }();
                        r && Object.defineProperty(r, "globalThis", {
                            value: r,
                            configurable: true
                        });
                    }
                }();

                window.kepixelAnalyticsAddScript(
                    'https://cdn.kepixel.com/kep.min.js?x=1.0.1',
                    "data-kep-write-key",
                    window.kepixelSourceKey || ""
                );
            };

            ("undefined" == typeof Promise || "undefined" == typeof globalThis)
                ? window.kepixelAnalyticsAddScript("https://polyfill-fastly.io/v3/polyfill.min.js?version=3.111.0&features=Symbol%2CPromise&callback=kepixelAnalyticsMount")
                : window.kepixelAnalyticsMount();

            var loadOptions = {};
            kepixelAnalytics.load(window.kepixelSourceKey || "", "https://edge.kepixel.com", loadOptions);
        }
    }
})();

(function() {
    var x = "_paq";
    window[x] || (window[x] = []);
    var _paq = window[x];

    if (Array.isArray(_paq)) {

    } else {
        _paq.push(['trackPageView']);
        _paq.push(['enableLinkTracking']);
        _paq.push(['setAppId', window.kepixelSourceKey || ""]);
        var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
        g.async = true; g.src = 'https://edge.kepixel.com/anubis.js'; s.parentNode.insertBefore(g, s);
        _paq.__loaded = true;
    }
})();

// GTM integration - only loads if kepixelTagId is defined
if (window.kepixelTagId) {
    (function(w, d, s, l, i) {
        w[l] = w[l] || [];
        w[l].push({ 'gtm.start': new Date().getTime(), event: 'gtm.js' });

        // Check if GTM script with specific ID is already present
        if (!d.getElementById('gtm-script-' + i)) {
            var f = d.getElementsByTagName(s)[0],
                j = d.createElement(s),
                dl = l != 'dataLayer' ? '&l=' + l : '';
            j.async = true;
            j.id = 'gtm-script-' + i;  // unique identifier
            j.src = 'https://www.googletagmanager.com/gtm.js?id=' + i + dl;
            f.parentNode.insertBefore(j, f);
        }
    })(window, document, 'script', 'dataLayer', window.kepixelTagId);
}
