(function () {

    if (window.dataLayer && Array.isArray(window.dataLayer)) {
        getUserProperties = function () {

        }, processEvent = function (e) {
            if (e.event === 'gtm.load') {
                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
                    window.kepixelAnalytics.page();
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    window.kepixelAnalytics.push(["page"]);
                }
            }
            if (e.ecommerce == 'undefined') {
                if (e.event === 'view_item') {
                    let event = 'Product Viewed'
                }
                if (e.event === 'add_to_cart') {
                    let event = 'Product Added'
                }
                if (e.event === 'begin_checkout') {
                    let event = 'Checkout Started'
                }
                if (e.event === 'purchase') {
                    let event = 'Order Completed'
                }
                console.log(e.ecommerce)
            }
        };
        var e = !1, r = [];
        setTimeout((function () {
            processBacklog()
        }), 2e3), checkAndProcessEvent = function (t) {
            processEvent(t)
        }, processBacklog = function () {
            e = !0, r.forEach((function (e) {
                processEvent(e)
            })), r = []
        }, window.dataLayer.forEach((function (e) {
            checkAndProcessEvent(e)
        }));
        var t = window.dataLayer.push;
        window.dataLayer.push = function () {
            return Array.prototype.forEach.call(arguments, (function (e) {
                try {
                    checkAndProcessEvent(e)
                } catch (r) {
                    console.error("Error processing event:", r, "Event:", e)
                }
            })), t.apply(this, arguments)
        }
    }
})();
