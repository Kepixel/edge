(function () {

    if (window.dataLayer && Array.isArray(window.dataLayer)) {
        getUserProperties = function () {

        }, processEvent = function (e) {
            if (e.ecommerce == 'undefined') {
                console.log(e)
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
