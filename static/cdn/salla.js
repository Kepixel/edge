(function () {

    if (window.dataLayer && Array.isArray(window.dataLayer)) {
        const events = [
            'Product Reviewed',
            'Product Shared',
            'Cart Shared',
            'Product Added to Wishlist',
            'Product Removed from Wishlist',
            'Wishlist Product Added to Cart',
            'Coupon Entered',
            'Coupon Applied',
            'Coupon Denied',
            'Coupon Removed',
            'Product Clicked',
            'Product Viewed',
            'Product Added',
            'Product Removed',
            'Cart Viewed',
            // 'Cart Updated',
            'Checkout Started',
            'Checkout Step Viewed',
            'Checkout Step Completed',
            'Payment Info Entered',
            // 'Payment Failed',
            'Order Updated',
            'Order Completed',
            'Order Refunded',
            'Order Cancelled',
            'Products Searched',
            'Product List Viewed',
            'Product List Filtered',
            // 'Product List Sorted'
        ];
        getUserProperties = function () {
            var e = window.Salla || window.salla || {};
            if (null != e.config) return {
                id: e.config.get("user.id"),
                email: e.config.get("user.email"),
                phone: e.config.get("user.mobile"),
                firstname: e.config.get("user.first_name"),
                lastname: e.config.get("user.last_name"),
                country: e.config.get("user.country_code")
            }
        }, processEvent = function (e) {
            let u = getUserProperties();
            let currency = null;

            if ('ecommerce' in e) {
                if ('currencyCode' in e.ecommerce) {
                    currency = e.ecommerce.currencyCode;
                }
            }

            if (e.event && events.includes(e.event)) {
                var payload;
                if (e[0] !== undefined) {
                    payload = e[0];
                } else {
                    payload = Object.assign({}, e);
                    delete payload.event;
                }
                if (
                    payload?.client_dedup_id != null ||
                    payload?.position != null ||
                    payload?.properties?.client_dedup_id != null ||
                    payload?.properties?.position != null ||
                    payload['client_dedup_id'] != null ||
                    payload['position'] != null
                ) {
                    return
                }
                if (payload != null && typeof payload === 'object' &&
                    (Object.prototype.hasOwnProperty.call(payload, 'client_dedup_id') ||
                     Object.prototype.hasOwnProperty.call(payload, 'position'))) {
                    return;
                }
                if ('client_dedup_id' in payload || 'position' in payload) {
                    return;
                }

                if (!('currency' in payload) && currency !== null) {
                    payload.currency = currency;
                }

                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                    if (u && u.id) {
                        window.kepixelAnalytics.identify(u.id, u);
                    }
                    window.kepixelAnalytics.track(e.event, payload);
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    if (u && u.id) {
                        window.kepixelAnalytics.push(["identify", u.id, u]);
                    }
                    window.kepixelAnalytics.push(["track", e.event, payload]);
                }
            }
            if (e.event == 'auth::logged.in' || e.event == 'auth::registration.success') {
                var options = getUserProperties();
                var userId = options ? options.id : null;
                if (userId) {
                    if (window.kepixelAnalytics && typeof window.kepixelAnalytics.identify === "function") {
                        window.kepixelAnalytics.alias(userId);
                        window.kepixelAnalytics.identify(userId, options);
                    } else {
                        window.kepixelAnalytics = window.kepixelAnalytics || [];
                        window.kepixelAnalytics.push(["alias", userId]);
                        window.kepixelAnalytics.push(["identify", userId, options]);
                    }
                }
            }
            if (e.event == 'page.view') {
                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.page === "function") {
                    window.kepixelAnalytics.page();
                    if (u && u.id) {
                        window.kepixelAnalytics.identify(u.id, u);
                    }
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    window.kepixelAnalytics.push(["page"]);
                    if (u && u.id) {
                        window.kepixelAnalytics.push(["identify", u.id, u]);
                    }
                }
            }

            if (e.event == "purchase") {
                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                    if (u && u.id) {
                        window.kepixelAnalytics.identify(u.id, u);
                    }
                    window.kepixelAnalytics.track("Order Completed", {
                        currency: e.ecommerce.currencyCode,
                        value: e.ecommerce.purchase.actionField && e.ecommerce.purchase.actionField.total,
                        transaction_id: e.ecommerce.event_id,
                        order_id: e.ecommerce.event_id,
                        items: e.ecommerce.purchase.products.map((function (e) {
                            return {
                                item_id: e.id,
                                item_name: e.name,
                                item_category: e.category,
                                quantity: e.quantity,
                                price: e.price,
                                discount: e.discount
                            }
                        }))
                    });
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    if (u && u.id) {
                        window.kepixelAnalytics.push(["alias", u.id]);
                        window.kepixelAnalytics.push(["identify", u.id, u]);
                    }
                    window.kepixelAnalytics.push(["Order Completed", {
                        currency: e.ecommerce.currencyCode,
                        value: e.ecommerce.purchase.actionField && e.ecommerce.purchase.actionField.total,
                        transaction_id: e.ecommerce.event_id,
                        order_id: e.ecommerce.event_id,
                        items: e.ecommerce.purchase.products.map((function (e) {
                            return {
                                item_id: e.id,
                                item_name: e.name,
                                item_category: e.category,
                                quantity: e.quantity,
                                price: e.price,
                                discount: e.discount
                            }
                        }))
                    }]);
                }
            }
        };
        var e = !1, r = [];
        setTimeout((function () {
            processBacklog()
        }), 2e3), checkAndProcessEvent = function (t) {
            if (!e) return t && "auth.token.session" == t.route && processBacklog(), void r.push(t);
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
