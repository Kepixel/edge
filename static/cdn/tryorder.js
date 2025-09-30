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
            if (e.ecommerce != 'undefined') {
                if (e.event == 'view_item') {
                    let event = 'Product Viewed'
                    let product = {
                        product_id: e.ecommerce.item.item_id,
                        sku: e.ecommerce.item.item_id,
                        category: e.ecommerce.item.item_name,
                        name: e.ecommerce.item.item_name,
                        brand: e.ecommerce.item.item_name,
                        price: e.ecommerce.item.price,
                        quantity: 1,
                        currency: e.ecommerce.currency,
                        position: 1,
                        url: window.location.href,
                    };

                    if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                        window.kepixelAnalytics.track(event, product);
                    } else {
                        window.kepixelAnalytics = window.kepixelAnalytics || [];
                        window.kepixelAnalytics.push(["track", event, product]);
                    }
                }
                if (e.event === 'add_to_cart') {
                    let event = 'Product Added'
                    let product = {
                        product_id: e.ecommerce.item.item_id,
                        sku: e.ecommerce.item.item_id,
                        category: e.ecommerce.item.item_name,
                        name: e.ecommerce.item.item_name,
                        brand: e.ecommerce.item.item_name,
                        price: e.ecommerce.item.price,
                        quantity: e.ecommerce.item.quantity,
                        position: 1,
                        url: window.location.href,
                        currency: e.ecommerce.currency,
                    }
                    if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                        window.kepixelAnalytics.track(event, product);
                    } else {
                        window.kepixelAnalytics = window.kepixelAnalytics || [];
                        window.kepixelAnalytics.push(["track", event, product]);
                    }
                }
                if (e.event === 'begin_checkout') {
                    let event = 'Checkout Started'
                    let data = {
                        value: e.ecommerce.total_price,
                        revenue: e.ecommerce.total_price,
                        currency: e.ecommerce.item_currency ?? e.ecommerce.currency,
                        products: e.ecommerce.items.map((item) => ({
                            product_id: item.main_item_id,
                            sku: item.main_item_id,
                            name: item.item_name,
                            price: item.total_price,
                            quantity: item.qty,
                            position: 1,
                            category: item.group_name,
                            image_url: item.item_image,
                        })),
                    }

                    if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                        window.kepixelAnalytics.track(event, data);
                    } else {
                        window.kepixelAnalytics = window.kepixelAnalytics || [];
                        window.kepixelAnalytics.push(["track", event, data]);
                    }
                }
                if (e.event === 'purchase') {
                    let event = 'Order Completed'
                    const calculatedTotal = Array.isArray(e.ecommerce.items)
                        ? e.ecommerce.items.reduce((sum, item) => {
                            const price = typeof item.total_price === 'number' ? item.total_price : parseFloat(item.total_price);
                            return Number.isFinite(price) ? sum + price : sum;
                        }, 0)
                        : 0;
                    const normalizedTotal = Math.round(calculatedTotal * 100) / 100;
                    const path = window.location.pathname;
                    const orderId = path.split("/order/")[1];

                    let data = {
                        checkout_id: orderId,
                        order_id: orderId,
                        total: normalizedTotal,
                        subtotal: normalizedTotal,
                        revenue: normalizedTotal,
                        currency: e.ecommerce.item_currency ?? e.ecommerce.currency,
                        products: e.ecommerce.items.map((item) => ({
                            product_id: item.main_item_id,
                            sku: item.main_item_id,
                            name: item.item_name,
                            price: item.total_price,
                            quantity: item.qty,
                            position: 1,
                            category: item.group_name,
                            image_url: item.item_image,
                        })),
                    }
                    if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                        window.kepixelAnalytics.track(event, data);
                    } else {
                        window.kepixelAnalytics = window.kepixelAnalytics || [];
                        window.kepixelAnalytics.push(["track", event, data]);
                    }
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
