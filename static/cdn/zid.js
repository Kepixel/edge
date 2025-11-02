(function () {
    let enablePolling = false;
    let customerReady = false;
    const q = [];

    const isSafari = ua => !!ua && ua.includes("Safari") && !ua.includes("Chrome");

    const mapType = name => ({
        "gtm.load": "page",
        add_payment_info: "track",
        add_shipping_info: "track",
        add_to_cart: "track",
        add_to_wishlist: "track",
        begin_checkout: "track",
        close_convert_lead: "track",
        close_unconvert_lead: "track",
        disqualify_lead: "track",
        earn_virtual_currency: "track",
        generate_lead: "track",
        join_group: "track",
        level_end: "track",
        level_start: "track",
        level_up: "track",
        login: "track",
        post_score: "track",
        purchase: "track",
        qualify_lead: "track",
        refund: "track",
        remove_from_cart: "track",
        search: "track",
        select_content: "track",
        select_item: "track",
        select_promotion: "track",
        share: "track",
        sign_up: "track",
        spend_virtual_currency: "track",
        tutorial_begin: "track",
        tutorial_complete: "track",
        unlock_achievement: "track",
        view_cart: "track",
        view_item: "track",
        view_item_list: "track",
        view_promotion: "track",
        working_lead: "track"
    })[name];

    const zidToKepixelMap = {
        // add_payment_info: "Payment Info Entered",
        // add_shipping_info: "Checkout Step Completed",
        add_to_cart: "Product Added",
        // add_to_wishlist: "Product Added to Wishlist",
        begin_checkout: "Checkout Started",
        close_convert_lead: "Order Completed",
        // close_unconvert_lead: "Order Cancelled",
        // disqualify_lead: "Order Cancelled",
        // generate_lead: "Order Updated",
        purchase: "Order Completed",
        // qualify_lead: "Order Updated",
        // refund: "Order Refunded",
        remove_from_cart: "Product Removed",
        search: "Products Searched",
        select_content: "Product Clicked",
        select_item: "Product Clicked",
        select_promotion: "Product Clicked",
        // share: "Product Shared",
        view_cart: "Cart Viewed",
        view_item: "Product Viewed",
        // view_item_list: "Product List Viewed",
        // view_promotion: "Product List Filtered",
        // order_updated: "Order Updated",
        order_completed: "Order Completed",
        // order_refunded: "Order Refunded",
        // order_cancelled: "Order Cancelled",
        // product_clicked: "Product Clicked",
        product_viewed: "Product Viewed"
    };

    function traits() {
        const c = window.customer || {};
        return {
            id: c.id,
            email: c.email,
            name: c.name,
            firstname: c.firstname,
            lastname: c.lastname,
            phone: c.mobile
        };
    }

    function process(ev) {
        switch (ev.type) {
            case "page":
                watchCustomer();
                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.page === "function") {
                    window.kepixelAnalytics.page();
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    window.kepixelAnalytics.push(["page"]);
                }
                break;
            case "track":
                watchCustomer();
                const kepixelEventName = zidToKepixelMap[ev.name];
                if (!kepixelEventName) return;

                if (kepixelEventName === 'Product Viewed') {
                    ev.properties.product_id = ev.properties.product_id || ev.properties.id || ev.properties['items'][0]?.id;
                    ev.properties.product = ev.properties['items'][0];
                }
                if (kepixelEventName === 'Product Added') {
                    ev.properties.product_id = ev.properties.product_id || ev.properties.id || ev.properties['items'][0]?.id;
                    ev.properties.product = ev.properties['items'][0];
                }

                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === "function") {
                    window.kepixelAnalytics.track(kepixelEventName, ev.properties);
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    window.kepixelAnalytics.push(["track", kepixelEventName, ev.properties]);
                }
                break;
        }
    }

    function drain() {
        while (q.length) process(q.shift());
    }

    function handleDL(ev) {
        if (!ev) return;
        let name, props;
        if (ev.event) {
            name = ev.event;
            props = {...(ev.ecommerce || {})};
            for (const k in ev) if (k !== "event" && k !== "ecommerce" && ev[k] != null) props[k] = ev[k];
        } else if (Array.isArray(ev) || Object.prototype.toString.call(ev) === '[object Arguments]') {
            name = ev[0];
            props = ev[1] || {};
        } else {
            return;
        }
        if (name === 'event') {
            name = ev[1]
            props = ev[2] || {};
        }
        const t = mapType(name);
        if (!t) return;
        const payload = {type: t, name, properties: props};

        // Always process events immediately - customer identification is handled separately
        process(payload);
    }

    function hookPush() {
        const orig = window.dataLayer.push.bind(window.dataLayer);
        window.dataLayer.push = function (...items) {
            items.forEach(it => {
                try {
                    handleDL(it);
                } catch (e) {
                    console.error("handle error", e, it);
                }
            });
            return orig(...items);
        };
    }

    function watchCustomer() {
        if (window.customer) {
            customerReady = true;
            drain();
            let user = traits();
            if (user && user.id) {
                if (window.kepixelAnalytics && typeof window.kepixelAnalytics.identify === "function") {
                    window.kepixelAnalytics.identify(user.id, user);
                } else {
                    window.kepixelAnalytics = window.kepixelAnalytics || [];
                    window.kepixelAnalytics.push(["identify", user.id, user]);
                }
            }
            return;
        }
        const timer = setTimeout(() => {
            if (!customerReady) {
                customerReady = true;
                drain();
            }
        }, 2000);

        Object.defineProperty(window, "customer", {
            configurable: true,
            get() {
                return this._customer;
            },
            set(v) {
                this._customer = v;
                clearTimeout(timer);
                customerReady = true;
                drain();
            }
        });
    }

    if (!window.dataLayer && window.gtmDataLayer) window.dataLayer = window.gtmDataLayer;

    if (window.dataLayer && Array.isArray(window.dataLayer)) {
        if (isSafari(navigator.userAgent)) {
            enablePolling = true;
        }

        // process existing events
        for (let i = 0; i < window.dataLayer.length; i++) {
            try {
                handleDL(window.dataLayer[i]);
            } catch (e) {
                console.error("handle error", e);
            }
        }

        hookPush();
        watchCustomer();
    } else {
        console.error("DataLayer not found");
    }
})();
