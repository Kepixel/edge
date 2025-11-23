(function() {
    var init = {}, getField = function (t, e) {
        return e.split(".").reduce(function (t, e) {
            return t ? t[e] : void 0
        }, t)
    }, addUserAddress = function (t, e) {
        userTraits.address = t && t.address1 || e && e.address1, userTraits.city = t && t.city || e && e.city, userTraits.country = t && t.country || e && e.country, userTraits.country_code = t && t.countryCode || e && e.countryCode, userTraits.state = t && t.province || e && e.province, userTraits.state_code = t && t.provinceCode || e && e.provinceCode, userTraits.postal_code = t && t.zip || e && e.zip
    }, setUserTraits = function (t) {
        let e = {};
        t && t.data && (e = t.data), userTraits.userId = getField(t, "clientId");
        let a = getField(e, "checkout.billingAddress.email"), i = getField(e, "checkout.shippingAddress.email"),
            r = getField(e, "checkout.email"), c = getField(init, "data.customer.email");
        userTraits.email = c || r || i || a || "";
        let n = getField(e, "checkout.billingAddress.phone"), d = getField(e, "checkout.shippingAddress.phone"),
            o = getField(e, "checkout.phone"), s = getField(init, "data.customer.phone");
        userTraits.phone = s || o || d || n || "";
        let u = getField(e, "checkout.billingAddress.firstName"),
            l = getField(e, "checkout.shippingAddress.firstName"), m = getField(e, "checkout.firstName"),
            p = getField(init, "data.customer.firstName");
        userTraits.firstname = p || m || l || u || "";
        let y = getField(e, "checkout.billingAddress.lastName"),
            _ = getField(e, "checkout.shippingAddress.lastName"), h = getField(e, "checkout.lastName"),
            v = getField(init, "data.customer.lastName");
        userTraits.lastname = v || h || _ || y || "", e && e.checkout && addUserAddress(e.checkout.billingAddress, e.checkout.shippingAddress)
    }, userTraits = {};


    setUserTraits();

    const handlePageViewed = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.page();
        console.log(event)
    };

    const handleProductViewed = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });

        console.log('product_viewed', event);
    };

    const handleCollectionViewed = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('collection_viewed', event);
    };

    const handleSearchSubmitted = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('search_submitted', event);
    };

    const handleCartViewed = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('cart_viewed', event);
    };

    const handleProductAddedToCart = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('product_added_to_cart', event);
    };

    const handleProductRemovedFromCart = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('product_removed_from_cart', event);
    };

    const handlePaymentInfoSubmitted = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('payment_info_submitted', event);
    };

    const handleCheckoutStarted = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('checkout_started', event);
    };

    const handleCheckoutCompleted = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('', {

        });
        console.log('checkout_completed', event);
    };

    // Expose handlers globally
    window.kepixel = {
        isReady: true,
        handlePageViewed,
        handleProductViewed,
        handleCollectionViewed,
        handleSearchSubmitted,
        handleCartViewed,
        handleProductAddedToCart,
        handleProductRemovedFromCart,
        handlePaymentInfoSubmitted,
        handleCheckoutStarted,
        handleCheckoutCompleted
    };
})();
