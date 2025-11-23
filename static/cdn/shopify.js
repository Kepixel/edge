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
        userTraits.lastname = v || h || _ || y || "", e && e.checkout && addUserAddress(e.checkout.billingAddress, e.checkout.shippingAddress);

        window.kepixelAnalytics.alias(userTraits.userId);
        window.kepixelAnalytics.identify(userTraits.userId, userTraits);
    }, userTraits = {};


    setUserTraits();

    const handlePageViewed = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.page();
    };

    const handleProductViewed = (event) => {
        setUserTraits(event);
        window.kepixelAnalytics.track('Product Viewed', {
            client_dedup_id: event.id,
            currency: event.data.productVariant.price.currencyCode,
            value: event.data.productVariant.price.amount,
            price: event.data.productVariant.price.amount,
            product_name: event.data.productVariant.product.title,
            name: event.data.productVariant.product.title,
            product_id: event.data.productVariant.product.id,
            category: event.data.productVariant.product.type,
            brand: event.data.productVariant.product.vendor
        });
    };

    const handleCollectionViewed = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track('Product List Viewed', {
            list_id: event.data.collection.id || 'default',
            category: event.data.collection.title,
            products: event.data.collection.productVariants.map(p => ({
                product_id: p.id,
                sku: p.sku || p.id,
                name: p.title,
                product_name: p.title,
                currency: p.price.currencyCode,
                value: p.price.amount,
                price: p.price.amount,
            }))
        });
    };

    const handleSearchSubmitted = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track("Products Searched", {
            query: event.data.searchResult && event.data.searchResult.query,
        });
    };

    const handleCartViewed = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track('Cart Viewed', {
            cart_id: event.data.cart.id,
            currency: event.data.cart.cost.totalAmount.currencyCode,
            value: event.data.cart.cost.totalAmount.amount,
            items_count: event.data.lines.length,
            products: event.data.collection.productVariants.map(p => ({
                product_id: p.merchandise.product.id,
                sku: p.merchandise.product.sku || p.merchandise.product.id,
                name: p.merchandise.product.title,
                brand: p.merchandise.product.vendor,
                price: p.cost.totalAmount.amount,
                currency: p.cost.totalAmount.currencyCode,
                quantity: p.quantity,
            })),
        });
    };

    const handleProductAddedToCart = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track('Product Added', {
            cart_id: event.data.cartLine.merchandise.id,
            product_id: event.data.cartLine.merchandise.product.id,
            sku: event.data.cartLine.merchandise.product.sku || event.data.cartLine.merchandise.product.id,
            name: event.data.cartLine.merchandise.product.title,
            brand: event.data.cartLine.merchandise.product.vendor,
            price: event.data.cartLine.cost.totalAmount.amount,
            currency: event.data.cartLine.cost.totalAmount.currencyCode,
            quantity: event.data.cartLine.quantity,
            position: 1,
            coupon: 'SUMMER20',
            url: event.data.cartLine.merchandise.product.url,
            image_url: event.data.cartLine.merchandise.product.image.src
        });
    };

    const handleProductRemovedFromCart = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track('Product Removed', {
            product_id: 'P12345',
            name: 'Wireless Headphones',
            price: 99.99,
            currency: 'USD',
            quantity: 1
        });
    };

    const handlePaymentInfoSubmitted = (event) => {
        setUserTraits(event);
        console.log('userTraits', userTraits)

        window.kepixelAnalytics.track('', {

        });
        console.log('payment_info_submitted', event);
    };

    const handleCheckoutStarted = (event) => {
        setUserTraits(event);
        console.log('userTraits', userTraits)
        window.kepixelAnalytics.track('', {

        });
        console.log('checkout_started', event);
    };

    const handleCheckoutCompleted = (event) => {
        setUserTraits(event);
        console.log('userTraits', userTraits)

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
