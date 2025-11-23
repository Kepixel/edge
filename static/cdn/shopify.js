(function () {
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
            items_count: event.data.cart.lines.length,
            products: event.data.cart.lines.map(p => ({
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
            url: event.data.cartLine.merchandise.product.url,
            image_url: event.data.cartLine.merchandise.product?.image?.src
        });
    };

    const handleProductRemovedFromCart = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track('Product Removed', {
            product_id: event.data.cartLine.merchandise.product.id,
            name: event.data.cartLine.merchandise.product.title,
            price: event.data.cartLine.cost.totalAmount.amount,
            currency: event.data.cartLine.cost.totalAmount.currencyCode,
            quantity: event.data.cartLine.quantity
        });
    };

    const handlePaymentInfoSubmitted = (event) => {
        setUserTraits(event);
        kepixelAnalytics.track('Payment Info Entered', {
            order_id: event.data.checkout.token,
            currency: event.data.checkout && event.data.checkout.currencyCode,
            value: event.data.checkout && event.data.checkout.totalPrice.amount,
            step: 1,
            step_name: 'Payment Information',
        });
    };

    const handleCheckoutStarted = (event) => {
        setUserTraits(event);
        const checkout = event.data.checkout;

        // Map products from line items
        const products = checkout.lineItems.map(item => ({
            product_id: item.variant.product.id,
            sku: item.variant.sku || null,
            name: item.title,
            brand: item.variant.product.vendor,
            price: item.variant.price.amount,
            currency: item.variant.price.currencyCode,
            category: item.variant.product.type || null,
            variant: item.variant.title || null,
            quantity: item.quantity,
            coupon: item.discountAllocations.length > 0 ? item.discountAllocations[0].discountApplication?.title : null
        }));

        // Extract discount/coupon info
        const discount = checkout.discountsAmount.amount;
        const coupon = checkout.discountApplications.length > 0
            ? checkout.discountApplications[0].title
            : null;

        // Extract shipping method
        const shippingMethod = checkout.delivery?.selectedDeliveryOptions?.[0]?.title || null;

        // Build the Kepixel event
        kepixelAnalytics.track('Checkout Started', {
            order_id: checkout.token,
            affiliation: checkout.localization.market.handle,
            value: checkout.totalPrice.amount,
            revenue: checkout.subtotalPrice.amount,
            shipping: checkout.shippingLine.price.amount,
            tax: checkout.totalTax.amount,
            discount: discount,
            coupon: coupon,
            currency: checkout.currencyCode,
            products: products,
            payment_method: null,
            shipping_method: shippingMethod,
            shipping_address: checkout.shippingAddress ? {
                name: `${checkout.shippingAddress.firstName} ${checkout.shippingAddress.lastName}`,
                street: checkout.shippingAddress.address1,
                street2: checkout.shippingAddress.address2,
                city: checkout.shippingAddress.city,
                state: checkout.shippingAddress.province,
                postal_code: checkout.shippingAddress.zip,
                country: checkout.shippingAddress.country
            } : null,
            billing_address: checkout.billingAddress ? {
                name: `${checkout.billingAddress.firstName} ${checkout.billingAddress.lastName}`,
                street: checkout.billingAddress.address1,
                street2: checkout.billingAddress.address2,
                city: checkout.billingAddress.city,
                state: checkout.billingAddress.province,
                postal_code: checkout.billingAddress.zip,
                country: checkout.billingAddress.country
            } : null,
            email: checkout.email,
            phone: checkout.phone
        });
    };

    const handleCheckoutCompleted = (event) => {
        setUserTraits(event);
        const checkout = event.data.checkout;

        // Map products from line items
        const products = checkout.lineItems.map(item => ({
            product_id: item.variant.product.id,
            sku: item.variant.sku || null,
            name: item.title,
            brand: item.variant.product.vendor,
            price: item.variant.price.amount,
            currency: item.variant.price.currencyCode,
            category: item.variant.product.type || null,
            variant: item.variant.title || null,
            quantity: item.quantity,
            coupon: item.discountAllocations.length > 0
                ? item.discountAllocations[0].discountApplication?.title
                : null
        }));

        // Extract discount/coupon info
        const discount = checkout.discountsAmount.amount;
        const coupon = checkout.discountApplications.length > 0
            ? checkout.discountApplications[0].title
            : null;

        // Extract payment method
        const paymentMethod = checkout.transactions.length > 0
            ? checkout.transactions[0].paymentMethod.type
            : null;

        // Extract shipping method
        const shippingMethod = checkout.delivery?.selectedDeliveryOptions?.[0]?.title || null;

        // Build the Kepixel event
        kepixelAnalytics.track('Order Completed', {
            order_id: checkout.order.id,
            checkout_token: checkout.token,
            affiliation: checkout.localization.market.handle || 'Online Store',
            value: checkout.totalPrice.amount,
            revenue: checkout.subtotalPrice.amount,
            shipping: checkout.shippingLine.price.amount,
            tax: checkout.totalTax.amount,
            discount: discount,
            coupon: coupon,
            currency: checkout.currencyCode,
            products: products,
            payment_method: paymentMethod,
            shipping_method: shippingMethod,
            shipping_address: checkout.shippingAddress ? {
                name: `${checkout.shippingAddress.firstName} ${checkout.shippingAddress.lastName}`,
                street: checkout.shippingAddress.address1,
                street2: checkout.shippingAddress.address2 || null,
                city: checkout.shippingAddress.city,
                state: checkout.shippingAddress.province,
                postal_code: checkout.shippingAddress.zip,
                country: checkout.shippingAddress.country
            } : null,
            billing_address: checkout.billingAddress ? {
                name: `${checkout.billingAddress.firstName} ${checkout.billingAddress.lastName}`,
                street: checkout.billingAddress.address1,
                street2: checkout.billingAddress.address2 || null,
                city: checkout.billingAddress.city,
                state: checkout.billingAddress.province,
                postal_code: checkout.billingAddress.zip,
                country: checkout.billingAddress.country
            } : null,
            customer_id: checkout.order.customer.id,
            is_first_order: checkout.order.customer.isFirstOrder,
            email: checkout.email,
            phone: checkout.phone
        });
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
