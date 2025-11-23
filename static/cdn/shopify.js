var getField = function (t, e) {
    console.log(t, e)
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
setUserTraits(), analytics.subscribe("page_viewed", t => {
    console.log('page_viewed')
}), analytics.subscribe("product_viewed", t => {
    console.log('product_viewed')
}), analytics.subscribe("collection_viewed", t => {
    console.log('collection_viewed')
}), analytics.subscribe("search_submitted", t => {
    console.log('search_submitted')
}), analytics.subscribe("cart_viewed", t => {
    console.log('cart_viewed')
}), analytics.subscribe("product_added_to_cart", t => {
    console.log('product_added_to_cart')
}), analytics.subscribe("product_removed_from_cart", t => {
    console.log('product_removed_from_cart')
}), analytics.subscribe("payment_info_submitted", t => {
    console.log('payment_info_submitted')
}), analytics.subscribe("checkout_started", t => {
    console.log('checkout_started')
}), analytics.subscribe("checkout_completed", t => {
    console.log('checkout_completed')

});
