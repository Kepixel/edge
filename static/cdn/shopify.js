analytics.subscribe("page_viewed", t => {
    console.log('page_viewed')
});
analytics.subscribe("product_viewed", t => {
    console.log('product_viewed')
});
analytics.subscribe("collection_viewed", t => {
    console.log('collection_viewed')
});
analytics.subscribe("search_submitted", t => {
    console.log('search_submitted')
});
analytics.subscribe("cart_viewed", t => {
    console.log('cart_viewed')
});
analytics.subscribe("product_added_to_cart", t => {
    console.log('product_added_to_cart')
});
analytics.subscribe("product_removed_from_cart", t => {
    console.log('product_removed_from_cart')
});
analytics.subscribe("payment_info_submitted", t => {
    console.log('payment_info_submitted')
});
analytics.subscribe("checkout_started", t => {
    console.log('checkout_started')
});
analytics.subscribe("checkout_completed", t => {
    console.log('checkout_completed')
});
