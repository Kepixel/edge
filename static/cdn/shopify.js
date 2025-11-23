(function() {
    const handlePageViewed = (event) => {
        console.log('kkk', window.kepixelAnalytics)
        console.log('page_viewed', event);
    };

    const handleProductViewed = (event) => {
        console.log('product_viewed', event);
    };

    const handleCollectionViewed = (event) => {
        console.log('collection_viewed', event);
    };

    const handleSearchSubmitted = (event) => {
        console.log('search_submitted', event);
    };

    const handleCartViewed = (event) => {
        console.log('cart_viewed', event);
    };

    const handleProductAddedToCart = (event) => {
        console.log('product_added_to_cart', event);
    };

    const handleProductRemovedFromCart = (event) => {
        console.log('product_removed_from_cart', event);
    };

    const handlePaymentInfoSubmitted = (event) => {
        console.log('payment_info_submitted', event);
    };

    const handleCheckoutStarted = (event) => {
        console.log('checkout_started', event);
    };

    const handleCheckoutCompleted = (event) => {
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
