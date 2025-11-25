(function () {
    if (!window.dataLayer || !Array.isArray(window.dataLayer)) return

    const qa = () => {
        window.kepixelAnalytics = window.kepixelAnalytics || []
        const api = window.kepixelAnalytics
        const call = (fn, ...args) => {
            if (typeof api[fn] === 'function') api[fn](...args)
            else api.push([fn, ...args])
        }
        return {
            page: () => call('page'),
            track: (event, payload) => call('track', event, payload),
        }
    }

    const kx = qa()

    const toNumber = v => {
        if (typeof v === 'number' && Number.isFinite(v)) return v
        const n = parseFloat(v)
        return Number.isFinite(n) ? n : 0
    }

    const round2 = n => Math.round(n * 100) / 100

    const toProduct = (item, idx) => ({
        product_id: item?.item_id ?? item?.main_item_id ?? item?.id ?? '',
        sku: item?.item_id ?? item?.main_item_id ?? '',
        name: item?.item_name ?? item?.name ?? '',
        category: item?.item_category ?? item?.group_name ?? '',
        brand: item?.item_brand ?? item?.brand ?? '',
        price: toNumber(item?.price ?? item?.total_price),
        quantity: toNumber(item?.quantity ?? item?.qty ?? 1),
        position: idx + 1,
        image_url: item?.item_image ?? item?.image_url ?? '',
        url: window.location.href,
    })

    const pickCurrency = ec => ec?.currency ?? ec?.item_currency ?? ''

    const sumItems = items => round2((items || []).reduce((s, it) => s + toNumber(it?.total_price ?? it?.price) * toNumber(it?.quantity ?? it?.qty ?? 1), 0))

    const getOrderId = ec => {
        const fromEvent = ec?.purchase?.transaction_id ?? ec?.transaction_id
        if (fromEvent) return fromEvent
        const m = (window.location.pathname || '').match(/\/order\/([^/]+)/)
        return m ? m[1] : ''
    }

    const handleEvent = e => {
        if (!e || typeof e !== 'object') return

        if (e.event === 'gtm.load') {
            kx.page()
            return
        }

        if (!(typeof e.ecommerce !== 'undefined' && e.ecommerce)) return
        const ec = e.ecommerce

        // view_item
        if (e.event === 'view_item') {
            const item = Array.isArray(ec.items) ? ec.items[0] : (ec.item || {})
            const payload = toProduct(item, 0)
            payload.currency = pickCurrency(ec)
            kx.track('Product Viewed', payload)
            return
        }

        // add_to_cart
        if (e.event === 'add_to_cart') {
            const item = Array.isArray(ec.items) ? ec.items[0] : (ec.item || {})
            const payload = toProduct(item, 0)
            payload.currency = pickCurrency(ec)
            kx.track('Product Added', payload)
            return
        }

        // begin_checkout
        if (e.event === 'begin_checkout') {
            const items = Array.isArray(ec.items) ? ec.items : []
            const value = toNumber(ec.value ?? ec.total_price ?? sumItems(items))
            const payload = {
                value,
                revenue: value,
                currency: pickCurrency(ec),
                products: items.map(toProduct),
            }
            kx.track('Checkout Started', payload)
            return
        }

        // purchase
        if (e.event === 'purchase') {
            const items =
                Array.isArray(ec.purchase?.items) ? ec.purchase.items :
                    Array.isArray(ec.items) ? ec.items : []
            const total = toNumber(
                ec.value ??
                ec.purchase?.value ??
                ec.total_price ??
                ec.purchase?.total_price ??
                sumItems(items)
            )
            const orderId = getOrderId(ec)
            const currency = pickCurrency(ec)
            const payload = {
                checkout_id: orderId,
                order_id: orderId,
                total: round2(total),
                subtotal: round2(total),
                revenue: round2(total),
                value: round2(total),
                currency,
                products: items.map(toProduct),
            }
            kx.track('Order Completed', payload)
            return
        }
    }

    // process existing events
    window.dataLayer.forEach(handleEvent)

    // intercept future pushes
    const originalPush = window.dataLayer.push
    window.dataLayer.push = function () {
        Array.prototype.forEach.call(arguments, ev => {
            try {
                handleEvent(ev)
            } catch (err) {
                console.error('kepixel handler error', err, ev)
            }
        })
        return originalPush.apply(this, arguments)
    }
})();

(function(history){
    const pushState = history.pushState;
    const replaceState = history.replaceState;

    history.pushState = function() {
        pushState.apply(history, arguments);
        window.dispatchEvent(new Event('urlchange'));
    };

    history.replaceState = function() {
        replaceState.apply(history, arguments);
        window.dispatchEvent(new Event('urlchange'));
    };
})(window.history);


// fire on load
document.addEventListener('DOMContentLoaded', () => {
    console.log('Page loaded:', window.location.href);
});


// fire on URL change
window.addEventListener('urlchange', () => {
    console.log('URL changed:', window.location.href);
});


// also handle back/forward
window.addEventListener('popstate', () => {
    console.log('URL changed:', window.location.href);
});
