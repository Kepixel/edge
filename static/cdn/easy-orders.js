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

    document.addEventListener('DOMContentLoaded', () => {
        kx.page()
    })
    const handleEvent = e => {
        if (!e || typeof e !== 'object') return

        console.log(e)
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
})()
