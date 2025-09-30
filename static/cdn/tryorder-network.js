;(() => {
    const URL_FILTER = "tryorder"

    const _open = XMLHttpRequest.prototype.open
    const _send = XMLHttpRequest.prototype.send
    XMLHttpRequest.prototype.open = function(method, url, ...rest) {
        this.__log = { method, url }
        return _open.call(this, method, url, ...rest)
    }
    XMLHttpRequest.prototype.send = function(body) {
        const { method, url } = this.__log || {}
        if (url && url.includes(URL_FILTER)) {
            if (url.includes('otp-login') || url.includes('customers/details')) {
                this.addEventListener("load", () => {
                    console.log("[XHR RESPONSE]", method, url, "Status:", this.status, "Body:", this.responseText)
                })
            }

        }
        return _send.call(this, body)
    }

    // if (window.fetch) {
    //     const _fetch = window.fetch
    //     window.fetch = async (...args) => {
    //         const [input, init = {}] = args
    //         const url = typeof input === "string" ? input : input.url
    //         if (url && url.includes(URL_FILTER)) {
    //             console.log("[FETCH REQUEST]", init.method || "GET", url, "Body:", init.body)
    //             const res = await _fetch(...args)
    //             const clone = res.clone()
    //             const text = await clone.text().catch(() => "")
    //             console.log("[FETCH RESPONSE]", init.method || "GET", url, "Status:", res.status, "Body:", text)
    //             return res
    //         }
    //         return _fetch(...args)
    //     }
    // }
    // console.log("Network logger active. Filtering by:", URL_FILTER)
})()
