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
                    if (this.status === 200) {
                        let data = JSON.parse(this.responseText);
                        let user = {
                            name: data.data.name,
                            gender: data.data.gender,
                            email: data.data.email ?? (data.data.phone || data.data.mobile) + '@tryorder.com',
                            phone: data.data.mobile,
                            mobile: data.data.mobile,
                            birthDate: data.data.birthDate,
                            countryCode: data.data.countryCode
                        }

                        if (window.kepixelAnalytics && typeof window.kepixelAnalytics.track === 'function') {
                            window.kepixelAnalytics.alias(data.data.id);
                            window.kepixelAnalytics.identify(data.data.id, user);
                        } else {
                            window.kepixelAnalytics = window.kepixelAnalytics || [];
                            window.kepixelAnalytics.push(["alias", data.data.id]);
                            window.kepixelAnalytics.push(["identify", data.data.id, user]);
                        }
                    }
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
