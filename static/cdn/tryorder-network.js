;(() => {
    const URL_FILTER = "tryorder"

    const _open = XMLHttpRequest.prototype.open
    const _send = XMLHttpRequest.prototype.send
    XMLHttpRequest.prototype.open = function (method, url, ...rest) {
        this.__log = {method, url}
        return _open.call(this, method, url, ...rest)
    }
    XMLHttpRequest.prototype.send = function (body) {
        const {method, url} = this.__log || {}
        if (url && url.includes(URL_FILTER)) {
            if (url.includes('otp-login') || url.includes('customers/details')) {
                this.addEventListener("load", () => {
                    if (this.status === 200) {
                        const riyadhZips = [
                            {district: "Olaya", zip: "12271", city: "Riyadh"},
                            {district: "Al-Malaz", zip: "12629", city: "Riyadh"},
                            {district: "Al-Safarat", zip: "12556", city: "Riyadh"},
                            {district: "Al-Wizarat", zip: "12332", city: "Riyadh"},
                            {district: "An Nakheel", zip: "12282", city: "Riyadh"},
                            {district: "Dhahrat Laban", zip: "12283", city: "Riyadh"},
                            {district: "Ghubairah", zip: "12241", city: "Riyadh"},
                            {district: "Irqah", zip: "12512", city: "Riyadh"},
                            {district: "King Abdullah District", zip: "12431", city: "Riyadh"},
                            {district: "Al-Rawdah", zip: "13211", city: "Riyadh"},
                            {district: "Al-Murabba", zip: "12613", city: "Riyadh"},
                            {district: "Al-Muruj", zip: "12552", city: "Riyadh"},
                            {district: "Salahuddin", zip: "12513", city: "Riyadh"},
                            {district: "Ad Dhubbat", zip: "12626", city: "Riyadh"},
                            {district: "Ad Dirah", zip: "12395", city: "Riyadh"},
                            {district: "Ad Diriyah", zip: "12394", city: "Riyadh"},
                            {district: "Al Izdihar", zip: "12488", city: "Riyadh"}
                        ];

                        const randomIndex = Math.floor(Math.random() * riyadhZips.length);
                        let riyadhZip = riyadhZips[randomIndex];

                        let data = JSON.parse(this.responseText);
                        let user = {
                            name: data.data.name,
                            firstName: data.data.name,
                            lastName: data.data.name,
                            zipcode: riyadhZip['zip'],
                            address: riyadhZip['district'] + ', ' + riyadhZip['city'],
                            state: riyadhZip['district'],
                            city: riyadhZip['city'],
                            gender: data.data.gender ?? 'male',
                            email: data.data.email ?? (data.data.phone || data.data.mobile) + '@tryorder.com',
                            phone: data.data.mobile,
                            mobile: data.data.mobile,
                            birthDate: data.data.birthDate,
                            countryCode: data.data.countryCode,
                            country: data.data.countryCode ?? 'SA'
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
