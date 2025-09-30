;(() => {
    // ===== settings =====
    const URL_FILTER = 'tryorder'
    const MAX_BODY_CHARS = 5000

    // ===== helpers =====
    const now = () => performance.now()
    const tryParse = (txt) => {
        try { return JSON.parse(txt) } catch { return txt }
    }
    const limit = (s) => (typeof s === 'string' && s.length > MAX_BODY_CHARS ? s.slice(0, MAX_BODY_CHARS) + '…[truncated]' : s)

    // ===== hook XMLHttpRequest (covers Axios) =====
    const _open = XMLHttpRequest.prototype.open
    const _send = XMLHttpRequest.prototype.send
    const _setRequestHeader = XMLHttpRequest.prototype.setRequestHeader

    const store = new WeakMap()

    XMLHttpRequest.prototype.open = function(method, url, async, user, password) {
        this.__reqLog = { method, url: String(url), headers: {}, start: 0, body: null }
        return _open.apply(this, arguments)
    }

    XMLHttpRequest.prototype.setRequestHeader = function(k, v) {
        if (this.__reqLog) this.__reqLog.headers[k] = v
        return _setRequestHeader.apply(this, arguments)
    }

    XMLHttpRequest.prototype.send = function(body) {
        if (this.__reqLog) {
            this.__reqLog.start = now()
            this.__reqLog.body = body instanceof Document ? '[Document]' : body instanceof FormData ? '[FormData]' : limit(String(body || ''))
            store.set(this, this.__reqLog)
        }

        const done = (type) => () => {
            const meta = store.get(this)
            if (!meta) return
            const match = URL_FILTER ? new RegExp(URL_FILTER).test(meta.url) : true
            if (!match) return

            const dur = (now() - meta.start).toFixed(1) + 'ms'
            const resHeadersRaw = this.getAllResponseHeaders() || ''
            const resHeaders = Object.fromEntries(resHeadersRaw.trim().split(/\r?\n/).filter(Boolean).map(l => {
                const i = l.indexOf(':')
                return i > -1 ? [l.slice(0, i).trim(), l.slice(i + 1).trim()] : [l, '']
            }))

            let bodyText = ''
            try { bodyText = this.responseType && this.responseType !== 'text' ? `[${this.responseType}]` : String(this.responseText || '') } catch { bodyText = '[unreadable]' }

            console.groupCollapsed(
                `%cXHR ${type.toUpperCase()} ${this.status} ${meta.method} ${meta.url} (${dur})`,
                'font-weight:bold'
            )
            console.log('Request URL', meta.url)
            console.log('Method', meta.method)
            console.log('Request Headers', meta.headers)
            console.log('Request Body', tryParse(limit(meta.body)))
            console.log('Status', this.status)
            console.log('Response Headers', resHeaders)
            console.log('Response Body', tryParse(limit(bodyText)))
            console.groupEnd()
        }

        this.addEventListener('load', done('load'))
        this.addEventListener('error', done('error'))
        this.addEventListener('abort', done('abort'))
        this.addEventListener('timeout', done('timeout'))

        return _send.apply(this, arguments)
    }

    if (window.fetch) {
        const _fetch = window.fetch
        window.fetch = async (...args) => {
            const started = now()
            const [input, init = {}] = args
            const url = typeof input === 'string' ? input : (input && input.url) || ''
            const match = URL_FILTER ? new RegExp(URL_FILTER).test(url) : true

            const res = await _fetch(...args)
            if (!match) return res

            const clone = res.clone()
            let body = ''
            try { body = await clone.text() } catch { body = '[unreadable]' }

            console.groupCollapsed(
                `%cFETCH ${res.status} ${(init.method || 'GET').toUpperCase()} ${url} (${(now() - started).toFixed(1)}ms)`,
                'font-weight:bold'
            )
            console.log('Request URL', url)
            console.log('Method', init.method || 'GET')
            console.log('Request Headers', init.headers || {})
            console.log('Request Body', tryParse(limit(init.body || '')))
            console.log('Status', res.status)
            console.log('Response Headers', [...res.headers.entries()])
            console.log('Response Body', tryParse(limit(body)))
            console.groupEnd()

            return res
        }
    }

    console.log('Network logger active. Set URL_FILTER for filtering. Paste again to update settings.')
})()
