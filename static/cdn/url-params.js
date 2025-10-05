(function captureMarketingParams() {
    const FIRST_TOUCH = false;

    const UTM_KEYS = [
        "utm_source","utm_medium","utm_campaign","utm_content","utm_term",
        "utm_id","utm_source_platform","utm_creative_format","utm_marketing_tactic","utm_campaign_id"
    ];
    const GOOGLE_KEYS = ["gclid","gbraid","wbraid","gclsrc","dclid","s_kwcid"];
    const MICROSOFT_KEYS = ["msclkid"];
    const META_KEYS = ["fbclid"];
    const TIKTOK_KEYS = ["ttclid","ttadid","tt_campaign_id","tt_adgroup_id","tt_ad_id","ttp"];
    const SNAP_KEYS = ["sc_click_id","snap_click_id","sccid","ScCid"];
    const TWITTER_KEYS = ["twclid"];
    const LINKEDIN_KEYS = ["li_fat_id"];
    const PINTEREST_KEYS = ["epik","pinclid"];
    const REDDIT_KEYS = ["rdt_cid"];
    const QUORA_KEYS = ["qclid"];
    const YAHOO_KEYS = ["yclid"];
    const APPLE_KEYS = ["pt","ct","mt","at","itscg","itsct"];
    const HUBSPOT_KEYS = ["hsa_cam","hsa_grp","hsa_mt","hsa_src","hsa_ad","hsa_acc","hsa_net","hsa_kw","hsa_tgt","hsa_ver"];
    const EXTRA_KEYS = ["ad_id","adset_id","campaign_id","placement","lang","ref","cid"];

    const ALL_KEYS = [
        ...UTM_KEYS,
        ...GOOGLE_KEYS, ...MICROSOFT_KEYS, ...META_KEYS, ...TIKTOK_KEYS, ...SNAP_KEYS,
        ...TWITTER_KEYS, ...LINKEDIN_KEYS, ...PINTEREST_KEYS, ...REDDIT_KEYS, ...QUORA_KEYS,
        ...YAHOO_KEYS, ...APPLE_KEYS, ...HUBSPOT_KEYS, ...EXTRA_KEYS
    ];

    const storage = (() => {
        const sources = [
            () => window.localStorage,
            () => window.sessionStorage
        ];
        for (let i = 0; i < sources.length; i++) {
            try {
                const s = sources[i]();
                if (!s) continue;
                const testKey = "__kepixel_storage_test__";
                s.setItem(testKey, "1");
                s.removeItem(testKey);
                return s;
            } catch (err) {
                /* try next storage */
            }
        }
        return null;
    })();

    function getCookie(name) {
        const parts = document.cookie ? document.cookie.split("; ") : [];
        for (let i = 0; i < parts.length; i++) {
            const row = parts[i];
            if (row.startsWith(name + "=")) {
                return decodeURIComponent(row.slice(name.length + 1));
            }
        }
        return null;
    }

    function setCookie(name, value, days) {
        if (value == null || value === "") return;
        if (FIRST_TOUCH && getExistingValue(name) != null) return;
        const expires = new Date(Date.now() + (days || 30) * 864e5).toUTCString();
        const encoded = encodeURIComponent(String(value));
        const cookie = [`${name}=${encoded}`, `expires=${expires}`, "path=/"];
        const isHttps = location.protocol === "https:";
        if (isHttps) {
            cookie.push("SameSite=None", "Secure");
        } else {
            cookie.push("SameSite=Lax");
        }
        document.cookie = cookie.join("; ");
    }

    function getStoredValue(name) {
        if (!storage) return null;
        try {
            const raw = storage.getItem(name);
            if (!raw) return null;
            const payload = JSON.parse(raw);
            if (payload && typeof payload === "object" && "value" in payload) {
                if (payload.expiresAt && Date.now() > payload.expiresAt) {
                    storage.removeItem(name);
                    return null;
                }
                return payload.value;
            }
            return raw;
        } catch (err) {
            const raw = storage.getItem(name);
            return raw || null;
        }
    }

    function setStoredValue(name, value, days) {
        if (!storage) return;
        if (FIRST_TOUCH && getExistingValue(name) != null) return;
        if (value == null || value === "") return;
        const expiresAt = Date.now() + (days || 30) * 864e5;
        const payload = {
            value: String(value),
            expiresAt
        };
        try {
            storage.setItem(name, JSON.stringify(payload));
        } catch (err) {
            /* noop */
        }
    }

    function getExistingValue(name) {
        const stored = getStoredValue(name);
        if (stored != null) return stored;
        return getCookie(name);
    }

    function persistValue(name, value, days) {
        if (value == null || value === "") return;
        setStoredValue(name, value, days);
        setCookie(name, value, days);
    }

    function parseQuery(qs) {
        const obj = {};
        if (!qs) return obj;
        qs.replace(/^\?/, "").split("&").forEach(p => {
            if (!p) return;
            const i = p.indexOf("=");
            const k = decodeURIComponent(i === -1 ? p : p.slice(0, i)).trim();
            const v = decodeURIComponent(i === -1 ? "" : p.slice(i + 1)).trim();
            if (k) obj[k] = v;
        });
        return obj;
    }

    function getParams() {
        const searchParams = parseQuery(window.location.search || "");
        if (Object.keys(searchParams).length) return searchParams;
        const hash = window.location.hash || "";
        const qIndex = hash.indexOf("?");
        return qIndex === -1 ? {} : parseQuery(hash.slice(qIndex));
    }

    function detectPlatform(p) {
        if (p.gclid || p.gbraid || p.wbraid || p.dclid) return "google";
        if (p.msclkid) return "microsoft";
        if (p.fbclid) return "meta";
        if (p.ttclid || p.ttadid) return "tiktok";
        if (p.sc_click_id || p.snap_click_id || p.sccid || p.ScCid) return "snapchat";
        if (p.twclid) return "twitter";
        if (p.li_fat_id) return "linkedin";
        if (p.epik || p.pinclid) return "pinterest";
        if (p.rdt_cid) return "reddit";
        if (p.qclid) return "quora";
        if (p.yclid) return "yahoo";
        if (p.pt || p.ct || p.mt || p.at) return "apple_search_ads";
        return "";
    }

    function ensureMetaCookiesFromFbclid(params) {
        const nowSeconds = Math.floor(Date.now() / 1000);
        if (params.fbclid) {
            const fbc = `fb.1.${nowSeconds}.${params.fbclid}`;
            persistValue("_fbc", fbc, 90);
        }
        if (!getExistingValue("_fbp")) {
            const rand = Math.floor(Math.random() * 1e10);
            const fbp = `fb.1.${nowSeconds}.${rand}`;
            persistValue("_fbp", fbp, 90);
        }
    }

    function ensureGoogleFirstParty(params) {
        const ts = Math.floor(Date.now() / 1000);
        const g = params.gclid;
        if (g) {
            const formatted = `GCL.${ts}.${g}`;
            persistValue("_gcl_aw", formatted, 90);
            persistValue("_gcl_dc", formatted, 90);
            persistValue("_gcl_gb", formatted, 90);
            persistValue("FPGCLAW", formatted, 90);
            persistValue("FPGCLDC", formatted, 90);
        }
        const gbraid = params.gbraid;
        if (gbraid) {
            const formatted = `GCLB.${ts}.${gbraid}`;
            persistValue("_gcl_gb", formatted, 90);
            persistValue("FPGCLGB", formatted, 90);
        }
        const wbraid = params.wbraid;
        if (wbraid) {
            const formatted = `GCLW.${ts}.${wbraid}`;
            persistValue("_gcl_gb", formatted, 90);
            persistValue("FPGCLGB", formatted, 90);
        }
    }

    function ensureTikTokCookies(params) {
        if (params.ttp) persistValue("_ttp", params.ttp, 90);
        const existingTtp = getExistingValue("_ttp");
        if (!existingTtp || existingTtp === "tt.tt.1") {
            const ts = Math.floor(Date.now()/1000);
            const rand = Math.floor(Math.random()*1e10);
            persistValue("_ttp", `tt.${ts}.${rand}`, 90);
        }
    }

    function ensureSnapchatCookies(params) {
        const scid = params.sc_click_id || params.snap_click_id || params.sccid || params.ScCid;
        if (scid) persistValue("_scclid", scid, 90);
        if (!getExistingValue("_scid")) {
            const ts = Math.floor(Date.now()/1000);
            const rand = Math.floor(Math.random()*1e10);
            persistValue("_scid", `sc.${ts}.${rand}`, 90);
        }
    }

    function ensureMicrosoftCookies(params) {
        if (params.msclkid) persistValue("_uetmsclkid", params.msclkid, 90);
    }

    function ensurePinterestCookies(params) {
        if (params.epik) persistValue("_epik", params.epik, 90);
    }

    function ensureTwitterCookies(params) {
        if (params.twclid) persistValue("_twclid", params.twclid, 90);
    }

    const params = getParams();
    const captured = {};

    ALL_KEYS.forEach(k => {
        if (params[k]) {
            captured[k] = params[k];
            persistValue(k, params[k], 90);
        }
    });

    ensureGoogleFirstParty(params);
    ensureMetaCookiesFromFbclid(params);
    ensureTikTokCookies(params);
    ensureSnapchatCookies(params);
    ensureMicrosoftCookies(params);
    ensurePinterestCookies(params);
    ensureTwitterCookies(params);

    const platform = detectPlatform(params);
    if (platform) persistValue("ad_platform", platform, 90);

    if (document.referrer) persistValue("referrer", document.referrer, 30);

    if (Object.keys(captured).length) {
        persistValue("utm_all", JSON.stringify(captured), 90);
    }
})();
