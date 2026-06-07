/* StockFlow 프론트엔드 공통 유틸 + API 래퍼 */

const API = {
    async get(path) {
        const res = await fetch(path, { headers: { 'Accept': 'application/json' } });
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json();
    },
    async post(path, body) {
        const res = await fetch(path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
        return res.status === 204 ? null : res.json();
    },
    async del(path) {
        const res = await fetch(path, { method: 'DELETE' });
        return res.ok;
    },

    // ---- 도메인별 엔드포인트 ----
    stocks: (marketType) =>
        API.get('/api/stocks' + (marketType ? `?marketType=${marketType}` : '')),
    stock: (symbol) => API.get(`/api/stocks/${encodeURIComponent(symbol)}`),
    ohlcv: (symbol, from, to) =>
        API.get(`/api/stocks/${encodeURIComponent(symbol)}/ohlcv?from=${from}&to=${to}`),
    intraday: (symbol, interval, from, to) =>
        API.get(`/api/stocks/${encodeURIComponent(symbol)}/intraday?interval=${interval}`
            + `&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`),
    indicators: (symbol, from, to) =>
        API.get(`/api/stocks/${encodeURIComponent(symbol)}/indicators?from=${from}&to=${to}`),
    latestPrice: (symbol) => API.get(`/api/price/${encodeURIComponent(symbol)}`),
    marketOverview: (marketType) =>
        API.get('/api/market/overview' + (marketType ? `?marketType=${marketType}` : '')),

    strategies: (symbol) =>
        API.get('/api/backtest/strategies' + (symbol ? `?symbol=${symbol}` : '')),
    createStrategy: (body) => API.post('/api/backtest/strategies', body),
    deleteStrategy: (id) => API.del(`/api/backtest/strategies/${id}`),
    runAdHoc: (body) => API.post('/api/backtest/run', body),
    runSaved: (id, from, to) =>
        API.post(`/api/backtest/strategies/${id}/run?from=${from}&to=${to}`, {}),
    runTrades: (runId) => API.get(`/api/backtest/runs/${runId}/trades`),
    equityCurve: (runId) => API.get(`/api/backtest/runs/${runId}/equity-curve`),
};

// ---- 포맷 헬퍼 ----
function fmtNum(v, digits = 2) {
    if (v == null || v === '') return '-';
    const n = Number(v);
    if (Number.isNaN(n)) return '-';
    return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
function fmtPrice(v) {
    if (v == null) return '-';
    const n = Number(v);
    if (n !== 0 && Math.abs(n) < 1) return n.toFixed(6);
    return fmtNum(n, 2);
}
function fmtPct(v) {
    if (v == null) return '-';
    const n = Number(v);
    return (n > 0 ? '+' : '') + n.toFixed(2) + '%';
}
function signClass(v) {
    if (v == null) return 'neutral';
    const n = Number(v);
    return n > 0 ? 'up' : n < 0 ? 'down' : 'neutral';
}
function fmtTime(ts) {
    if (!ts) return '-';
    return new Date(ts).toLocaleTimeString('ko-KR');
}
function isoDaysAgo(days) {
    const d = new Date();
    d.setDate(d.getDate() - days);
    return d.toISOString().slice(0, 10);
}
function todayIso() { return new Date().toISOString().slice(0, 10); }

function toast(msg, type = '') {
    const el = document.createElement('div');
    el.className = 'toast ' + type;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3500);
}

// ---- 상단 네비 주입 ----
function renderNav(active) {
    const links = [
        ['index.html', '대시보드'],
        ['stocks.html', '종목 & 차트'],
        ['backtest.html', '백테스팅'],
        ['/price-monitor.html', '실시간 모니터(데모)'],
    ];
    const nav = links.map(([href, label]) =>
        `<a href="${href}" class="${href.includes(active) ? 'active' : ''}">${label}</a>`
    ).join('');
    document.body.insertAdjacentHTML('afterbegin', `
        <div class="topbar">
            <div class="brand">📈 StockFlow</div>
            <nav>${nav}</nav>
            <div id="connPill" class="conn-pill"><span class="dot"></span><span id="connText">대기</span></div>
        </div>
    `);
}
function setConn(state, text) {
    const pill = document.getElementById('connPill');
    if (!pill) return;
    pill.className = 'conn-pill ' + (state === 'on' ? 'on' : state === 'off' ? 'off' : '');
    document.getElementById('connText').textContent = text;
}

// ---- STOMP 실시간 ----
function connectStomp(onConnect, onError) {
    const socket = new SockJS('/ws');
    const client = Stomp.over(socket);
    client.debug = null;
    setConn('', '연결 중...');
    client.connect({},
        () => { setConn('on', '실시간 연결됨'); onConnect && onConnect(client); },
        (err) => { setConn('off', '연결 끊김 - 재시도'); onError && onError(err); setTimeout(() => connectStomp(onConnect, onError), 5000); }
    );
    return client;
}
