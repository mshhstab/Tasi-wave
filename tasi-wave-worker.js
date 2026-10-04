// ============================================================
//  tasi-wave — سكرينر سهم على موجة السوق (Wave Stock v2)
//
//  المسارات:
//  1) /          → صفحة السكرينر (الفحص يتم في المتصفح عبر tasi-proxy)
//  2) /sectors   → قطاع واسم كل سهم من TradingView scanner (كاش 24 ساعة)
//
//  المنطق مطابق لـ Pine:
//  - إشارة السوق: TASI Range v3 (محاكاة f_tasi في Wave Stock v2)
//  - وقف الحساب: قاع آخر 20 يوم − 0.5 × ATR(14)
//  - الكمية: الأقل بين (مخاطرة السهم ÷ (السعر − الوقف)) و (رأس المال ÷ عدد الأسهم ÷ السعر)
//  - الاختيار: أعلى 20 سيولة، الأعلى سيولة من كل قطاع لين نوصل 4
// ============================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/sectors') return handleSectors(ctx);
    if (url.pathname === '/tasi') return handleTasi(ctx, url.searchParams.get('debug') === '1');
    return new Response(HTML, {
      headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' },
    });
  },
};

// ============================================================
//  تاريخ تاسي من TradingView (ياهو يرجع شمعة وحدة للمؤشر)
// ============================================================
async function handleTasi(ctx, debug) {
  const headers = { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' };
  const cache = caches.default;
  const key = new Request('https://tasi-wave-cache.internal/tasi');
  if (!debug) {
    const hit = await cache.match(key);
    if (hit) return new Response(await hit.text(), { headers: { ...headers, 'X-Cache': 'HIT' } });
  }
  try {
    const raw = await tvBars('TADAWUL:TASI', 1500);
    const b = { t: [], o: [], h: [], l: [], c: [] };
    for (const x of raw || []) {
      const v = x.v;
      if (!v || v.length < 5 || v.slice(0, 5).some(n => n == null)) continue;
      b.t.push(v[0] * 1000); b.o.push(v[1]); b.h.push(v[2]); b.l.push(v[3]); b.c.push(v[4]);
    }
    if (b.c.length < 200) throw new Error('only ' + b.c.length + ' bars');
    const payload = JSON.stringify(debug
      ? { ok: true, source: 'tradingview', bars: b.c.length, first: new Date(b.t[0]).toISOString().slice(0, 10),
          last: new Date(b.t[b.t.length - 1]).toISOString().slice(0, 10), lastClose: b.c[b.c.length - 1] }
      : { source: 'tradingview', bars: b });
    if (!debug) ctx.waitUntil(cache.put(key, new Response(payload, {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=600' } })));
    return new Response(payload, { headers });
  } catch (e) {
    return new Response(JSON.stringify({ error: 'no TASI history', detail: String(e) }), { status: 502, headers });
  }
}

// اتصال WebSocket بخادم بيانات TradingView (بدون تسجيل دخول)
async function tvBars(symbol, count) {
  const resp = await fetch('https://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart', {
    headers: { 'Upgrade': 'websocket', 'Origin': 'https://www.tradingview.com',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)' },
  });
  const ws = resp.webSocket;
  if (!ws) throw new Error('websocket refused, HTTP ' + resp.status);
  ws.accept();
  const frame = (str) => '~m~' + str.length + '~m~' + str;
  const send = (m, p) => ws.send(frame(JSON.stringify({ m, p })));

  return new Promise((resolve, reject) => {
    let bars = null, done = false;
    const finish = (err) => {
      if (done) return; done = true; clearTimeout(timer);
      try { ws.close(); } catch (e) {}
      if (bars) resolve(bars); else reject(err || new Error('no data'));
    };
    const timer = setTimeout(() => finish(new Error('timeout')), 15000);
    ws.addEventListener('message', (ev) => {
      const data = typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data);
      for (const part of data.split(/~m~\d+~m~/)) {
        if (!part) continue;
        if (part.startsWith('~h~')) { ws.send(frame(part)); continue; }
        let j; try { j = JSON.parse(part); } catch (e) { continue; }
        if (j.m === 'timescale_update' && j.p && j.p[1] && j.p[1].sds_1 && j.p[1].sds_1.s) bars = j.p[1].sds_1.s;
        else if (j.m === 'series_completed') finish();
        else if (j.m === 'symbol_error' || j.m === 'series_error' || j.m === 'critical_error' || j.m === 'protocol_error')
          finish(new Error(j.m + ' ' + JSON.stringify(j.p)));
      }
    });
    ws.addEventListener('close', () => finish(new Error('socket closed')));
    ws.addEventListener('error', () => finish(new Error('socket error')));
    send('set_auth_token', ['unauthorized_user_token']);
    send('chart_create_session', ['cs_wave', '']);
    send('resolve_symbol', ['cs_wave', 'sds_sym_1', '=' + JSON.stringify({ symbol, adjustment: 'splits', session: 'regular' })]);
    send('create_series', ['cs_wave', 'sds_1', 's1', 'sds_sym_1', '1D', count, '']);
  });
}

async function handleSectors(ctx) {
  const headers = { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' };
  const cache = caches.default;
  const key = new Request('https://tasi-wave-cache.internal/sectors');
  const hit = await cache.match(key);
  if (hit) return new Response(await hit.text(), { headers: { ...headers, 'X-Cache': 'HIT' } });

  try {
    const r = await fetch('https://scanner.tradingview.com/ksa/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)' },
      body: JSON.stringify({
        filter: [{ left: 'type', operation: 'equal', right: 'stock' }],
        markets: ['ksa'],
        columns: ['name', 'description', 'sector'],
        range: [0, 800],
      }),
    });
    if (!r.ok) throw new Error('TradingView HTTP ' + r.status);
    const data = await r.json();
    const map = {};
    for (const row of data.data || []) {
      const p = String(row.s || '').split(':');
      if (p.length !== 2 || p[0] !== 'TADAWUL' || !/^\d{4}$/.test(p[1])) continue;
      map[p[1]] = { name: (row.d && row.d[1]) || '', sector: (row.d && row.d[2]) || '' };
    }
    if (Object.keys(map).length < 100) throw new Error('suspicious result');
    const payload = JSON.stringify({ updated: new Date().toISOString(), map });
    ctx.waitUntil(cache.put(key, new Response(payload, {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=86400' },
    })));
    return new Response(payload, { headers: { ...headers, 'X-Cache': 'MISS' } });
  } catch (e) {
    return new Response(JSON.stringify({ error: 'sectors fetch failed', detail: String(e) }), { status: 502, headers });
  }
}

// ============================================================
//  الصفحة
// ============================================================
const HTML = String.raw`<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>سكرينر سهم على موجة السوق — TASI</title>
<style>
  :root {
    --bg:#131722; --panel:#1e222d; --panel2:#262b38; --line:#2a2e39; --line2:#363c4e;
    --text:#d1d4dc; --white:#ffffff; --muted:#787b86; --blue:#2962ff;
    --green:#089981; --red:#f23645; --yellow:#f7a600; --sky:#42a5f5;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--text);
         font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Tahoma, sans-serif; }
  .wrap { max-width:1200px; margin:0 auto; padding:24px 16px 48px; }
  h1 { color:var(--white); font-size:24px; margin:0 0 8px; }
  .sub { color:var(--muted); font-size:15px; line-height:1.6; margin:0 0 20px; }
  .btns { display:flex; gap:12px; flex-wrap:wrap; }
  button { font:inherit; cursor:pointer; border-radius:10px; }
  .btn-main { background:var(--blue); color:#fff; border:0; padding:13px 26px; font-size:16px; }
  .btn-main:disabled { opacity:.55; cursor:default; }
  .btn-sec { background:var(--panel); color:var(--white); border:1px solid var(--line2); padding:12px 24px; font-size:16px; }
  button:focus-visible, input:focus-visible { outline:2px solid var(--sky); outline-offset:2px; }

  .settings { display:none; margin-top:16px; background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:16px; }
  .settings.open { display:block; }
  .settings h3 { color:var(--white); font-size:15px; margin:14px 0 10px; }
  .settings h3:first-child { margin-top:0; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(170px, 1fr)); gap:10px; }
  .grid label { display:flex; flex-direction:column; gap:4px; font-size:13px; color:var(--muted); }
  .grid input { background:var(--bg); color:var(--white); border:1px solid var(--line2); border-radius:6px; padding:8px; font-size:15px; }

  .progress { margin-top:18px; color:var(--muted); font-size:14px; display:none; }
  .bar { height:4px; background:var(--line); border-radius:2px; margin-top:6px; overflow:hidden; }
  .bar > div { height:100%; width:0; background:var(--blue); transition:width .2s; }

  .market { display:none; margin-top:20px; background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:16px; }
  .m-head { display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }
  .m-state { font-size:20px; font-weight:700; color:var(--white); }
  .m-note { color:var(--muted); font-size:14px; margin-top:4px; line-height:1.6; }
  .m-date { color:var(--muted); font-size:13px; }
  .stats { display:grid; grid-template-columns:repeat(auto-fill, minmax(130px, 1fr)); gap:8px; margin-top:14px; }
  .stat { background:var(--bg); border:1px solid var(--line); border-radius:8px; padding:10px; }
  .stat span { display:block; color:var(--muted); font-size:12px; margin-bottom:4px; }
  .stat b { color:var(--white); font-size:16px; font-weight:600; }
  .rangebar { position:relative; height:10px; border-radius:5px; margin:18px 0 6px;
              background:linear-gradient(to left, rgba(8,153,129,.45) 0 15%, var(--panel2) 15% 85%, rgba(242,54,69,.45) 85% 100%); }
  .rangebar i { position:absolute; top:-4px; width:4px; height:18px; background:var(--white); border-radius:2px; }
  .rb-labels { display:flex; justify-content:space-between; font-size:12px; color:var(--muted); }
  .checks { display:flex; flex-wrap:wrap; gap:8px; margin-top:12px; }
  .chk { background:var(--bg); border:1px solid var(--line); border-radius:6px; padding:6px 10px; font-size:13px; }
  .ok { color:var(--green); } .no { color:var(--red); }
  .waves { margin-top:14px; font-size:13px; color:var(--muted); }
  .waves table { margin-top:6px; }

  .filters { display:none; gap:8px; margin-top:20px; flex-wrap:wrap; }
  .filters button { background:var(--panel); color:var(--text); border:1px solid var(--line2); padding:8px 14px; font-size:14px; }
  .filters button.on { background:var(--blue); border-color:var(--blue); color:#fff; }
  .summary { color:var(--muted); font-size:14px; margin-top:12px; }

  .tablebox { overflow-x:auto; margin-top:12px; border:1px solid var(--line); border-radius:10px; display:none; }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th { background:var(--panel); color:var(--muted); font-weight:500; text-align:right; padding:10px; white-space:nowrap; position:sticky; top:0; }
  td { padding:10px; border-top:1px solid var(--line); white-space:nowrap; }
  tr.pick td { background:rgba(41,98,255,.10); }
  tr.pick td:first-child { box-shadow:inset -3px 0 0 var(--blue); }
  tr.sens td { background:rgba(156,39,176,.12); }
  tr.sens td:first-child { box-shadow:inset -3px 0 0 #ab47bc; }
  .br { display:inline-block; background:rgba(171,71,188,.22); color:#ce93d8; border:1px solid rgba(171,71,188,.5);
        border-radius:5px; padding:1px 6px; font-size:12px; font-weight:600; font-style:italic; margin-right:4px; }
  th.sort { cursor:pointer; user-select:none; }
  th.sort.on { color:var(--white); }
  .legend { color:var(--muted); font-size:13px; margin-top:10px; line-height:1.7; }
  .sym b { color:var(--white); }
  .sym small { display:block; color:var(--muted); font-size:12px; max-width:180px; overflow:hidden; text-overflow:ellipsis; }
  .num { font-variant-numeric:tabular-nums; }
  .muted { color:var(--muted); }
  .err { color:var(--red); margin-top:14px; font-size:14px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>سكرينر سهم على موجة السوق 🌊</h1>
  <p class="sub">مطابق لاستراتيجية Wave Stock v2 — إشارة الدخول والخروج من تاسي v3، والخروج مع السوق فقط | الاختيار: أعلى 20 سيولة، الأعلى من كل قطاع</p>

  <div class="btns">
    <button class="btn-main" id="scanBtn">🔍 فحص السوق</button>
    <button class="btn-sec" id="setBtn">⚙️ الإعدادات</button>
  </div>

  <div class="settings" id="settings">
    <h3>إشارات السوق (v3)</h3>
    <div class="grid">
      <label>طول النطاق (أيام)<input type="number" id="rngLen" value="120"></label>
      <label>منطقة القاع/السقف %<input type="number" id="zonePct" value="15" step="1"></label>
      <label>أدنى عرض للنطاق %<input type="number" id="minRange" value="8" step="0.5"></label>
      <label>لمس المنطقة خلال (أيام)<input type="number" id="touchBars" value="3"></label>
      <label>قاع الارتداد: آخر (أيام)<input type="number" id="stopLook" value="5"></label>
      <label>مسافة الوقف × ATR<input type="number" id="stopBuf" value="0.5" step="0.1"></label>
    </div>
    <h3>إدارة المخاطر</h3>
    <div class="grid">
      <label>رأس المال<input type="number" id="capital" value="400000"></label>
      <label>المخاطرة الكلية للموجة %<input type="number" id="totalRisk" value="5" step="0.5"></label>
      <label>عدد الأسهم في الموجة<input type="number" id="nStocks" value="4" min="1"></label>
      <label>وقف الحساب: قاع آخر (أيام)<input type="number" id="sStopLook" value="20"></label>
      <label>مسافة وقف الحساب × ATR<input type="number" id="sStopBuf" value="0.5" step="0.1"></label>
    </div>
    <h3>الاختيار</h3>
    <div class="grid">
      <label>أعلى كم سهم سيولة<input type="number" id="topN" value="20"></label>
      <label>فترة متوسط السيولة (أيام)<input type="number" id="liqLen" value="20"></label>
      <label>أدنى متوسط قيمة تداول (مليون)<input type="number" id="minValueM" value="15"></label>
    </div>
    <h3>اختيار الحساسية βr</h3>
    <div class="grid">
      <label>أدنى بيتا β<input type="number" id="minBeta" value="1" step="0.1"></label>
      <label>أدنى ارتباط r<input type="number" id="minCorr" value="0.5" step="0.05"></label>
      <label>فترة الحساب (أيام)<input type="number" id="betaLen" value="120"></label>
    </div>
  </div>

  <div class="progress" id="progress"><span id="progText">جاري الفحص…</span><div class="bar"><div id="progBar"></div></div></div>
  <div class="err" id="err"></div>

  <div class="market" id="market"></div>

  <div class="filters" id="filters">
    <button data-f="all" class="on">الكل</button>
    <button data-f="pick">المختارة ⭐</button>
    <button data-f="sens">الحساسية βr</button>
    <button data-f="top">أعلى 20</button>
  </div>
  <div class="summary" id="summary"></div>

  <div class="tablebox" id="tablebox">
    <table>
      <thead><tr>
        <th class="sort" data-k="rank">#</th><th>السهم</th><th>القطاع</th>
        <th class="sort" data-k="beta">β</th><th class="sort" data-k="corr">r</th>
        <th class="sort" data-k="close">السعر</th><th class="sort" data-k="avgValue">متوسط السيولة</th>
        <th class="sort" data-k="sStop">وقف الحساب</th><th class="sort" data-k="distPct">البُعد عن الوقف</th>
        <th class="sort" data-k="qty">الكمية</th><th class="sort" data-k="posValue">قيمة المركز</th><th>الحالة</th>
      </tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
  <div class="legend" id="legend" style="display:none">β البيتا، r الارتباط بالسوق — آخر <span id="legLen">120</span> يوم | اضغط على رأس أي عمود للترتيب، ومرة ثانية لعكسه | ⭐ الاختيار الرسمي، <span class="br">βr</span> اختيار الحساسية</div>
</div>

<script>
var PROXY = 'https://tasi-proxy.mshhstab.workers.dev';
var TASI_TICKER = '^TASI.SR';

var SECTOR_AR = {
  'Finance':'المالية', 'Energy Minerals':'الطاقة', 'Non-Energy Minerals':'التعدين والمواد',
  'Process Industries':'الصناعات التحويلية', 'Communications':'الاتصالات', 'Utilities':'المرافق',
  'Retail Trade':'التجزئة', 'Consumer Non-Durables':'السلع الاستهلاكية', 'Consumer Durables':'السلع المعمرة',
  'Consumer Services':'الخدمات الاستهلاكية', 'Health Services':'الخدمات الصحية', 'Health Technology':'التقنية الصحية',
  'Technology Services':'خدمات التقنية', 'Electronic Technology':'التقنية الإلكترونية',
  'Producer Manufacturing':'التصنيع', 'Industrial Services':'الخدمات الصناعية', 'Transportation':'النقل',
  'Commercial Services':'الخدمات التجارية', 'Distribution Services':'التوزيع', 'Miscellaneous':'متنوع'
};

var rows = [];
var currentFilter = 'all';
var sortKey = 'rank', sortDesc = false;
var tasiByDate = null;

function $(id) { return document.getElementById(id); }
function num(id) { return parseFloat($(id).value); }

$('setBtn').onclick = function () { $('settings').classList.toggle('open'); };
$('scanBtn').onclick = scan;
Array.prototype.forEach.call(document.querySelectorAll('#filters button'), function (b) {
  b.onclick = function () {
    Array.prototype.forEach.call(document.querySelectorAll('#filters button'), function (x) { x.classList.remove('on'); });
    b.classList.add('on');
    currentFilter = b.getAttribute('data-f');
    renderTable();
  };
});

Array.prototype.forEach.call(document.querySelectorAll('th.sort'), function (h) {
  h.onclick = function () {
    var k = h.getAttribute('data-k');
    if (k === sortKey) sortDesc = !sortDesc;
    else { sortKey = k; sortDesc = k !== 'rank'; }
    renderTable();
  };
});

function getParams() {
  return {
    rngLen: num('rngLen'), zonePct: num('zonePct'), minRange: num('minRange'),
    touchBars: num('touchBars'), stopLook: num('stopLook'), stopBuf: num('stopBuf'),
    capital: num('capital'), totalRisk: num('totalRisk'), nStocks: Math.max(1, Math.round(num('nStocks'))),
    sStopLook: num('sStopLook'), sStopBuf: num('sStopBuf'),
    topN: Math.round(num('topN')), liqLen: Math.round(num('liqLen')), minValue: num('minValueM') * 1e6,
    minBeta: num('minBeta'), minCorr: num('minCorr'), betaLen: Math.round(num('betaLen'))
  };
}

// ───────── جلب البيانات ─────────
function fetchBars(ticker, range) {
  var u = PROXY + '/?ticker=' + encodeURIComponent(ticker) + '&interval=1d&range=' + range + '&_=' + Date.now();
  return fetch(u, { cache: 'no-store' }).then(function (r) { return r.json(); }).then(function (d) {
    var res = d && d.chart && d.chart.result && d.chart.result[0];
    if (!res || !res.timestamp) return null;
    var q = res.indicators.quote[0];
    var b = { t: [], o: [], h: [], l: [], c: [], v: [] };
    for (var i = 0; i < res.timestamp.length; i++) {
      if (q.open[i] == null || q.high[i] == null || q.low[i] == null || q.close[i] == null) continue;
      b.t.push(res.timestamp[i] * 1000);
      b.o.push(q.open[i]); b.h.push(q.high[i]); b.l.push(q.low[i]); b.c.push(q.close[i]);
      b.v.push(q.volume && q.volume[i] != null ? q.volume[i] : 0);
    }
    return b.c.length ? b : null;
  }).catch(function () { return null; });
}

function fetchTasi() {
  return fetch('/tasi', { cache: 'no-store' }).then(function (r) { return r.json(); })
    .then(function (d) { return d && d.bars && d.bars.c.length ? d.bars : null; })
    .catch(function () { return null; });
}

function fetchSymbols() {
  return fetch(PROXY + '/symbols').then(function (r) { return r.json(); })
    .then(function (d) { return d && d.symbols && d.symbols.length ? d.symbols : null; })
    .catch(function () { return null; });
}

function fetchSectors() {
  return fetch('/sectors').then(function (r) { return r.json(); })
    .then(function (d) { return d && d.map ? d.map : {}; })
    .catch(function () { return {}; });
}

// ───────── مؤشرات بنفس سلوك Pine ─────────
function atrArr(b, n) {
  var len = b.c.length, tr = [], out = new Array(len).fill(null);
  for (var i = 0; i < len; i++) {
    if (i === 0) tr.push(b.h[0] - b.l[0]);
    else tr.push(Math.max(b.h[i] - b.l[i], Math.abs(b.h[i] - b.c[i - 1]), Math.abs(b.l[i] - b.c[i - 1])));
  }
  if (len < n) return out;
  var s = 0;
  for (var j = 0; j < n; j++) s += tr[j];
  out[n - 1] = s / n;
  for (var k = n; k < len; k++) out[k] = (out[k - 1] * (n - 1) + tr[k]) / n;
  return out;
}
function lowest(a, end, n) { var m = Infinity; for (var i = Math.max(0, end - n + 1); i <= end; i++) m = Math.min(m, a[i]); return m; }
function highest(a, end, n) { var m = -Infinity; for (var i = Math.max(0, end - n + 1); i <= end; i++) m = Math.max(m, a[i]); return m; }

// ───────── محاكاة تاسي v3 (مطابقة f_tasi) ─────────
function simTasi(b, p) {
  var n = b.c.length, atr = atrArr(b, 14);
  var pos = false, stp = null, ceilHit = false, entryIdx = -1;
  var waves = [], last = null;
  for (var i = p.rngLen; i < n; i++) {
    var rH = highest(b.h, i - 1, p.rngLen), rL = lowest(b.l, i - 1, p.rngLen);
    var rW = rH - rL, rPct = rW / rL * 100;
    var rangeOk = rPct >= p.minRange;
    var floorTop = rL + rW * p.zonePct / 100, ceilBot = rH - rW * p.zonePct / 100;
    var touchedF = lowest(b.l, i, p.touchBars) <= floorTop;
    var touchedC = highest(b.h, i, p.touchBars) >= ceilBot;
    var revUp = b.c[i] > b.o[i] && b.c[i] > b.h[i - 1];
    var revDn = b.c[i] < b.o[i] && b.c[i] < b.l[i - 1];
    var broken = b.c[i] < rL;
    var stopC = atr[i] == null ? null : Math.min(rL, lowest(b.l, i, p.stopLook)) - p.stopBuf * atr[i];
    var entrySig = false, exitSig = false, exitWhy = 0;
    if (!pos) {
      if (rangeOk && touchedF && revUp && !broken && stopC != null) {
        pos = true; stp = stopC; ceilHit = false; entrySig = true; entryIdx = i;
      }
    } else {
      if (b.h[i] >= ceilBot) ceilHit = true;
      if (b.c[i] < stp) { pos = false; exitSig = true; exitWhy = 1; }
      else if (ceilHit && revDn) { pos = false; exitSig = true; exitWhy = 2; }
      if (exitSig) waves.push({ inT: b.t[entryIdx], inP: b.c[entryIdx], outT: b.t[i], outP: b.c[i], why: exitWhy });
    }
    last = { i: i, rH: rH, rL: rL, rPct: rPct, rangeOk: rangeOk, floorTop: floorTop, ceilBot: ceilBot,
             touchedF: touchedF, touchedC: touchedC, revUp: revUp, broken: broken, stopC: stopC,
             entrySig: entrySig, exitSig: exitSig, exitWhy: exitWhy, pos: pos, stp: stp, ceilHit: ceilHit,
             entryIdx: entryIdx, close: b.c[i], t: b.t[i] };
  }
  if (last && last.pos) waves.push({ inT: b.t[entryIdx], inP: b.c[entryIdx], outT: null, outP: last.close, why: 0 });
  return { last: last, waves: waves };
}

// ───────── حسابات السهم (مطابقة Wave Stock v2) ─────────
function calcStock(b, p) {
  var n = b.c.length;
  if (n < Math.max(p.liqLen, p.sStopLook, 15)) return null;
  var i = n - 1, s = 0;
  for (var k = n - p.liqLen; k < n; k++) s += b.c[k] * b.v[k];
  var avgValue = s / p.liqLen;
  var atr = atrArr(b, 14)[i];
  var close = b.c[i];
  var sStop = lowest(b.l, i, p.sStopLook) - p.sStopBuf * atr;
  var riskPer = p.capital * p.totalRisk / 100 / p.nStocks;
  var riskPS = close - sStop;
  var qtyRisk = riskPS > 0 ? Math.floor(riskPer / riskPS) : 0;
  var qtyCap = Math.floor(p.capital / p.nStocks / close);
  var qty = Math.min(qtyRisk, qtyCap);
  var bc = betaCorr(b, p.betaLen);
  return { close: close, avgValue: avgValue, sStop: sStop, distPct: riskPS / close * 100,
           qty: qty, posValue: qty * close, liqOk: avgValue >= p.minValue, t: b.t[i],
           beta: bc ? bc.beta : null, corr: bc ? bc.corr : null };
}

// ───────── البيتا والارتباط (مطابقة v2: عوائد يومية، ta.correlation و ta.stdev) ─────────
function dayKey(t) { return new Date(t + 3 * 3600000).toISOString().slice(0, 10); }
function buildTasiIndex(tb) {
  var keys = [], closes = [];
  for (var i = 0; i < tb.t.length; i++) { keys.push(dayKey(tb.t[i])); closes.push(tb.c[i]); }
  return { keys: keys, closes: closes };
}
function tasiCloseAt(key) {
  var a = tasiByDate.keys, lo = 0, hi = a.length - 1, ans = -1;
  while (lo <= hi) { var m = (lo + hi) >> 1; if (a[m] <= key) { ans = m; lo = m + 1; } else hi = m - 1; }
  return ans < 0 ? null : tasiByDate.closes[ans];
}
function betaCorr(b, n) {
  if (!tasiByDate || b.c.length < n + 1) return null;
  var rs = [], rm = [], prevM = null;
  var start = b.c.length - n - 1;
  for (var i = start; i < b.c.length; i++) {
    var m = tasiCloseAt(dayKey(b.t[i]));
    if (i > start) {
      if (m == null || prevM == null) return null;
      rs.push(b.c[i] / b.c[i - 1] - 1);
      rm.push(m / prevM - 1);
    }
    prevM = m;
  }
  var ms = 0, mm = 0;
  for (var k = 0; k < n; k++) { ms += rs[k]; mm += rm[k]; }
  ms /= n; mm /= n;
  var cov = 0, vs = 0, vm = 0;
  for (k = 0; k < n; k++) { var a = rs[k] - ms, c = rm[k] - mm; cov += a * c; vs += a * a; vm += c * c; }
  if (vs === 0 || vm === 0) return null;
  var corr = cov / Math.sqrt(vs * vm);
  return { corr: corr, beta: corr * Math.sqrt(vs / n) / Math.sqrt(vm / n) };
}

// ───────── الفحص ─────────
function scan() {
  var p = getParams();
  $('scanBtn').disabled = true;
  $('err').textContent = '';
  $('progress').style.display = 'block';
  $('progText').textContent = 'جاري جلب تاسي والقطاعات…';
  $('progBar').style.width = '0';

  Promise.all([fetchTasi(), fetchSymbols(), fetchSectors()]).then(function (res) {
    var tasiBars = res[0], symbols = res[1], sectors = res[2];
    if (!tasiBars || tasiBars.c.length <= p.rngLen + 2) throw new Error('تعذر جلب تاريخ تاسي (افتح /tasi?debug=1 للتفاصيل)');
    var tasi = simTasi(tasiBars, p);
    tasiByDate = buildTasiIndex(tasiBars);
    renderMarket(tasi, p);

    if (!symbols) symbols = Object.keys(sectors).filter(function (c) {
      var x = parseInt(c, 10); return x < 9000 && !(x >= 4330 && x <= 4349);
    }).map(function (c) { return c + '.SR'; });
    if (!symbols.length) throw new Error('تعذر جلب قائمة الرموز');

    var out = [], idx = 0, done = 0, total = symbols.length;
    function worker() {
      if (idx >= total) return Promise.resolve();
      var sym = symbols[idx++];
      return fetchBars(sym, '1y').then(function (b) {
        var r = b ? calcStock(b, p) : null;
        if (r) {
          var code = sym.replace('.SR', '');
          var info = sectors[code] || {};
          r.code = code;
          r.name = info.name || '';
          r.sectorEn = info.sector || '';
          r.sector = SECTOR_AR[info.sector] || info.sector || 'غير معروف';
          r.stale = (tasiBars.t[tasiBars.t.length - 1] - r.t) > 5 * 86400000;
          out.push(r);
        }
        done++;
        $('progText').textContent = 'جاري الفحص ' + done + ' / ' + total;
        $('progBar').style.width = (done / total * 100) + '%';
      }).then(worker);
    }
    var pool = [];
    for (var w = 0; w < 8; w++) pool.push(worker());
    return Promise.all(pool).then(function () { finish(out, p, Object.keys(sectors).length > 0); });
  }).catch(function (e) {
    $('err').textContent = 'فشل الفحص: ' + e.message + ' — تأكد من عمل tasi-proxy ثم أعد المحاولة.';
  }).then(function () {
    $('scanBtn').disabled = false;
    $('progress').style.display = 'none';
  });
}

function finish(list, p, haveSectors) {
  list = list.filter(function (r) { return !r.stale; });
  list.sort(function (a, b) { return b.avgValue - a.avgValue; });
  var used = {}, picks = 0;
  list.forEach(function (r, i) {
    r.rank = i + 1;
    r.top = i < p.topN;
    r.pick = false;
    if (r.top && picks < p.nStocks && r.liqOk && r.qty > 0) {
      var key = haveSectors ? r.sectorEn || ('_' + r.code) : ('_' + r.code);
      if (!used[key]) { used[key] = true; r.pick = true; picks++; }
    }
  });
  var usedS = {}, sPicks = 0;
  list.forEach(function (r) {
    r.sens = false;
    var sensOk = r.beta != null && r.corr != null && r.beta >= p.minBeta && r.corr >= p.minCorr;
    if (r.top && sPicks < p.nStocks && r.liqOk && r.qty > 0 && sensOk) {
      var key = haveSectors ? r.sectorEn || ('_' + r.code) : ('_' + r.code);
      if (!usedS[key]) { usedS[key] = true; r.sens = true; sPicks++; }
    }
  });
  rows = list;
  var tot = 0, totS = 0;
  list.forEach(function (r) { if (r.pick) tot += r.posValue; if (r.sens) totS += r.posValue; });
  $('legLen').textContent = p.betaLen;
  $('legend').style.display = 'block';
  $('summary').textContent = 'تم فحص ' + list.length + ' سهم | ⭐ المختارة: ' + picks + ' بقيمة ' + fmt(tot, 0) +
    ' ريال | βr الحساسية: ' + sPicks + ' بقيمة ' + fmt(totS, 0) + ' ريال' +
    (sPicks < p.nStocks ? ' (أقل من ' + p.nStocks + ' أسهم تحقق الشرط ضمن أعلى ' + p.topN + ')' : '') +
    (haveSectors ? '' : ' | ⚠️ تعذر جلب القطاعات، الاختيار بالسيولة فقط');
  $('filters').style.display = 'flex';
  $('tablebox').style.display = 'block';
  renderTable();
}

// ───────── العرض ─────────
function fmt(x, d) { return x == null || isNaN(x) ? '—' : x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }); }
function dstr(t) { if (!t) return '—'; var d = new Date(t); return d.getFullYear() + '-' + ('0' + (d.getMonth() + 1)).slice(-2) + '-' + ('0' + d.getDate()).slice(-2); }
function mark(ok) { return ok ? '<span class="ok">✓</span>' : '<span class="no">✗</span>'; }

function renderMarket(tasi, p) {
  var L = tasi.last, state, note;
  if (L.entrySig) {
    state = '🟢 دخول الآن';
    note = 'شمعة انعكاس من منطقة القاع على إغلاق آخر جلسة. ادخل الأسهم المختارة ⭐ بالكميات تحت. وقف السوق: ' + fmt(L.stp, 0);
  } else if (L.exitSig) {
    state = '🔴 اخرج الآن';
    note = L.exitWhy === 1 ? 'تاسي أغلق تحت الوقف (كسر القاع). اطلع من كل أسهم الموجة.' : 'بداية نزول من منطقة السقف. اطلع من كل أسهم الموجة.';
  } else if (L.pos) {
    state = '🔵 داخل الموجة';
    note = 'دخلنا ' + dstr(tasiBarsTime(tasi)) + '. نبقى لين إشارة السقف أو كسر الوقف ' + fmt(L.stp, 0) +
           (L.ceilHit ? ' | لمس منطقة السقف ✓ — ننتظر شمعة نزول' : ' | ما لمس منطقة السقف بعد');
  } else if (L.broken) {
    state = '⚠️ تحت قاع النطاق';
    note = 'المؤشر أغلق تحت قاع النطاق. ما فيه دخول لين يتكون نطاق جديد.';
  } else if (L.rangeOk && L.touchedF) {
    state = '🟡 في منطقة القاع';
    note = 'ننتظر شمعة انعكاس: إغلاق أخضر فوق قمة اليوم السابق. لو دخلنا اليوم يكون الوقف ' + fmt(L.stopC, 0);
  } else {
    state = '⚪ ننتظر';
    note = L.rangeOk ? 'المؤشر بعيد عن منطقة القاع.' : 'عرض النطاق أقل من ' + p.minRange + '%، النطاق غير صالح.';
  }
  var posPct = Math.max(0, Math.min(100, (L.close - L.rL) / (L.rH - L.rL) * 100));

  var wavesHtml = '';
  var w = tasi.waves.slice(-4).reverse();
  if (w.length) {
    wavesHtml = '<div class="waves">آخر الموجات على تاسي<table><thead><tr><th>الدخول</th><th>الخروج</th><th>السبب</th><th>التغير</th></tr></thead><tbody>';
    w.forEach(function (x) {
      var ch = (x.outP / x.inP - 1) * 100;
      wavesHtml += '<tr><td>' + dstr(x.inT) + '</td><td>' + (x.outT ? dstr(x.outT) : 'مفتوحة') + '</td><td>' +
        (x.why === 1 ? 'وقف' : x.why === 2 ? 'سقف' : '—') + '</td><td class="num ' + (ch >= 0 ? 'ok' : 'no') + '">' +
        (ch >= 0 ? '+' : '') + fmt(ch, 1) + '%</td></tr>';
    });
    wavesHtml += '</tbody></table></div>';
  }

  $('market').innerHTML =
    '<div class="m-head"><div><div class="m-state">' + state + '</div><div class="m-note">' + note + '</div></div>' +
    '<div class="m-date">آخر جلسة: ' + dstr(L.t) + '</div></div>' +
    '<div class="stats">' +
      '<div class="stat"><span>تاسي</span><b class="num">' + fmt(L.close, 0) + '</b></div>' +
      '<div class="stat"><span>قاع النطاق</span><b class="num">' + fmt(L.rL, 0) + '</b></div>' +
      '<div class="stat"><span>سقف النطاق</span><b class="num">' + fmt(L.rH, 0) + '</b></div>' +
      '<div class="stat"><span>عرض النطاق</span><b class="num">' + fmt(L.rPct, 1) + '%</b></div>' +
      '<div class="stat"><span>' + (L.pos ? 'الوقف الحالي' : 'الوقف لو دخلنا') + '</span><b class="num">' + fmt(L.pos ? L.stp : L.stopC, 0) + '</b></div>' +
    '</div>' +
    '<div class="rangebar"><i style="right:calc(' + posPct + '% - 2px)"></i></div>' +
    '<div class="rb-labels"><span>القاع ' + fmt(L.rL, 0) + '</span><span>موقع المؤشر ' + fmt(posPct, 0) + '%</span><span>السقف ' + fmt(L.rH, 0) + '</span></div>' +
    '<div class="checks">' +
      '<div class="chk">نطاق صالح ' + mark(L.rangeOk) + '</div>' +
      '<div class="chk">في منطقة القاع ' + mark(L.touchedF) + '</div>' +
      '<div class="chk">بداية انعكاس ' + mark(L.revUp) + '</div>' +
      '<div class="chk">في منطقة السقف ' + mark(L.touchedC) + '</div>' +
    '</div>' + wavesHtml;
  $('market').style.display = 'block';
}
function tasiBarsTime(tasi) { var w = tasi.waves[tasi.waves.length - 1]; return w ? w.inT : null; }

function renderTable() {
  var list = rows.filter(function (r) {
    return currentFilter === 'all' || (currentFilter === 'pick' && r.pick) ||
           (currentFilter === 'sens' && r.sens) || (currentFilter === 'top' && r.top);
  });
  var show = function (r) { return r.pick || r.sens; };
  var val = function (r) {
    if ((sortKey === 'sStop' || sortKey === 'distPct' || sortKey === 'qty' || sortKey === 'posValue') && !show(r)) return null;
    return r[sortKey];
  };
  list.sort(function (a, b) {
    var x = val(a), y = val(b);
    if (x == null && y == null) return a.rank - b.rank;
    if (x == null) return 1;
    if (y == null) return -1;
    return sortDesc ? y - x : x - y;
  });
  Array.prototype.forEach.call(document.querySelectorAll('th.sort'), function (h) {
    var base = h.getAttribute('data-base') || h.textContent;
    h.setAttribute('data-base', base);
    var on = h.getAttribute('data-k') === sortKey;
    h.classList.toggle('on', on);
    h.textContent = base + (on ? (sortDesc ? ' ↓' : ' ↑') : '');
  });
  var dash = '<span class="muted">—</span>';
  var html = '';
  list.forEach(function (r) {
    var badge = '<span class="br">βr</span>';
    var status = r.pick && r.sens ? '⭐ ' + badge : r.pick ? '⭐ مختار' : r.sens ? badge :
                 r.top ? (r.liqOk ? 'أعلى 20' : 'أعلى 20 — سيولة أقل من الحد') : '—';
    var cls = r.pick ? 'pick' : r.sens ? 'sens' : '';
    html += '<tr class="' + cls + '">' +
      '<td class="num muted">' + r.rank + '</td>' +
      '<td class="sym"><b>' + r.code + '</b>' + (r.sens ? badge : '') + '<small>' + r.name + '</small></td>' +
      '<td>' + r.sector + '</td>' +
      '<td class="num">' + (r.beta == null ? dash : fmt(r.beta, 2)) + '</td>' +
      '<td class="num">' + (r.corr == null ? dash : fmt(r.corr, 2)) + '</td>' +
      '<td class="num">' + fmt(r.close, 2) + '</td>' +
      '<td class="num">' + fmt(r.avgValue / 1e6, 1) + ' م</td>' +
      '<td class="num">' + (show(r) ? fmt(r.sStop, 2) : dash) + '</td>' +
      '<td class="num">' + (show(r) ? fmt(r.distPct, 1) + '%' : dash) + '</td>' +
      '<td class="num">' + (show(r) ? fmt(r.qty, 0) : dash) + '</td>' +
      '<td class="num">' + (show(r) ? fmt(r.posValue, 0) : dash) + '</td>' +
      '<td>' + status + '</td></tr>';
  });
  $('tbody').innerHTML = html || '<tr><td colspan="12" class="muted">لا توجد أسهم في هذا الفلتر.</td></tr>';
}
</script>
</body>
</html>`;
