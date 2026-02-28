/**
 * app.js
 * 地図初期化・COG 読み込み・スライダー・レイヤー制御・キャッシュ
 *
 * 依存: Leaflet, renderer.js (RENDERER), chart.js (CHART)
 * ロード順: renderer.js → chart.js → app.js
 */

// ──────────────────────────────────────────────────────────
// 定数
// ──────────────────────────────────────────────────────────
const CONFIG = {
  // データファイル URL（GitHub Pages 同一オリジンから配信）
  // deploy_pages.yml がリリースアセットを ./data/ 以下に展開する
  COG_URL_TEMPLATE:     "./data/{indicator}/{indicator}_{yyyy}_{mm}.tif",
  SUMMARY_URL_TEMPLATE: "./data/summary_{indicator}.json",

  // エリア定義（Leaflet 形式: [[south, west], [north, east]]）
  BBOX:         [[35.594, 139.543], [35.626, 139.582]],
  CENTER:       [35.610, 139.563],
  ZOOM_DEFAULT: 14,

  // 時間軸
  START_YEAR:  2016,
  START_MONTH: 1,

  // 先読みキャッシュ範囲（±N ヶ月）
  PREFETCH_RADIUS: 1,

  // COG オーバーレイ透明度
  COG_OPACITY: 0.75,
};

// ──────────────────────────────────────────────────────────
// 状態管理
// ──────────────────────────────────────────────────────────
const state = {
  map:             null,   // Leaflet Map インスタンス
  indicator:       "ndvi", // 現在選択中の指標
  monthIndex:      0,      // スライダー値（0 = 2016-01）
  totalMonths:     120,    // summary JSON 取得後に更新
  selectedLatLng:  null,   // クリックされた地点（Leaflet LatLng）
  cogOverlay:      null,   // 現在表示中の L.imageOverlay
  clickMarker:     null,   // クリック地点マーカー
  summaryData:     {},     // { ndvi: [...], evi: [...], ndwi: [...], lst: [...] }
};

// キャッシュ: key = "{indicator}_{yyyy}_{mm}", value = Promise<CogResult|null>
const cogCache = new Map();

// ──────────────────────────────────────────────────────────
// URL ヘルパー
// ──────────────────────────────────────────────────────────
function monthIndexToYYYYMM(index) {
  const totalMonths = CONFIG.START_YEAR * 12 + (CONFIG.START_MONTH - 1) + index;
  const year  = Math.floor(totalMonths / 12);
  const month = (totalMonths % 12) + 1;
  return {
    year,
    month,
    yyyy: String(year),
    mm:   String(month).padStart(2, "0"),
  };
}

function buildCogUrl(indicator, index) {
  const { yyyy, mm } = monthIndexToYYYYMM(index);
  // /g フラグで全出現箇所を置換（テンプレート内に {indicator}/{yyyy} が複数回ある）
  return CONFIG.COG_URL_TEMPLATE
    .replace(/\{indicator\}/g, indicator)
    .replace(/\{yyyy\}/g,      yyyy)
    .replace(/\{mm\}/g,        mm);
}

function buildSummaryUrl(indicator) {
  return CONFIG.SUMMARY_URL_TEMPLATE
    .replace(/\{indicator\}/g, indicator);
}

// ──────────────────────────────────────────────────────────
// キャッシュ付き COG フェッチ
// ──────────────────────────────────────────────────────────
function getCogCacheKey(indicator, index) {
  const { yyyy, mm } = monthIndexToYYYYMM(index);
  return `${indicator}_${yyyy}_${mm}`;
}

async function fetchAndRenderCog(indicator, index) {
  const key = getCogCacheKey(indicator, index);
  if (!cogCache.has(key)) {
    // Promise をキャッシュに登録（重複フェッチ防止）
    cogCache.set(key, (async () => {
      const url = buildCogUrl(indicator, index);
      return await RENDERER.loadAndRender(url, indicator);
    })());
  }
  return cogCache.get(key);
}

// ──────────────────────────────────────────────────────────
// ステータス表示
// ──────────────────────────────────────────────────────────
function showStatus(message) {
  let el = document.getElementById("map-status");
  if (!el) {
    el = document.createElement("div");
    el.id = "map-status";
    document.getElementById("map").appendChild(el);
  }
  el.textContent    = message;
  el.style.display  = "block";
}

function hideStatus() {
  const el = document.getElementById("map-status");
  if (el) el.style.display = "none";
}

// ──────────────────────────────────────────────────────────
// 月ラベル更新
// ──────────────────────────────────────────────────────────
function updateMonthLabel() {
  const { yyyy, mm } = monthIndexToYYYYMM(state.monthIndex);
  document.getElementById("month-label").textContent = `${yyyy}-${mm}`;
}

// ──────────────────────────────────────────────────────────
// 地図初期化
// ──────────────────────────────────────────────────────────
function initMap() {
  state.map = L.map("map", {
    center:      CONFIG.CENTER,
    zoom:        CONFIG.ZOOM_DEFAULT,
    zoomControl: true,
  });

  // ベースマップ：OpenStreetMap
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> contributors",
    maxZoom: 18,
  }).addTo(state.map);

  // エリア境界（薄いアウトラインのみ）
  L.rectangle(CONFIG.BBOX, {
    color:     "#4fc3f7",
    weight:    1,
    fill:      false,
    dashArray: "4",
  }).addTo(state.map);

  state.map.on("click", onMapClick);
}

// ──────────────────────────────────────────────────────────
// Summary JSON の一括読み込み
// ──────────────────────────────────────────────────────────
async function loadAllSummaries() {
  const indicators = ["ndvi", "evi", "ndwi", "lst"];
  await Promise.all(indicators.map(async (ind) => {
    const url = buildSummaryUrl(ind);
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      state.summaryData[ind] = await res.json();
    } catch (e) {
      console.warn(`[app] summary fetch failed for ${ind}:`, e);
      state.summaryData[ind] = [];
    }
  }));

  // totalMonths を NDVI summary の行数から確定（欠損が多い場合は他指標で補完）
  const maxLen = Math.max(
    ...indicators.map(ind => state.summaryData[ind].length)
  );
  state.totalMonths = maxLen > 0 ? maxLen : 120;

  const slider = document.getElementById("month-slider");
  slider.max        = state.totalMonths - 1;
  state.monthIndex  = state.totalMonths - 1;  // 最新月を初期表示
  slider.value      = state.monthIndex;
}

// ──────────────────────────────────────────────────────────
// COG レンダリング・地図オーバーレイ更新
// ──────────────────────────────────────────────────────────
async function renderCurrentMonth() {
  showStatus("読み込み中...");

  const result = await fetchAndRenderCog(state.indicator, state.monthIndex);

  // 既存オーバーレイを削除
  if (state.cogOverlay) {
    state.map.removeLayer(state.cogOverlay);
    state.cogOverlay = null;
  }

  if (result === null) {
    showStatus("欠損月：データがありません");
    return;
  }

  hideStatus();

  // Canvas の DataURL を L.imageOverlay で地図に重畳
  state.cogOverlay = L.imageOverlay(
    result.canvas.toDataURL(),
    CONFIG.BBOX,
    { opacity: CONFIG.COG_OPACITY, interactive: false }
  ).addTo(state.map);

  prefetchAdjacentMonths();
}

// ──────────────────────────────────────────────────────────
// 先読みキャッシュ
// ──────────────────────────────────────────────────────────
function prefetchAdjacentMonths() {
  const radius = CONFIG.PREFETCH_RADIUS;
  for (let delta = -radius; delta <= radius; delta++) {
    if (delta === 0) continue;
    const idx = state.monthIndex + delta;
    if (idx >= 0 && idx < state.totalMonths) {
      fetchAndRenderCog(state.indicator, idx).catch(() => {});
    }
  }
}

// ──────────────────────────────────────────────────────────
// グラフ更新
// ──────────────────────────────────────────────────────────
function updateChart() {
  if (!state.selectedLatLng) return;
  const rows = state.summaryData[state.indicator];
  if (!rows || rows.length === 0) return;

  CHART.update(state.indicator, rows, state.monthIndex);
}

// ──────────────────────────────────────────────────────────
// 地図クリック
// ──────────────────────────────────────────────────────────
function onMapClick(e) {
  state.selectedLatLng = e.latlng;

  // クリックマーカーを更新
  if (state.clickMarker) state.map.removeLayer(state.clickMarker);
  state.clickMarker = L.circleMarker(e.latlng, {
    radius:      5,
    color:       "#fff",
    weight:      2,
    fillColor:   "#333",
    fillOpacity: 1,
  }).addTo(state.map);

  updateChart();
}

// ──────────────────────────────────────────────────────────
// コントロール初期化
// ──────────────────────────────────────────────────────────
function initControls() {
  // 指標セレクター（ラジオボタン）
  document.querySelectorAll('input[name="indicator"]').forEach((radio) => {
    radio.addEventListener("change", (e) => {
      state.indicator = e.target.value;
      cogCache.clear();           // 指標変更時はキャッシュを全クリア
      renderCurrentMonth();
      if (state.selectedLatLng) updateChart();
    });
  });

  const slider = document.getElementById("month-slider");

  // 月スライダー
  slider.addEventListener("input", () => {
    state.monthIndex = parseInt(slider.value, 10);
    updateMonthLabel();
    renderCurrentMonth();
    if (state.selectedLatLng) updateChart();
  });

  // ◀ ボタン
  document.getElementById("btn-prev").addEventListener("click", () => {
    if (state.monthIndex > 0) {
      state.monthIndex--;
      slider.value = state.monthIndex;
      updateMonthLabel();
      renderCurrentMonth();
      if (state.selectedLatLng) updateChart();
    }
  });

  // ▶ ボタン
  document.getElementById("btn-next").addEventListener("click", () => {
    if (state.monthIndex < state.totalMonths - 1) {
      state.monthIndex++;
      slider.value = state.monthIndex;
      updateMonthLabel();
      renderCurrentMonth();
      if (state.selectedLatLng) updateChart();
    }
  });

  // ベースマップ モノクロ切替（デフォルト: モノクロ ON）
  const mapEl   = document.getElementById("map");
  const btnBase = document.getElementById("btn-basemap");
  mapEl.classList.add("basemap-grayscale");   // 初期状態: モノクロ
  btnBase.addEventListener("click", () => {
    const isGray = mapEl.classList.toggle("basemap-grayscale");
    btnBase.classList.toggle("active", isGray);
    btnBase.textContent = isGray ? "モノクロ" : "カラー";
  });

  // 免責事項トグル
  document.querySelector(".disclaimer-link").addEventListener("click", (e) => {
    e.preventDefault();
    document.getElementById("disclaimer").classList.toggle("visible");
  });
}

// ──────────────────────────────────────────────────────────
// エントリーポイント
// ──────────────────────────────────────────────────────────
async function initApp() {
  initMap();
  await loadAllSummaries();
  initControls();
  updateMonthLabel();
  renderCurrentMonth();
}

document.addEventListener("DOMContentLoaded", initApp);
