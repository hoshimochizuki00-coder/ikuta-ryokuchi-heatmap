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
  map:             null,      // Leaflet Map インスタンス
  indicator:       "ndvi",   // 現在選択中の指標
  monthIndex:      0,        // スライダー値（0 = 2016-01）
  totalMonths:     120,      // summary JSON 取得後に更新
  selectedLatLng:  null,     // クリックされた地点（Leaflet LatLng）
  cogOverlay:      null,     // 現在表示中の L.imageOverlay
  clickMarker:     null,     // クリック地点マーカー
  summaryData:     {},       // { ndvi: [...], evi: [...], ndwi: [...], lst: [...] }
  cogMeta:         null,     // 初回 COG ロード時に格納（BBox + 幅高さ）
  chartMode:       "summary", // "summary" | "pixel"
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

  // totalMonths を現在日付から算出（前月まで）
  // summary JSON の行数に依存しないため、データ未配信月もスライダーに表示される
  {
    const now = new Date();
    let endYear  = now.getFullYear();
    let endMonth = now.getMonth(); // 0-based: 0=Jan → このままで「前月の月番号（1-based）」
    if (endMonth === 0) { endYear--; endMonth = 12; }
    state.totalMonths = (endYear - CONFIG.START_YEAR) * 12 + (endMonth - CONFIG.START_MONTH) + 1;
  }

  const slider = document.getElementById("month-slider");
  slider.max        = state.totalMonths - 1;
  state.monthIndex  = state.totalMonths - 1;  // 最新月を初期表示
  slider.value      = state.monthIndex;

  updateMissingIndicator(state.indicator);
}

// ──────────────────────────────────────────────────────────
// COG レンダリング・地図オーバーレイ更新
// ──────────────────────────────────────────────────────────
async function renderCurrentMonth() {
  showStatus("読み込み中...");

  const result = await fetchAndRenderCog(state.indicator, state.monthIndex);

  // 初回 COG ロード時に cogMeta を WGS84 固定で構築（image.getBoundingBox は UTM になる場合があるため CONFIG.BBOX を使用）
  if (!state.cogMeta && result) {
    state.cogMeta = {
      west:   CONFIG.BBOX[0][1],  // [[south, west], [north, east]] → west
      south:  CONFIG.BBOX[0][0],  // south
      east:   CONFIG.BBOX[1][1],  // east
      north:  CONFIG.BBOX[1][0],  // north
      width:  result.width,
      height: result.height,
    };
  }

  // 既存オーバーレイを削除
  if (state.cogOverlay) {
    state.map.removeLayer(state.cogOverlay);
    state.cogOverlay = null;
  }

  if (result === null) {
    showStatus("欠損月：データがありません");
    updateMissingIndicator(state.indicator);
    return;
  }

  hideStatus();
  updateMissingIndicator(state.indicator);

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
// ④ 地点別ピクセル値グラフ
// ──────────────────────────────────────────────────────────

/**
 * 緯度経度から COG のピクセルインデックス（col, row）を算出する。
 * COG の空間参照が WGS84（EPSG:4326）・原点が左上であることを前提とする。
 * @param {number} lat
 * @param {number} lng
 * @param {{ west, south, east, north, width, height }} cogMeta
 * @returns {{ col: number, row: number } | null}  範囲外は null
 */
function latLngToPixel(lat, lng, cogMeta) {
  const { west, south, east, north, width, height } = cogMeta;
  if (lng < west || lng > east || lat < south || lat > north) return null;
  const col = Math.floor(((lng - west) / (east - west)) * width);
  const row = Math.floor(((north - lat) / (north - south)) * height);
  return {
    col: Math.max(0, Math.min(col, width - 1)),
    row: Math.max(0, Math.min(row, height - 1)),
  };
}

/**
 * CogResult から指定ピクセルの値を取得する。
 * @param {{ data: Float32Array, width: number }} cogResult
 * @param {number} col
 * @param {number} row
 * @returns {number | null}  NaN/Infinity は null に変換
 */
function getPixelValue(cogResult, col, row) {
  const idx = row * cogResult.width + col;
  const v = cogResult.data[idx];
  if (!isFinite(v) || isNaN(v)) return null;
  return v;
}

/**
 * クリック地点の全月ピクセル値を収集し、CHART.updatePixel() に渡す。
 * 未フェッチ月はフェッチしながら進捗を showStatus() で表示する。
 */
async function collectPixelTimeseries(indicator, latlng) {
  if (!state.cogMeta) {
    showStatus("COGメタデータ未取得。地図上のデータ読み込み後に再試行してください。");
    return;
  }

  const pixel = latLngToPixel(latlng.lat, latlng.lng, state.cogMeta);
  if (!pixel) {
    showStatus("選択地点がエリア外です。");
    return;
  }

  const values = new Array(state.totalMonths).fill(null);
  let loaded = 0;

  showStatus(`地点データ取得中… 0 / ${state.totalMonths}`);

  const CONCURRENCY = 8;
  const queue = Array.from({ length: state.totalMonths }, (_, i) => i);

  async function processOne(monthIndex) {
    try {
      const result = await fetchAndRenderCog(indicator, monthIndex);
      if (result) {
        values[monthIndex] = getPixelValue(result, pixel.col, pixel.row);
      }
    } catch {
      // 欠損月は null のまま
    }
    loaded++;
    showStatus(`地点データ取得中… ${loaded} / ${state.totalMonths}`);
  }

  while (queue.length > 0) {
    const batch = queue.splice(0, CONCURRENCY);
    await Promise.all(batch.map(processOne));
  }

  hideStatus();

  CHART.updatePixel(indicator, values, state.monthIndex, {
    lat: latlng.lat.toFixed(5),
    lng: latlng.lng.toFixed(5),
  });
}

// ──────────────────────────────────────────────────────────
// ⑤ COG キャッシュ無効化（値域変更時）
// ──────────────────────────────────────────────────────────

/**
 * COG キャッシュの Canvas を無効化して現在の値域で再描画する。
 * Float32Array（data）は保持するためネットワーク再フェッチは発生しない。
 */
function invalidateCogCache() {
  const entries = [...cogCache.entries()]; // イテレーション中の変更を回避するためスナップショット
  for (const [key, promise] of entries) {
    const indicator = key.split("_")[0]; // "{indicator}_{yyyy}_{mm}" の先頭部分
    cogCache.set(key, promise.then((result) => {
      if (result === null) return null;
      return RENDERER.rerender({ ...result, indicator });
    }));
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

  if (state.chartMode === "pixel") {
    collectPixelTimeseries(state.indicator, e.latlng);
  } else {
    updateChart();
  }
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
      updateLegend(state.indicator);
      updateMissingIndicator(state.indicator);
      renderCurrentMonth();
      if (state.selectedLatLng) updateChart();
    });
  });

  const slider = document.getElementById("month-slider");

  // 月スライダー
  slider.addEventListener("input", () => {
    state.monthIndex = parseInt(slider.value, 10);
    updateMonthLabel();
    updateMissingIndicator(state.indicator);
    renderCurrentMonth();
    if (state.selectedLatLng) updateChart();
  });

  // ◀ ボタン
  document.getElementById("btn-prev").addEventListener("click", () => {
    if (state.monthIndex > 0) {
      state.monthIndex--;
      slider.value = state.monthIndex;
      updateMonthLabel();
      updateMissingIndicator(state.indicator);
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
      updateMissingIndicator(state.indicator);
      renderCurrentMonth();
      if (state.selectedLatLng) updateChart();
    }
  });

  // グラフモード切り替え（エリア平均 ↔ 地点別）
  document.querySelectorAll('input[name="chart-mode"]').forEach((radio) => {
    radio.addEventListener("change", (e) => {
      state.chartMode = e.target.value;
      if (state.chartMode === "summary") {
        updateChart();
      } else if (state.selectedLatLng) {
        collectPixelTimeseries(state.indicator, state.selectedLatLng);
      } else {
        CHART.clear();
      }
    });
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
// ① カラースケール凡例
// ──────────────────────────────────────────────────────────
let _legendControl = null;

function initLegend() {
  _legendControl = L.control({ position: "bottomright" });

  _legendControl.onAdd = function () {
    const div = L.DomUtil.create("div", "map-legend");
    div.id = "map-legend";
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);
    return div;
  };

  _legendControl.addTo(state.map);
  updateLegend(state.indicator);
}

function updateLegend(indicator) {
  const container = document.getElementById("map-legend");
  if (!container) return;

  const colormap = RENDERER.getColormap(indicator);
  const legendCanvas = RENDERER.buildLegendCanvas(indicator, 140);

  const UNITS = { ndvi: "", evi: "", ndwi: "", lst: "°C" };
  const unit = UNITS[indicator] ?? "";
  const fmt = (v) => Number.isInteger(v) ? String(v) : v.toFixed(1);

  container.innerHTML = "";

  // ── 最大値入力 ──
  const maxWrap = document.createElement("div");
  maxWrap.className = "legend-range-wrap";
  const maxInput = document.createElement("input");
  maxInput.type = "number";
  maxInput.className = "legend-range-input";
  maxInput.value = fmt(colormap.max);
  maxInput.step = "any";
  maxWrap.appendChild(maxInput);
  if (unit) {
    const maxUnit = document.createElement("span");
    maxUnit.className = "legend-unit";
    maxUnit.textContent = unit;
    maxWrap.appendChild(maxUnit);
  }
  container.appendChild(maxWrap);

  // ── グラデーションバー ──
  container.appendChild(legendCanvas);

  // ── 最小値入力 ──
  const minWrap = document.createElement("div");
  minWrap.className = "legend-range-wrap";
  const minInput = document.createElement("input");
  minInput.type = "number";
  minInput.className = "legend-range-input";
  minInput.value = fmt(colormap.min);
  minInput.step = "any";
  minWrap.appendChild(minInput);
  if (unit) {
    const minUnit = document.createElement("span");
    minUnit.className = "legend-unit";
    minUnit.textContent = unit;
    minWrap.appendChild(minUnit);
  }
  container.appendChild(minWrap);

  // ── リセットボタン ──
  const resetBtn = document.createElement("button");
  resetBtn.className = "legend-reset-btn";
  resetBtn.textContent = "リセット";
  resetBtn.title = "デフォルト値域に戻す";
  container.appendChild(resetBtn);

  // ── エラーラベル ──
  const errorLabel = document.createElement("div");
  errorLabel.className = "legend-error";
  container.appendChild(errorLabel);

  // ── イベント登録 ──
  function applyRange() {
    const newMin = parseFloat(minInput.value);
    const newMax = parseFloat(maxInput.value);
    if (isNaN(newMin) || isNaN(newMax)) {
      errorLabel.textContent = "数値を入力してください";
      return;
    }
    if (newMin >= newMax) {
      errorLabel.textContent = "min < max にしてください";
      return;
    }
    errorLabel.textContent = "";
    RENDERER.setRange(indicator, newMin, newMax);
    invalidateCogCache();
    updateLegend(indicator);
    renderCurrentMonth();
  }

  minInput.addEventListener("change", applyRange);
  maxInput.addEventListener("change", applyRange);
  minInput.addEventListener("keydown", (e) => { if (e.key === "Enter") applyRange(); });
  maxInput.addEventListener("keydown", (e) => { if (e.key === "Enter") applyRange(); });

  resetBtn.addEventListener("click", () => {
    RENDERER.resetRange(indicator);
    const def = RENDERER.getDefaultRange(indicator);
    minInput.value = fmt(def.min);
    maxInput.value = fmt(def.max);
    errorLabel.textContent = "";
    invalidateCogCache();
    updateLegend(indicator);
    renderCurrentMonth();
  });
}

// ──────────────────────────────────────────────────────────
// ③ 欠損月インジケーター
// ──────────────────────────────────────────────────────────
function updateMissingIndicator(indicator) {
  const canvas = document.getElementById("missing-indicator");
  if (!canvas) return;

  const rows = state.summaryData[indicator];
  if (!rows || rows.length === 0) return;

  const wrapper = document.getElementById("slider-wrapper");
  canvas.width = wrapper.clientWidth;

  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  const THUMB_MARGIN = 8;
  const trackWidth = canvas.width - THUMB_MARGIN * 2;
  const totalSlots = state.totalMonths;  // totalMonths はスライダー全体の月数

  rows.forEach((row) => {
    const isMissing = row.valid_ratio === null || row.valid_ratio === 0;
    if (!isMissing) return;

    // year/month から月インデックスを算出（スライダーと同じ基準）
    const rowIndex = (row.year - CONFIG.START_YEAR) * 12 + (row.month - CONFIG.START_MONTH);
    if (rowIndex < 0 || rowIndex >= totalSlots) return;

    const x = THUMB_MARGIN + (rowIndex / (totalSlots - 1)) * trackWidth;

    ctx.beginPath();
    if (rowIndex === state.monthIndex) {
      ctx.fillStyle = "#ff5252";
      ctx.arc(x, 3, 3, 0, Math.PI * 2);
    } else {
      ctx.fillStyle = "#e57373";
      ctx.arc(x, 3, 2, 0, Math.PI * 2);
    }
    ctx.fill();
  });
}

// ──────────────────────────────────────────────────────────
// エントリーポイント
// ──────────────────────────────────────────────────────────
async function initApp() {
  initMap();
  initLegend();
  await loadAllSummaries();
  initControls();
  updateMonthLabel();
  renderCurrentMonth();
}

window.addEventListener("resize", () => {
  updateMissingIndicator(state.indicator);
});

document.addEventListener("DOMContentLoaded", initApp);
