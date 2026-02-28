/**
 * renderer.js
 * geotiff.js を使って COG を Canvas に描画するモジュール。
 * app.js から RENDERER.loadAndRender(url, indicator) として呼ばれる。
 */

const RENDERER = (() => {

  // 指標ごとの固定値域とカラーパレット
  const COLORMAPS = {
    ndvi: {
      min: -0.2,
      max:  0.9,
      // 低→高：赤 → 黄緑 → 濃緑（RdYlGn 風）
      palette: [
        [165,   0,  38],
        [215,  48,  39],
        [244, 109,  67],
        [253, 174,  97],
        [254, 224, 139],
        [255, 255, 191],
        [217, 239, 139],
        [166, 217, 106],
        [102, 189,  99],
        [ 26, 152,  80],
        [  0, 104,  55],
      ],
    },
    evi: {
      min: -0.1,
      max:  0.8,
      // NDVI と同系統
      palette: [
        [165,   0,  38],
        [215,  48,  39],
        [244, 109,  67],
        [253, 174,  97],
        [254, 224, 139],
        [255, 255, 191],
        [217, 239, 139],
        [166, 217, 106],
        [102, 189,  99],
        [ 26, 152,  80],
        [  0, 104,  55],
      ],
    },
    ndwi: {
      min: -0.5,
      max:  0.5,
      // 低（乾燥）→ 高（水分）：茶 → 白 → 青緑
      palette: [
        [140,  81,  10],
        [191, 129,  45],
        [223, 194, 125],
        [246, 232, 195],
        [245, 245, 245],
        [199, 234, 229],
        [128, 205, 193],
        [ 53, 151, 143],
        [  1, 102,  94],
        [  0,  60,  48],
      ],
    },
    lst: {
      min: 10,
      max: 45,
      // 低温→高温：青 → 白 → 赤（感熱的）
      palette: [
        [ 49,  54, 149],
        [ 69, 117, 180],
        [116, 173, 209],
        [171, 217, 233],
        [224, 243, 248],
        [255, 255, 191],
        [254, 224, 144],
        [253, 174,  97],
        [244, 109,  67],
        [215,  48,  39],
        [165,   0,  38],
      ],
    },
  };

  /**
   * 値 → RGB 変換（リニア補間）
   * @param {number} value
   * @param {{min: number, max: number, palette: number[][]}} colormap
   * @returns {number[]|null} [r, g, b] or null（NaN/Inf → 透明）
   */
  function valueToRGB(value, colormap) {
    if (!isFinite(value)) return null;

    const { min, max, palette } = colormap;
    const t = Math.max(0, Math.min(1, (value - min) / (max - min)));
    const n = palette.length - 1;
    const i = Math.floor(t * n);
    const f = t * n - i;

    const c0 = palette[Math.min(i,     n)];
    const c1 = palette[Math.min(i + 1, n)];

    return [
      Math.round(c0[0] + (c1[0] - c0[0]) * f),
      Math.round(c0[1] + (c1[1] - c0[1]) * f),
      Math.round(c0[2] + (c1[2] - c0[2]) * f),
    ];
  }

  /**
   * COG を URL から取得し、Canvas に描画して返す。
   * @param {string} url
   * @param {string} indicator  "ndvi" | "evi" | "ndwi" | "lst"
   * @returns {Promise<{canvas: HTMLCanvasElement, data: Float32Array, width: number, height: number}|null>}
   *          欠損月（404等）は null を返す。
   */
  async function loadAndRender(url, indicator) {
    let tiff, image, data;
    try {
      tiff  = await GeoTIFF.fromUrl(url, { allowHttpErrors: false });
      image = await tiff.getImage();
      // COG は Float32 / バンド 1 枚
      const rasters = await image.readRasters({ interleave: false });
      data = rasters[0];  // Float32Array
    } catch (e) {
      // 欠損月（404 など）は null を返す
      console.warn(`[renderer] fetch failed: ${url}`, e);
      return null;
    }

    const width     = image.getWidth();
    const height    = image.getHeight();
    const colormap  = COLORMAPS[indicator];

    const canvas    = document.createElement("canvas");
    canvas.width    = width;
    canvas.height   = height;
    const ctx       = canvas.getContext("2d");
    const imgData   = ctx.createImageData(width, height);

    for (let i = 0; i < width * height; i++) {
      const rgb = valueToRGB(data[i], colormap);
      if (rgb === null) {
        // NaN/Inf → 完全透明
        imgData.data[i * 4 + 3] = 0;
      } else {
        imgData.data[i * 4 + 0] = rgb[0];
        imgData.data[i * 4 + 1] = rgb[1];
        imgData.data[i * 4 + 2] = rgb[2];
        imgData.data[i * 4 + 3] = 200;   // alpha（0.78）
      }
    }

    ctx.putImageData(imgData, 0, 0);
    return { canvas, data, width, height };
  }

  /**
   * 指標の colormap 定義を返す（凡例用）
   * @param {string} indicator
   * @returns {{ min: number, max: number, palette: number[][] }}
   */
  function getColormap(indicator) {
    return COLORMAPS[indicator];
  }

  /**
   * 凡例用グラデーションバー Canvas を生成して返す。
   * 上 → 下：max → min（上が高値）
   * @param {string} indicator
   * @param {number} height  バーのピクセル高さ（デフォルト 160）
   * @returns {HTMLCanvasElement}
   */
  function buildLegendCanvas(indicator, height = 160) {
    const colormap = COLORMAPS[indicator];
    const canvas = document.createElement("canvas");
    canvas.width  = 16;
    canvas.height = height;
    const ctx = canvas.getContext("2d");

    for (let y = 0; y < height; y++) {
      const t = 1 - y / (height - 1);  // 1.0（上・max）→ 0.0（下・min）
      const value = colormap.min + t * (colormap.max - colormap.min);
      const rgb = valueToRGB(value, colormap);
      if (rgb) {
        ctx.fillStyle = `rgb(${rgb[0]},${rgb[1]},${rgb[2]})`;
        ctx.fillRect(0, y, 16, 1);
      }
    }
    return canvas;
  }

  return { loadAndRender, getColormap, buildLegendCanvas };
})();
