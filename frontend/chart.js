/**
 * chart.js
 * Chart.js ラッパー。app.js から CHART.update() / CHART.clear() として呼ばれる。
 */

const CHART = (() => {

  // 指標ごとのラベル・単位・色
  const META = {
    ndvi: { label: "NDVI（植生活性度）", unit: "",    color: "#2ca02c" },
    evi:  { label: "EVI（強化植生指数）", unit: "",    color: "#1f77b4" },
    ndwi: { label: "NDWI（水分量）",     unit: "",    color: "#17becf" },
    lst:  { label: "LST（地表面温度）",  unit: "°C",  color: "#d62728" },
  };

  let _chartInstance = null;   // Chart.js インスタンス（シングルトン）

  /**
   * 時系列グラフを更新（または初回作成）する。
   * @param {string} indicator
   * @param {Array<{year: number, month: number, mean: number, max: number, min: number}>} summaryRows
   * @param {number} selectedMonthIndex  現在表示月（縦線表示用）
   */
  function update(indicator, summaryRows, selectedMonthIndex) {
    const meta        = META[indicator];
    const canvas      = document.getElementById("timeseries-chart");
    const placeholder = document.getElementById("chart-placeholder");

    canvas.style.display      = "block";
    placeholder.style.display = "none";
    document.getElementById("chart-note").style.display = "block";

    const labels = summaryRows.map(r => `${r.year}-${String(r.month).padStart(2, "0")}`);
    const means  = summaryRows.map(r => (r.mean  !== undefined && r.mean  !== null) ? r.mean  : null);
    const maxs   = summaryRows.map(r => (r.max   !== undefined && r.max   !== null) ? r.max   : null);
    const mins   = summaryRows.map(r => (r.min   !== undefined && r.min   !== null) ? r.min   : null);

    const datasets = [
      {
        label:           `${meta.label} 平均`,
        data:            means,
        borderColor:     meta.color,
        backgroundColor: "transparent",
        borderWidth:     1.5,
        pointRadius:     2,
        spanGaps:        false,
      },
      {
        label:           "最大値",
        data:            maxs,
        borderColor:     meta.color,
        borderDash:      [4, 4],
        backgroundColor: "transparent",
        borderWidth:     1,
        pointRadius:     0,
        spanGaps:        false,
      },
      {
        label:           "最小値",
        data:            mins,
        borderColor:     meta.color,
        borderDash:      [2, 4],
        backgroundColor: "transparent",
        borderWidth:     1,
        pointRadius:     0,
        spanGaps:        false,
      },
    ];

    const selectedLabel = labels[selectedMonthIndex] ?? null;

    // 現在表示月の縦線を描画するインラインプラグイン
    const currentLinePlugin = {
      id: "currentLinePlugin",
      afterDraw(chart) {
        const label = chart.options.plugins.currentLine?.label;
        if (!label) return;
        const idx = chart.data.labels.indexOf(label);
        if (idx < 0) return;
        const metaDs = chart.getDatasetMeta(0);
        if (!metaDs.data[idx]) return;
        const x = metaDs.data[idx].x;
        const { top, bottom } = chart.chartArea;
        const ctx = chart.ctx;
        ctx.save();
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.strokeStyle = "rgba(255, 255, 255, 0.6)";
        ctx.lineWidth = 1.5;
        ctx.setLineDash([4, 3]);
        ctx.stroke();
        ctx.restore();
      },
    };

    if (_chartInstance) {
      // データ更新（チャート再生成より高速）
      _chartInstance.data.labels   = labels;
      _chartInstance.data.datasets = datasets;
      _chartInstance.options.plugins.currentLine = { label: selectedLabel };
      _chartInstance.update("none");
    } else {
      _chartInstance = new Chart(canvas, {
        type: "line",
        data: { labels, datasets },
        options: {
          responsive:          true,
          maintainAspectRatio: false,
          animation:           false,
          color:               "#ccc",
          plugins: {
            legend: {
              position: "top",
              labels:   { boxWidth: 12, font: { size: 11 }, color: "#ccc" },
            },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const val = ctx.raw;
                  if (val === null) return `${ctx.dataset.label}: 欠損`;
                  return `${ctx.dataset.label}: ${val.toFixed(3)}${meta.unit}`;
                },
              },
            },
            currentLine: { label: selectedLabel },
          },
          scales: {
            x: {
              ticks: {
                maxTicksLimit: 24,
                font:          { size: 10 },
                color:         "#aaa",
              },
              grid: { color: "rgba(255,255,255,0.05)" },
            },
            y: {
              title: {
                display: true,
                text:    meta.unit || meta.label,
                font:    { size: 11 },
                color:   "#aaa",
              },
              ticks: { color: "#aaa", font: { size: 10 } },
              grid:  { color: "rgba(255,255,255,0.05)" },
            },
          },
        },
        plugins: [currentLinePlugin],
      });
    }
  }

  /**
   * グラフを破棄してプレースホルダーを表示する。
   */
  function clear() {
    if (_chartInstance) {
      _chartInstance.destroy();
      _chartInstance = null;
    }
    document.getElementById("timeseries-chart").style.display = "none";
    document.getElementById("chart-placeholder").style.display = "block";
    document.getElementById("chart-note").style.display = "none";
  }

  return { update, clear };
})();
