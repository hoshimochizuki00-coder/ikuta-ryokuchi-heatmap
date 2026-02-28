# 生田緑地 環境ヒートマップ

川崎市・生田緑地を対象に、過去10年分の衛星データ（NDVI・EVI・NDWI・LST）を
月次ヒートマップで可視化するウェブサービスです。

---

## セットアップ

Python 3.11 以上が必要です。

```bash
pip install -r requirements.txt
```

---

## 実行方法

### 単月処理（動作確認用）

```bash
python pipeline/main.py --mode historical --start 2023-07 --end 2023-07
```

### 任意の期間を処理

```bash
python pipeline/main.py --mode historical --start 2016-01 --end 2016-12
```

### 月次更新（当月の前月を処理）

```bash
python pipeline/main.py --mode monthly
```

---

## 出力ファイル

`output/` ディレクトリ（`.gitignore` に追加済み）に生成されます。

```
output/
├── ndvi/ndvi_2023_07.tif      # Cloud Optimized GeoTIFF
├── evi/evi_2023_07.tif
├── ndwi/ndwi_2023_07.tif
├── lst/lst_2023_07.tif
├── summary_ndvi.csv           # 時系列サマリー（全期間）
├── summary_ndvi.json          # フロントエンド用（同内容）
├── ...
└── missing.json               # 欠損記録（空配列 [] ならすべて成功）
```

---

## GitHub Actions 設定

GitHub Releases へのアップロードを有効にするには、リポジトリの以下の設定が必要です。

**Settings > Actions > General > Workflow permissions**
→ **"Read and write permissions"** に変更してください。

ローカル実行時は `GITHUB_REPO` 環境変数を設定しない限り、アップロードはスキップされます。

---

## データソース

- **Sentinel-2 L2A**（NDVI / EVI / NDWI）: Microsoft Planetary Computer
- **Landsat Collection 2 Level-2**（LST）: Microsoft Planetary Computer

いずれも匿名アクセス可能です（API キー不要）。

---

## プロジェクト構成

```
pipeline/      バックエンド処理スクリプト
frontend/      GitHub Pages 配信ファイル（Phase 3 で実装）
docs/          設計ドキュメント
tests/         ユニット・統合テスト
```
