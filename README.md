# Athena Data Lake PoC

## 概念

1. capturer 將資料庫異動 (insert & update) 以 gzip json lines 儲存在 S3
1. Athena 讀取 gzip json lines 儲存到 iceberg table

## 表格

1. `sql/CREATE TABLE/products_cdc.sql` 讀取資料庫異動的表格
1. `sql/CREATE TABLE/products_iceberg.sql` 主要 data lake 的表格
1. `sql/VIEW/products.sql` 近即時 view

## 程式

1. `gen-test-products-cdc.go` 產生測試資料，模擬 capturer
1. `sql/append_cdc_to_iceberg.sql` 將資料從 `products_cdc` 複製到 `products_iceberg`，loader 的核心邏輯
1. `sql/merge_products_iceberg.sql` 合併資料列，compactor 的核心邏輯
1. `sql/delete_products_iceberg.sql` 刪除已合併的資料列，compactor

## 欄位

name | 說明
-- | --
__dlcapturedat | 擷取時間
__dlloadedat | 載入到 data lake 的時間
__dlismerged | 是否為合併的資料列，一個 id 只會有一筆 __dlismerged 為 true，可能有多筆 __dlismerged 為 false

