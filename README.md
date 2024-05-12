# Athena Data Lake PoC

## 概念

1. capturer 將資料庫異動 (insert & update) 以 gzip json lines 儲存在 S3
1. Athena 讀取 gzip json lines 儲存到 iceberg table
1. 參考資料的設計是分成兩張 iceberg 表，一張放 history 一張放 fact。這個專案把兩張表合併在一起，用 __dlismerged 欄位區分並當作 partition field

## 表格

1. `sql/CREATE TABLE/products_cdc.sql` 讀取資料庫異動的表格
1. `sql/CREATE TABLE/products_iceberg.sql` 主要 data lake 的表格
1. `sql/append_cdc_to_iceberg.sql` 將資料從 `products_cdc` 複製到 `products_iceberg`
1. `sql/VIEW/products.sql` 近即時的 view
1. `sql/merge_products_iceberg.sql` 合併資料列
1. `sql/delete_products_iceberg.sql` 刪除已合併的資料列

## 程式

1. `gen-test-products-cdc.go` 產生測試資料 