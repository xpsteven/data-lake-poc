-- 將 CDC 資料複製到 Iceberg 表格
-- Athena 無法對 External 表格做資料刪除
-- 必須另外寫 code 刪除 S3 上的檔案
-- 建議作法：
-- 1. 執行 INSERT INTO 前快照 products_cdc 對應的 S3 目錄的檔案清單
-- 2. 執行 INSERT INTO
-- 3. 刪除步驟一的檔案清單
INSERT INTO products_iceberg
SELECT id,
	title,
	code,
	price,
	desc,
	__dlcapturedat,
	CURRENT_TIMESTAMP AS __dlloadedat,
	false AS __dlismerged
FROM products_cdc