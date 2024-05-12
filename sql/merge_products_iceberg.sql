-- 合併 __dlismerged 為 false 的資料列到 __dlismerged 為 true 的資料列
-- 沒有已合併資料列就用 INSERT INTO
-- MERGE INTO 後已合併的資料列需要刪除
-- 建議作法
-- 1. 執行 MERGE INTO 前快照 __dlismerged 為 false 資料列中的 MAX(__dlcapturedat)
-- 2. 執行 MERGE INTO
-- 3. 刪除 __dlismerged 為 false 且 __dlcapturedat 小於步驟一的資料列
MERGE INTO products_iceberg AS t
USING (
    WITH tmp AS (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY __dlcapturedat DESC) __dlRowNumber
        FROM products_iceberg
        WHERE __dlismerged = false
    )
    SELECT *
    FROM tmp
    WHERE __dlRowNumber = 1
) AS c ON t.id = c.id AND t.__dlismerged = true
WHEN MATCHED THEN
    UPDATE SET
        title = c.title,
        code = c.code,
        price = c.price,
        desc = c.desc,
        __dlcapturedat = c.__dlcapturedat,
        __dlloadedat = CURRENT_TIMESTAMP,
        __dlismerged = true
WHEN NOT MATCHED THEN
    INSERT (id, title, code, price, desc, __dlcapturedat, __dlloadedat, __dlismerged)
    VALUES (c.id, c.title, c.code, c.price, c.desc, c.__dlcapturedat, CURRENT_TIMESTAMP, true);
