-- 合併 __dlismerged 為 false 的記錄到 __dlismerged 為 true 的記錄
-- 沒有已合併記錄就用 INSERT INTO
-- MERGE INTO 後已合併的記錄需要刪除
-- 建議作法
-- 1. 執行 MERGE INTO 前快照 __dlismerged 為 false 記錄中的 MAX(__dlcapturedat)
-- 2. 執行 MERGE INTO
-- 3. 刪除 __dlismerged 為 false 且 __dlcapturedat 小於步驟一的記錄
MERGE INTO products_iceberg AS target
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
) AS changes ON target.id = changes.id AND target.__dlismerged = true
WHEN MATCHED THEN
    UPDATE SET
        title = changes.title,
        code = changes.code,
        price = changes.price,
        desc = changes.desc,
        __dlcapturedat = changes.__dlcapturedat,
        __dlismerged = true
WHEN NOT MATCHED THEN
    INSERT (id, title, code, price, desc, __dlcapturedat, __dlismerged)
    VALUES (changes.id, changes.title, changes.code, changes.price, changes.desc, changes.__dlcapturedat, true);
