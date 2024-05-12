-- products_by_join 確保每個 id 都只有最新的那列
-- 只有 JOIN 法可以穿透 WHERE 條件下去
-- 並啟用 dynamic filter
CREATE OR REPLACE VIEW "products" AS
WITH tmp AS (
    SELECT id,
        MAX(__dlcapturedat) latest_capturedat,
        BOOL_AND(__dlismerged) __dlismerged
    FROM products_iceberg
    GROUP BY id
)
SELECT a.*
FROM (
    products_iceberg a
    INNER JOIN tmp b ON (
        (a.id = b.id AND a.__dlcapturedat = b.latest_capturedat AND a.__dlismerged = b.__dlismerged)
    )
)