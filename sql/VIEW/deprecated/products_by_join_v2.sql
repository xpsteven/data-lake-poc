CREATE OR REPLACE VIEW "products_merged" AS 
WITH delta AS (
   SELECT *,
   ROW_NUMBER() OVER (PARTITION BY id ORDER BY __dlcapturedat DESC) rn
   FROM products_iceberg
   WHERE __dlismerged = false
),
latest AS (
    SELECT *
    FROM delta
    WHERE rn = 1
),
merged AS (
    SELECT *
    FROM products_iceberg
    WHERE __dlismerged = true
)
SELECT COALESCE(b.id, a.id) id,
    COALESCE(b.title, a.title) title,
    COALESCE(b.code, a.code) code,
    COALESCE(b.price, a.price) price,
    COALESCE(b.desc, a.desc) desc,
    COALESCE(b.__dlcapturedat, a.__dlcapturedat) __dlcapturedat
FROM merged a
FULL OUTER JOIN latest b ON a.id = b.id;