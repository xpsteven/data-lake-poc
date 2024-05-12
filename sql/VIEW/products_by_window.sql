CREATE OR REPLACE VIEW "products_by_window" AS 
WITH tmp AS (
   SELECT *,
   ROW_NUMBER() OVER (PARTITION BY id ORDER BY __dlismerged ASC, __dlcapturedat DESC) rn
   FROM products_iceberg
) 
SELECT *
FROM tmp
WHERE (rn = 1)