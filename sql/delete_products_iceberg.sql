DELETE FROM products_iceberg
WHERE __dlismerged = false
AND __dlcapturedat > timestamp '2024-05-12 00:00:00 UTC';