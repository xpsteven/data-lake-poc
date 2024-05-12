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