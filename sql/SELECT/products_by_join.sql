-- 欄位選擇會影響輸入資料大小
-- WHERE 條件影響不大
-- LIMIT 越小輸入資料越小
-- WHERE 條件可以穿透到 SOURCE 任務
SELECT id,
	title,
	code,
	price,
-- 	desc,
	__dlcapturedat
FROM products_by_join
WHERE price < 100
ORDER BY id DESC
LIMIT 100