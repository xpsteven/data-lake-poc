-- 建立 products_iceberg 表格
-- 每個 id 只會有一筆資料 __dlismerged 為 true
-- 但可能會有多筆 __dlismerged 為 false 的資料，代表剛 insert into 近來的資料列
-- note 2024/05/12: Athena 只支援小寫的 table 和 column 名稱
-- note 2024/05/12: 有註解 Athena 會執行失敗
CREATE TABLE IF NOT EXISTS products_iceberg (
    id int,
    title string,
    code string,
    price double,
    desc string,
    -- 擷取時間
    __dlcapturedat timestamp,
    -- 載入時間
    __dlloadedat timestamp,
    -- 是否合併
    __dlismerged boolean
)
LOCATION 's3://athena-20240123/tables/default/products_iceberg/'
TBLPROPERTIES (
    'table_type'='iceberg',
    'write_compression'='zstd',
    'compression_level'='10'
);
