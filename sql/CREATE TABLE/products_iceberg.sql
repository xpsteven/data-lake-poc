-- 建立 products_iceberg 表格
-- 每個 id 只會有一筆資料 __dlismerged 為 true
-- 但可能會有多筆 __dlismerged 為 false 的資料，代表剛 insert into 近來的資料列
-- 網路資料 ORC 效能最好
-- 簡單實測 ZSTD 壓縮比最好
-- note 2024/05/12: Athena 只支援小寫的 table 和 column 名稱
-- note 2024/05/12: 有註解 Athena 會執行失敗
CREATE TABLE products_iceberg (
    id int,
    title string,
    code string,
    price double,
    desc string,
    __dlcapturedat timestamp,
    __dlloadedat timestamp,
    __dlismerged boolean
)
PARTITIONED BY (__dlismerged)
LOCATION 's3://athena-20240123/tables/default/products_iceberg/'
TBLPROPERTIES (
    'table_type'='iceberg',
    'write_compression'='ZSTD',
    'format'='orc'
);
