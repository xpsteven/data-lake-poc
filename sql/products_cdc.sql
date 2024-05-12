-- 建立 products_cdc 外部表
-- 用 JSON LINES 格式儲存，檔案用 gzip 壓縮
-- note 2024/05/12: __dlcapturedat 有底線開頭，用反引號包起來
-- note 2024/05/12: hive 的 jsonserde 才支援 timestamp.formats，openx 的沒有
CREATE EXTERNAL TABLE products_cdc (
    id int,
    title string,
    code string,
    price double,
    desc string,
    `__dlcapturedat` timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    "timestamp.formats" = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://athena-20240123/tables/default/products_cdc/'
TBLPROPERTIES ('compressionType'='gzip');
