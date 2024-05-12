CREATE TABLE products_delta (
    id int,
    title string,
    code string,
    price double,
    desc string,
    __dlcapturedat timestamp,
    __dlloadedat timestamp
)
LOCATION 's3://athena-20240123/tables/default/products_delta/'
TBLPROPERTIES (
    'table_type'='iceberg',
    'format'='orc',
    'write_compression'='ZSTD'
);
