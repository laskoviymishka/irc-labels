-- ClickHouse initialization: connect to UC via IRC (through labels proxy)
-- The DataLakeCatalog engine connects to the Labels Proxy which enriches
-- LoadTableResponse with labels before passing to ClickHouse.

-- Note: This creates the database connection. Tables are auto-discovered.
-- The labels-proxy URL is used so ClickHouse sees the enriched responses.

CREATE DATABASE IF NOT EXISTS healthcare
ENGINE = DataLakeCatalog('http://labels-proxy:8181/api/2.1/unity-catalog/iceberg/v1/unity')
SETTINGS catalog_type = 'rest',
         storage_endpoint = 'http://minio:9000/warehouse';
