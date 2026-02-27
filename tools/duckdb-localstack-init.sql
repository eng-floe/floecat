-- DuckDB bootstrap for Floecat quickstart (host mode).
-- Use with: duckdb -init tools/duckdb-localstack-init.sql

INSTALL httpfs;
LOAD httpfs;

INSTALL aws;
LOAD aws;

INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET floecat_localstack_s3 (
  TYPE S3,
  PROVIDER config,
  KEY_ID 'test',
  SECRET 'test',
  REGION 'us-east-1',
  ENDPOINT 'localhost:4566',
  URL_STYLE 'path',
  USE_SSL false
);

SET s3_endpoint='localhost:4566';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_region='us-east-1';
SET s3_access_key_id='test';
SET s3_secret_access_key='test';

ATTACH 'examples' AS iceberg_floecat (
  TYPE iceberg,
  ENDPOINT 'http://localhost:9200/',
  AUTHORIZATION_TYPE none,
  ACCESS_DELEGATION_MODE 'none'
);

