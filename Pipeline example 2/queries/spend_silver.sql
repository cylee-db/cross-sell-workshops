CREATE TABLE IF NOT EXISTS field_demos.cross_sell_cyl.spend
AS
  SELECT * FROM parquet.`s3://path_to_s3_location/retail_raw/spend`