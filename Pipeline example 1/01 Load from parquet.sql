-- Databricks notebook source
USE CATALOG field_demos
;
USE DATABASE cross_sell_cyl
;

CREATE TABLE IF NOT EXISTS ex1_users_bronze (
  id BIGINT,
  email STRING,
  firstname STRING,
  lastname STRING,
  address STRING,
  city STRING,
  creation_date STRING,
  last_activity_date STRING,
  last_ip STRING,
  postcode STRING
)
;

COPY INTO ex1_users_bronze
FROM
  's3://path_to_s3_location/retail_raw/user_parquet/'
FILEFORMAT = parquet
;

SELECT
  *
FROM
  ex1_users_bronze
;
