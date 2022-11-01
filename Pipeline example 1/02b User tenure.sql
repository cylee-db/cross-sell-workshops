-- Databricks notebook source
USE CATALOG field_demos;
USE DATABASE cross_sell_cyl;

CREATE
OR REPLACE TABLE ex1_user_tenure AS
SELECT
  id,
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date,
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date,
  now() as current_date,
  datediff(
    YEAR,
    to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss"),
    now()
  ) tenure_years
FROM
  ex1_users_bronze;
