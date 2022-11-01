-- Databricks notebook source
USE CATALOG field_demos
;
USE DATABASE cross_sell_cyl
;

CREATE OR REPLACE TABLE ex1_postcode_user_cnt
AS
SELECT
  postcode,
  count(id) as user_cnt
FROM
  ex1_users_bronze
GROUP BY
  postcode
;
