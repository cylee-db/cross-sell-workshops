-- Databricks notebook source
USE CATALOG field_demos;
USE DATABASE cross_sell_cyl;

CREATE OR REPLACE TABLE ex1_user_features AS
SELECT
  u.id,
  u.email,
  u.firstname,
  u.lastname,
  u.address,
  u.city,
  t.creation_date,
  t.last_activity_date,
  t.current_date,
  t.tenure_years,
  u.last_ip,
  u.postcode,
  p.user_cnt AS postcode_user_cnt
     FROM ex1_users_bronze AS u
LEFT JOIN ex1_postcode_user_cnt AS p
       ON u.postcode = p.postcode
LEFT JOIN ex1_user_tenure AS t
       ON u.id = t.id
;
