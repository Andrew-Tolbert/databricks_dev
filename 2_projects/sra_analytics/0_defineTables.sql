-- Databricks notebook source
USE CATALOG shared; 
USE SCHEMA sra; 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS views (
  ingestDate DATE, 
  date DATE, 
  count INT, 
  uniques INT, 
  total_count INT, 
  total_uniques INT
); 


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS clones (
  ingestDate DATE, 
  date DATE, 
  count INT, 
  uniques INT, 
  total_count INT, 
  total_uniques INT
); 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS referrers (
  ingestDate DATE, 
  referrer STRING, 
  count BIGINT, 
  uniques BIGINT
); 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS referral_paths (
  ingestDate DATE, 
  path STRING, 
  title STRING, 
  count BIGINT, 
  uniques BIGINT
); 
