-- Databricks notebook source
USE CATALOG shared;

USE SCHEMA sra; 

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS sra_analytics; 

-- COMMAND ----------

ALTER SHARE sra_analytics ADD TABLE views; 
ALTER SHARE sra_analytics ADD TABLE referrers; 
ALTER SHARE sra_analytics ADD TABLE clones; 
ALTER SHARE sra_analytics ADD TABLE referral_paths; 
