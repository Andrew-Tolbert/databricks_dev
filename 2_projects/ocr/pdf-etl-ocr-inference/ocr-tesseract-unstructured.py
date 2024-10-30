# Databricks notebook source
# MAGIC %md
# MAGIC ## Offline OCR on Databricks
# MAGIC This notebook installs `tesseract` and other necessary libraries on a single node cluster and utilizes the `unstructured` library to perfome OCR and chunking without hitting the `unstructured` API.
# MAGIC
# MAGIC This performs single-threaded OCR. If you want to do parallel OCR using Ray, refer to [this notebook](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/2617645319412845/command/2617645319499319).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### init scripts 
# MAGIC We utilize the [`unstructured`](https://unstructured.io/) library to perform OCR and chunking of pdf documents. Unstructured utilizes [tesseract](https://github.com/tesseract-ocr/tesseract) for OCR, which requires tesseract to be installed on every node of the cluster. In order to do that, we could take advantage of databricks [init scripts](https://docs.databricks.com/en/init-scripts/index.html). Init scripts are run at cluster startup and are the best way to install os-level libraries before spark is started on each cluster.
# MAGIC
# MAGIC In this case, it is a very simple, two-line file:
# MAGIC ```bash
# MAGIC apt update
# MAGIC apt-get install -y poppler-utils libmagic-dev tesseract-ocr
# MAGIC ```
# MAGIC
# MAGIC Otherwise, if you use a single-node cluster, you can perform the same installations by running the next cell with the `%sh` magic command.

# COMMAND ----------

# MAGIC %sh
# MAGIC apt update
# MAGIC apt install -y poppler-utils libmagic-dev tesseract-ocr
# MAGIC # if you're working on a single node cluster, you can just run this cell in the notebook instead of using an init script

# COMMAND ----------

# MAGIC %pip install unstructured[pdf] --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ahtsa_dev;
# MAGIC USE SCHEMA raw;

# COMMAND ----------

catalog = "ahtsa_dev"
db = "raw"
volume = "/Volumes/ahtsa_dev/raw/data/pdf"

# COMMAND ----------

# MAGIC %md
# MAGIC Get the file names of all of PDFs stored in the Volume

# COMMAND ----------

import glob, os

pdfs = []
for file in glob.glob(f"{volume}/*.pdf"):
    pdfs.append(file)

print(pdfs)

# COMMAND ----------

# MAGIC %md
# MAGIC Declare a function to run [`unstructured`](https://unstructured.io/) paritioning and chunking functions. 

# COMMAND ----------

import pandas
import datetime as dt

from unstructured.partition.pdf import partition_pdf
from unstructured.chunking.title import chunk_by_title

from pyspark.sql.types import ArrayType, StringType

def parse_pdfs(location: str) -> list:
  stime = dt.datetime.now()
  # elements = partition_pdf(location, strategy="ocr_only")
  elements = partition_pdf(location, strategy="hi_res")
  chunks = chunk_by_title(elements)
  print(str(dt.datetime.now() - stime) + f" for {location}")
  return [str(x) for x in chunks if len(str(x)) > 50]


# COMMAND ----------

all_chunks = []
for i in range(len(pdfs)):
  chunks = parse_pdfs(pdfs[i])
  all_chunks = all_chunks + [{
                                "chunk_id": f"{str(i)}_{str(j)}",
                                "file": pdfs[i],
                                "chunk": x
                             } for j,x in enumerate(chunks)]

all_chunks

# COMMAND ----------

# MAGIC %md
# MAGIC Take the chunking results and write them to a Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC You may have to wait a couple of minutes for the embedding model to populate the vectors on the vector search table.
# MAGIC
# MAGIC But then you can query the table with vector search!

# COMMAND ----------


