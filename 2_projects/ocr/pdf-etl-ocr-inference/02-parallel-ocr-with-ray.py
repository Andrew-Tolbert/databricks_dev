# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 - Parallel OCR with Ray on Databricks
# MAGIC This notebook converts your databricks cluster into a [Ray cluster](https://docs.databricks.com/en/machine-learning/ray-integration.html) for distributed processing. This allows us to use every core available on the cluster's worker node to perform OCR and chunking of PDF documents at the same time. In testing, you can observe a nearly linear scaling for every core added to the cluster. For example:
# MAGIC * _Traditional processing:_ OCR on average takes `3 minutes` per document, so `100 documents` would take approximately **`5 hours.`**
# MAGIC * _Ray on Databricks:_ OCR on average take `3 minutes` per document per core, so `100 documents` running on a cluster of `3 worker nodes` with `8 cores per node` takes approximately **`15 minutes.`**
# MAGIC
# MAGIC Note: in this example, there are some complex outlier documents that can take upwards of 30 minutes for OCR, so this makes the total processing time for 100 documents longer. But those numbers hold true for functions having more consistent processing speeds. These outliers would even more greatly affect processing times if running in series.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### init scripts 
# MAGIC We utilize the [`unstructured`](https://unstructured.io/) library to perform OCR and chunking of pdf documents. Unstructured utilizes [tesseract](https://github.com/tesseract-ocr/tesseract) for OCR, which requires tesseract to be installed on every node of the cluster. In order to do that, we have to take advantage of databricks [init scripts](https://docs.databricks.com/en/init-scripts/index.html). Init scripts are run at cluster startup and are the best way to install os-level libraries before spark is started on each cluster.
# MAGIC
# MAGIC In this case, it is a very simple, two line file:
# MAGIC ```bash
# MAGIC apt update -y
# MAGIC apt-get install -y poppler-utils libmagic-dev tesseract-ocr
# MAGIC ```

# COMMAND ----------

# if you're working on a single node cluster, you can just run this line in the notebook instead of using an init script
# %sh apt-get install -y poppler-utils libmagic-dev tesseract-ocr

# COMMAND ----------

# MAGIC %pip install databricks-sdk unstructured[pdf] ray[default]>=2.3.0 --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG 'justinm';
# MAGIC USE SCHEMA ner;

# COMMAND ----------

VOLUME = "/Volumes/justinm/ner/data/"

# COMMAND ----------

# MAGIC %md
# MAGIC Pull filenames from `papers` table into a list. We order by filesize descending to make sure we process the larger files first so we don't have the longest files processed at the very end of the parallel process.

# COMMAND ----------

filenames = spark.sql('SELECT filename FROM papers ORDER BY filesize DESC').collect()
filenames = [row.filename for row in filenames]

# COMMAND ----------

# MAGIC %md
# MAGIC To prepare for Ray cluster declaration, we pull the current number of worker nodes and cores using the [`databricks-sdk`](https://databricks-sdk-py.readthedocs.io/en/latest/)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
w = WorkspaceClient()
cluster_info = w.clusters.get(cluster_id=cluster_id)
nodes = w.clusters.list_node_types()

max_worker_nodes = len(cluster_info.executors)
num_cpus_worker_node = int([x.num_cores for x in nodes.node_types if x.node_type_id==cluster_info.node_type_id][0])

print(f"max_worker_nodes={max_worker_nodes}")
print(f"num_cpus_worker_node={num_cpus_worker_node}")

# COMMAND ----------

# MAGIC %md
# MAGIC Start a Ray cluster on your databricks cluster. This allows you to broadcast a `ray.remote` function across the cluster to take advantage of processing on every core in the cluster. This temporarily overrides the spark functionality of the cluster. However, when you shutdown your Ray cluster, all of your native spark functionality comes back.
# MAGIC
# MAGIC I recommend clicking on the `Open Ray Cluster Dashboard in a new tab` link at the bottom of this cell's output to track the utilization/parallelization of the Ray cluster.

# COMMAND ----------

import ray
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster

try:
  shutdown_ray_cluster()
  print("Successfully shut down the currently running Ray cluster. Setting up a new one.")
except:
  print("No current Ray cluster. Setting up a new one.")

setup_ray_cluster(
  max_worker_nodes=max_worker_nodes,
  num_cpus_worker_node=num_cpus_worker_node,
  num_gpus_worker_node=0
)

# COMMAND ----------

# MAGIC %md
# MAGIC Declare a remote ray function to run [`unstructured`](https://unstructured.io/) paritioning and chunking functions in parallel across the cluster. 

# COMMAND ----------

import pandas
import datetime as dt

from unstructured.partition.pdf import partition_pdf
from unstructured.chunking.title import chunk_by_title

from pyspark.sql.types import ArrayType, StringType

@ray.remote
def parse_pdf_ray(location: str) -> list:
  stime = dt.datetime.now()
  # elements = partition_pdf(location, strategy="ocr_only")
  elements = partition_pdf(location, strategy="hi_res")
  chunks = chunk_by_title(elements)
  print(str(dt.datetime.now() - stime) + f" for {location}")
  return [str(x) for x in chunks if len(str(x)) > 50]


# COMMAND ----------

stime = dt.datetime.now()
chunked_pdfs = [parse_pdf_ray.remote(loc) for loc in filenames]
chunks = ray.get(chunked_pdfs)
print("\n\n" + str(dt.datetime.now() - stime) + f" for all")

# COMMAND ----------

# MAGIC %md
# MAGIC Shutdown your Ray cluster after processing to return to expected spark functionality

# COMMAND ----------

# shutdown ray cluster
try:
  shutdown_ray_cluster()
  print("Successfully shut down the currently running Ray cluster.")
except:
  print("No Ray cluster to shut down.")

# COMMAND ----------

# MAGIC %md
# MAGIC Take the chunking results from your `ray.remote` function (stored in a list of lists) and join it with your `papers` dataframe.

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number, explode
from pyspark.sql import Window

papers = spark.sql("SELECT * FROM papers ORDER BY filesize DESC")

add_chunks_udf = udf(lambda i: chunks[i-1], ArrayType(StringType())) 

papers = papers.withColumn("chunk_id", row_number().over(Window.orderBy(monotonically_increasing_id())))
papers = papers.withColumn("chunks", add_chunks_udf("chunk_id")).drop("chunk_id")


# COMMAND ----------

# MAGIC %md
# MAGIC Use [`explode`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.explode.html) to create a row for each chunk. This will allow you to apply your model to each chunk indivually and efficiently. Then save the table for use in subsequent notebooks.

# COMMAND ----------

chunks_df = (papers.withColumn("chunk", explode("chunks"))
                   .select(["entry_id","title","chunk"])
                   .write
                   .mode("overwrite")
                   .saveAsTable("chunks")
            )

# COMMAND ----------

c = spark.sql("SELECT * FROM chunks")
c.count()

# COMMAND ----------

display(c)

# COMMAND ----------


