# Databricks notebook source
import inspect

# COMMAND ----------

"""
Helper Functions 
Andrew Tolbert , Databricks Solution Architect 
Some Repurposed (not original author) and Some Original - flagged in comments
"""

# REPRUPOSED Use to Embed Google Slides in dbx notebooks 
def display_slide(slide_id, slide_number):
  displayHTML(f'''
  <div style="width:1150px; margin:auto">
  <iframe
    src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}"
    frameborder="0"
    width="1150"
    height="683"
  ></iframe></div>
  ''')

# COMMAND ----------

def display_html_v2(html: str) -> None:
    """
    Use databricks displayHTML from an external package
    
    Args:
    - html : html document to display
    """
    import inspect
    for frame in inspect.getouterframes(inspect.currentframe()):
        global_names = set(frame.frame.f_globals)
        # Use multiple functions to reduce risk of mismatch
        if all(v in global_names for v in ["displayHTML", "display", "spark"]):
            return frame.frame.f_globals["displayHTML"](html)
    raise EnvironmentError(
        "Unable to detect displayHTML function"
    )


# COMMAND ----------

import inspect

dbutils.library()

# COMMAND ----------

dbutils.fs.ls('s3://aht-fitbit/sleep')

# COMMAND ----------

dbutils.fs.ls('s3://aht-fitbit/sleep')

# COMMAND ----------


