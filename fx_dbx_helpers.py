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