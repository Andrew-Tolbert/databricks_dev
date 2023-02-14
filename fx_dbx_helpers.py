"""
Helper Functions 
Andrew Tolbert , Databricks Solution Architect 
Some Repurposed (not original author) and Some Original - flagged in comments
"""

# REPRUPOSED Use to Embed Google Slides in dbx notebooks 
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

