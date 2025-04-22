from datetime import datetime
import pytz
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb 
import pandas as pd


def deploy_time():
  # Get the current time in EST
  est_tz = pytz.timezone('America/New_York')
  current_time = datetime.now(est_tz)
  # Format the datetime
  formatted_time = current_time.strftime("%Y-%m-%d %I:%M:%S %p %Z")  
  return formatted_time 

def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0.00'}) 
    worksheet.set_column('A:A', None, format1)  
    writer.close()
    processed_data = output.getvalue()
    return processed_data