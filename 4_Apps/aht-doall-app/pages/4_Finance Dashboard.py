import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from functions import deploy_time, to_excel
from streamlit_lightweight_charts import renderLightweightCharts
from datetime import datetime
from streamlit_date_picker import date_range_picker, PickerType


# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

st.set_page_config(
    page_title="Finance Dashboard",
    page_icon="🏦",
    layout="wide",
)
st.sidebar.info("Last Deployment: {}".format(deploy_time()), icon="ℹ️")

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

@st.cache_data(ttl=3)  # only re-query if it's been 30 seconds
def getData():
    priceData = {}
    try:
        tick_filter = st.session_state.ticker
    except: 
        tick_filter = "AAPL"
    
    if st.session_state.get('date_range_picker') is None:
        date_range = [datetime(2023,1,1), datetime(2023,7,19)]
    else:
        date_range = st.session_state.date_range_picker

    # This example query depends on the nyctaxi data set in Unity Catalog, see https://docs.databricks.com/en/discover/databricks-datasets.html for details
    data= sqlQuery(f"""
                    select 
                        cast(date as string)as date,
                        unix_timestamp(date,'yyyy-MM-dd') as unix_time,
                        adj_open, adj_high, adj_low, adj_close, volume, return, '' as trader_notes, 
                        case 
                            when
                                adj_close > lag(adj_close) over (partition by ticker order by date)
                            then 'rgba(0, 150, 136, 0.8)' 
                            else 'rgba(255,82,82, 0.8)' 
                        end as color
                    from ahtsa.fins.ticker_trading 
                    where ticker = '{tick_filter}' 
                    and date >= '{date_range[0]}' and date <= '{date_range[1]}'
                    """)

    timeSeriesData = data.rename(columns={'date': 'time', 'adj_close': 'value'}).to_dict(orient='records')
    volumeSeriesData = data.rename(columns={'date': 'time', 'volume': 'value','color':'color'}).to_dict(orient='records')
    candleStickSeriesData = data.rename(columns={'adj_open': 'open', 'adj_high': 'high','adj_low': 'low','adj_close': 'close','unix_time': 'time'}).to_dict(orient='records')
    priceData['priceSeries'] = timeSeriesData
    priceData['volumeSeries'] = volumeSeriesData
    priceData['candleStickSeriesData'] = candleStickSeriesData
    return priceData,data


col1, col2 = st.columns([1,4])
with col1:
    with st.container(border=True):
        all_tickers = ["AAPL","TSLA","MSFT","META","AMZN","JPM","NFLX","NVDA","AMD","ORCL","BA"]
        ticker = st.selectbox("Select Ticker", all_tickers,key="ticker")
with col2:
    with st.container(border=True):
        default_start, default_end = datetime(2023,1,1), datetime(2023,7,19)
        date_range_string = date_range_picker(picker_type=PickerType.date,
                                        start=default_start, end=default_end,
                                        key='date_range_picker')
    

st.header(f"Trading Activity for {ticker}")

priceVolumeChartOptions = {
    "height": 400,
    "rightPriceScale": {
        "scaleMargins": {
            "top": 0.2,
            "bottom": 0.25,
        },
        "borderVisible": True,
    },
    "overlayPriceScales": {
        "scaleMargins": {
            "top": 0.7,
            "bottom": 0,
        }
    },
    "layout": {
        "background": {
            "type": 'solid',
            "color": '#131722'
        },
        "textColor": '#d1d4dc',
    },
    "grid": {
        "vertLines": {
            "color": 'rgba(42, 46, 57, 0)',
        },
        "horzLines": {
            "color": 'rgba(42, 46, 57, 0.6)',
        }
    }
}
priceData,data = getData()
priceVolumeSeries = [
    {
        "type": 'Area',
        "data": priceData['priceSeries'],
        "options": {
            "topColor": 'rgba(38,198,218, 0.56)',
            "bottomColor": 'rgba(38,198,218, 0.04)',
            "lineColor": 'rgba(38,198,218, 1)',
            "lineWidth": 2,
        }
    }
    ,{
        "type": 'Histogram',
        "data": priceData['volumeSeries'],
        "options": {
            "color": '#26a69a',
            "priceFormat": {
                "type": 'volume',
            },
            "priceScaleId": "" # set as an overlay setting,
        },
        "priceScale": {
            "scaleMargins": {
                "top": 0.7,
                "bottom": 0,
            }
        }
    }
]

seriesCandlestickChartOptions = {
    "layout": {
        "textColor": '#d1d4dc',
         "background": {
            "type": 'solid',
            "color": '#131722'
        },
    }
}

seriesCandlestickChart = [{
    "type": 'Candlestick',
    "data": priceData['candleStickSeriesData'],
    "options": {
        "upColor": '#26a69a',
        "downColor": '#ef5350',
        "borderVisible": False,
        "wickUpColor": '#26a69a',
        "wickDownColor": '#ef5350'
    }
}]

with st.container(border=True):
    st.subheader(f"Price and Volume Series Chart: ${ticker}")
    renderLightweightCharts([
        {
            "chart": priceVolumeChartOptions,
            "series": priceVolumeSeries
        }
    ], 'priceAndVolume')

with st.container(border=True):
    st.subheader(f"Candlestick Chart for: ${ticker}")
    renderLightweightCharts([
        {
            "chart": priceVolumeChartOptions,
            "series": seriesCandlestickChart
        }
    ], 'candlestick')


with st.container(border=True):
    edited_df = st.data_editor(data.drop(columns=['color']), num_rows="dynamic") 


df_xlsx = to_excel(edited_df)
st.download_button(label='📥 Download Current Result',
                                data=df_xlsx ,
                                file_name= 'trader_report.xlsx')

st.write(st.session_state)

