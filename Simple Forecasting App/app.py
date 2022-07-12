import pandas as pd
import streamlit as st
import plotly.express as px
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

@st.experimental_singleton # magic to cache db connection
def create_snowpark_session(connection_parameters):
    session = Session.builder.configs(connection_parameters).create()
    return session

connection_parameters = {
    "account": st.secrets["account"],
    "user": st.secrets["user"],
    "password": st.secrets["password"],
    "role": st.secrets["role"],
    "warehouse": st.secrets["warehouse"],
    "database": st.secrets["database"],
    "schema": st.secrets["schema"]
  }


session = create_snowpark_session(connection_parameters)

st.write('# Sales Forecast Builder')

st.write('### Pick forecast parameters')

sales_df = session.table('SALES_DATA')

store_list = list(sales_df[['STORE']].distinct().sort('STORE',ascending=True).to_pandas()['STORE'])
item_list = list(sales_df[['ITEM']].distinct().sort('ITEM',ascending=True).to_pandas()['ITEM'])

v_store = st.selectbox('Store: ',store_list)
v_item = st.selectbox('Item: ',item_list)
v_days = st.slider('Days to forecast:',1,80)

forecast_query = f"""
with train as (
    select date, store, item, sales
    from "BUSINESS_DATA"."PUBLIC"."SALES_DATA"
    where store = {v_store} and item = {v_item} and date < '2017-10-31'
)
select train.store, train.item, res.*
from train, table(my_forecasting_app.snowml.forecast(train.date, train.sales, {v_days}) over (partition by 1)) res;
"""


forecast_df = session.sql(forecast_query)
# forecast_df = session.sql(forecast_query).filter(F.col("TS") >= '2015-10-31')

if st.button('Run Forecast'):

    st.write(f"### {v_days} day forecast for Item {v_item} at Store {v_store}")
    forecast_df = forecast_df.filter(F.col("TS") >= '2017-6-01').to_pandas()
    forecast_df['TS'] = pd.to_datetime(forecast_df['TS']).dt.date

    fig1 = px.line(
            forecast_df,
            x="TS",
            y=["Y","FORECAST"],
            # color="NICKNAME",
            markers=False,
            template='none'
            # hover_data=["NICKNAME","SCORE","UPDATED"]       
        )

    # fig1.update_yaxes(autorange="reversed")
    st.plotly_chart(fig1,use_container_width=True)
    st.write(f'#### Store {v_store}, Item {v_item}')

    csv = forecast_df[["TS","FORECAST"]][forecast_df.FORECAST > 0]
    st.download_button(
        label = "Download as CSV",
        data = csv.to_csv().encode('utf-8'),
        file_name=f"item{v_item}_store{v_store}_{v_days}_periods.csv",
        mime = 'text/csv'
    )
    st.table(csv)


