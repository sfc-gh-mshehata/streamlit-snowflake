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
v_months = st.slider('Periods to forecast:',1,80)

forecast_query = f"""
with train as (
    select date, store, item, sales
    from "BUSINESS_DATA"."PUBLIC"."SALES_DATA"
    where store = {v_store} and item = {v_item} and date < '2017-10-31'
)
select train.store, train.item, res.*
from train, table(my_forecasting_app.snowml.forecast(train.date, train.sales, {v_months}) over (partition by 1)) res;
"""


forecast_df = session.sql(forecast_query)
# forecast_df = session.sql(forecast_query).filter(F.col("TS") >= '2015-10-31')

if st.button('Run Forecast'):

    st.write(f"### {v_months} month forecast for Item {v_item} at Store {v_store}")
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

    fig1.update_yaxes(autorange="reversed")
    st.plotly_chart(fig1,use_container_width=True)

# if st.checkbox('Show forecast details'):
#     st.dataframe(forecast_df.filter(forecast_df['FORECAST'] != NullType))



