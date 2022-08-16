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

@st.cache
def generate_forecast(store,item,days):
    forecast_query = f"""
    with train as (
        select date, store, item, sales
        from "BUSINESS_DATA"."PUBLIC"."SALES_DATA"
        where store = {store} and item = {item} and date < '2017-10-31'
    )
    select train.store, train.item, res.*
    from train, table(my_forecasting_app.snowml.forecast(train.date, train.sales, {days}) over (partition by 1)) res;
    """
    forecast_df = session.sql(forecast_query)
    forecast_df = forecast_df.filter(F.col("TS") >= '2017-6-01').to_pandas()
    forecast_df['TS'] = pd.to_datetime(forecast_df['TS']).dt.date
    return forecast_df

@st.cache
def insert_comment(date,store,item,comment):
    key = str(date)+'|'+str(store)+'|'+str(item)
    session.create_dataframe(data=[[key,date,store,item,comment]],schema=["key","date","store","item","comment"]).write.mode("append").save_as_table('forecast_comments')
    return None

sales_df = session.table('SALES_DATA')

@st.cache
def populate_dropdown(column):
    return list(sales_df[[column]].distinct().sort(column,ascending=True).to_pandas()[column])


st.write('# Sales Forecast Builder')
st.write('### Pick forecast parameters')

# Data for selecting forecast parameters
store_list = populate_dropdown('STORE')
item_list = populate_dropdown('ITEM')

# Dropdowns and slider for inputs
v_store = st.selectbox('Store: ',store_list)
v_item = st.selectbox('Item: ',item_list)
v_days = st.slider('Days to forecast:',1,80)

# generate a forecast and visualize using Plotly
if st.checkbox('Run Forecast'):

    st.write(f'### {v_days} day forecast for Item {v_item} at Store {v_store}')
    forecast_df = generate_forecast(v_store, v_item, v_days)

    fig1 = px.line(
            forecast_df,
            x="TS",
            y=["Y","FORECAST"],
            # color="NICKNAME",
            markers=False,
            template='none'
            # hover_data=["NICKNAME","SCORE","UPDATED"]       
        )
    # Figuring out if reference lines are a good fit for this use case
    # fig1.add_vline('2017-11-01')

    # fig1.update_yaxes(autorange="reversed")
    st.plotly_chart(fig1,use_container_width=True)

    # Add a download as CSV button and the raw forecast data display
    st.write(f'#### Forecast for Store {v_store}, Item {v_item}')

    csv = forecast_df[["TS","FORECAST"]][forecast_df.FORECAST > 0]
    st.download_button(
        label = "Download as CSV",
        data = csv.to_csv().encode('utf-8'),
        file_name=f"item{v_item}_store{v_store}_{v_days}_periods.csv",
        mime = 'text/csv'
    )
    st.table(csv)
    
    # Adding a comment section for users to make notes on forecast dates
    st.write(f'#### Comments for Store {v_store}, Item {v_item}')

    v_comment_date = st.selectbox('Date to comment on:',list(csv["TS"]))
    v_comment = st.text_input('Comment')

    comment_df = session.table('BUSINESS_DATA.PUBLIC.FORECAST_COMMENTS')
    comment_df_filtered = comment_df.filter((F.col("STORE") == v_store) & (F.col("ITEM") == v_item))

    if st.button('Save Comment'):
        insert_comment(v_comment_date,v_store,v_item,v_comment)
        filtered_comments = comment_df_filtered[["DATE","COMMENT"]].to_pandas()
    else:
        filtered_comments = comment_df_filtered[["DATE","COMMENT"]].to_pandas()

    st.write("#### Comment History")
    st.dataframe(filtered_comments)
