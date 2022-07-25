import streamlit as st
from snowflake.snowpark import Session

# Boilerplate code to create a Snowpark Session
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

# Webapp code with Streamlit

# set up a Snowpark data frame for a specific Store
my_snowpark_dataframe = session.table('SALES_DATA')
store_5_dataframe = my_snowpark_dataframe.filter(my_snowpark_dataframe['STORE'] == 5)

# Create a Webpage title and render a dataframe of the first 100 records
st.write(' # My First App')
st.dataframe(store_5_dataframe.limit(n=100).to_pandas())