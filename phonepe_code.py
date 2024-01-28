#Importing necessary packages
import pandas as pd
import numpy as np
import json
import os
from pprint import pprint 
import mysql.connector
from sqlalchemy import create_engine,text
import pymysql
from urllib.parse import quote
import json
import streamlit as st
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.graph_objects as go
from babel.numbers import format_number
import matplotlib.pyplot as plt


#Function to extract statewise aggregated transactions data from quarterly json files
def extract_agg_txn_data(path):
    agg_data = []
    state_list = os.listdir(path)
    for state in state_list:
        state_path = path+"\\"+state
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path+"\\"+year
            quarter_list = os.listdir(year_path)
            for quarter in quarter_list:
                quarter_path = year_path+"\\"+quarter
                with open(quarter_path) as f:
                    data = json.load(f)                
                    for i in data['data']['transactionData']:
                        result = dict(State=state,Year=int(year),Quarter=quarter[0],Transaction_type = i['name'],
                                      Transaction_count = i['paymentInstruments'][0]['count'],
                                      Transaction_amount = round(i['paymentInstruments'][0]['amount'],2))
                        agg_data.append(result)
    df = pd.DataFrame.from_dict(agg_data)
    return df

#Function to extract statewise aggregated users data from Quarterly json Files
def extract_agg_user_data(path):
    agg_user_data = []
    brand_user_data = []
    state_list = os.listdir(path)
    for state in state_list:
        state_path = path+"\\"+state
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path+"\\"+year
            quarter_list = os.listdir(year_path)
            for quarter in quarter_list:
                quarter_path = year_path+"\\"+quarter
                with open(quarter_path) as f:
                    data = json.load(f)
                    registered_users = data['data']['aggregated']['registeredUsers']
                    user_result = dict(State=state,Year=year,Quarter=quarter[0],Registered_users = registered_users)
                    agg_user_data.append(user_result)                    
                    try:
                        for i in data['data']['usersByDevice']:
                            brand_result = dict(State=state,Year=year,Quarter=quarter[0],Registered_users = registered_users,
                                         Brand = i['brand'],User_count = i['count'],Percentage = round(i['percentage'],2))
                            brand_user_data.append(brand_result)
                    except:
                        continue
    user_df = pd.DataFrame.from_dict(agg_user_data)
    brand_df = pd.DataFrame.from_dict(brand_user_data)
    return user_df,brand_df

#Function to extract map transaction data from statewise quarterly json file
def extract_map_txn_data(path):
    map_data = []
    state_list = os.listdir(path)
    for state in state_list:
        state_path = path+"\\"+state
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path+"\\"+year
            quarter_list = os.listdir(year_path)
            for quarter in quarter_list:
                quarter_path = year_path+"\\"+quarter
                with open(quarter_path) as f:
                    data = json.load(f)
                    for i in data['data']['hoverDataList']:
                        result = dict(State=state,Year=year,Quarter=quarter[0],District_name=i['name'],District_users_count=i['metric'][0]['count'],
                                     District_amount=round(i['metric'][0]['amount'],2))
                        map_data.append(result)
    df = pd.DataFrame.from_dict(map_data)
    return df

#Function to extract map user data from statewise quarterly json files
def extract_map_user_data(path):
    map_data = []
    state_list = os.listdir(path)
    for state in state_list:
        state_path = path+"\\"+state
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path+"\\"+year
            quarter_list = os.listdir(year_path)
            for quarter in quarter_list:
                quarter_path = year_path+"\\"+quarter
                with open(quarter_path) as f:
                    data = json.load(f)
                    for i,j in data['data']['hoverData'].items():
                        result = dict(State=state,Year=year,Quarter=quarter[0],District_name = i,District_users = j['registeredUsers'])
                        map_data.append(result)
    df = pd.DataFrame.from_dict(map_data)
    return df  

#Function to extract top transaction district and postalcode wise
def extract_top_txn_data(path):
    top_data = []
    postal_data = []
    state_list = os.listdir(path)
    for state in state_list:
        state_path = path+"\\"+state
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path+"\\"+year
            quarter_list = os.listdir(year_path)
            for quarter in quarter_list:
                quarter_path = year_path+"\\"+quarter
                with open(quarter_path) as f:
                    data = json.load(f)
                    for i in data['data']['districts']:
                        result = dict(State=state,Year=year,Quarter=quarter[0],District_name=i['entityName'],
                                      Transaction_count = i['metric']['count'],
                                     Transaction_amount = i['metric']['amount'])
                        top_data.append(result)
                    for i in data['data']['pincodes']:
                        result1 = dict(State=state,Year=year,Quarter=quarter[0],Postcode=i['entityName'],
                                     Transaction_count = i['metric']['count'],
                                     Transaction_amount = i['metric']['amount'])
                        postal_data.append(result1)
    top_df = pd.DataFrame.from_dict(top_data)
    postal_df = pd.DataFrame.from_dict(postal_data)
    return top_df,postal_df
#Function to extract top users district and postalcode wise
def extract_top_user_data(path):
    top_data = []
    postal_data = []
    state_list = os.listdir(path)
    for state in state_list:
        state_path = path+"\\"+state
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path+"\\"+year
            quarter_list = os.listdir(year_path)
            for quarter in quarter_list:
                quarter_path = year_path+"\\"+quarter
                with open(quarter_path) as f:
                    data = json.load(f)
                    for i in data['data']['districts']:
                        result = dict(State=state,Year=year,Quarter=quarter[0],District_name=i['name'],
                                      Registered_users = i["registeredUsers"])
                        top_data.append(result)
                    for i in data['data']['pincodes']:
                        result1 = dict(State=state,Year=year,Quarter=quarter[0],Postcode=i['name'],
                                     Registered_users = i["registeredUsers"])
                        postal_data.append(result1)
    top_df = pd.DataFrame.from_dict(top_data)
    postal_df = pd.DataFrame.from_dict(postal_data)
    return top_df,postal_df

# Function to display Bar chart for Transaction category
def bar_chart(year,quesn):
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    conn = engine.connect()
    query = text(f'''SELECT Transaction_type,SUM(Transaction_count) AS Total_txn_count,
                    SUM(Transaction_amount) AS Total_txn_amount
                    FROM phonepe_project.agg_txn_data
                    WHERE Year = {year} GROUP BY Year,Transaction_type''')
    query1 = text(f'''SELECT State,Year,SUM(Transaction_count) AS Total_txn_count,SUM(Transaction_amount) AS Total_txn_amount
                    FROM phonepe_project.agg_txn_data   WHERE Year = {year} GROUP BY Year,State ORDER BY Total_txn_count LIMIT 10 ''')
    mysql_data = pd.read_sql(query, con=conn)
    mysql_data1 = pd.read_sql(query1,con=conn)
    if quesn == 1:
        fig = px.bar(mysql_data, x="Transaction_type", y="Total_txn_count", title=f"{year} : Transaction categories", width=600, height=400)
    if quesn == 2:
        fig = px.bar(mysql_data, x="Transaction_type", y="Total_txn_amount", title=f"{year} : Transaction categories", width=600, height=400)
    if quesn == 3:
        fig = px.bar(mysql_data1, x="State", y="Total_txn_count", title=f"{year} : Least Transaction count States", width=600, height=400)
    if quesn == 4:
        fig = px.bar(mysql_data1, x="State", y="Total_txn_amount", title=f"{year} : Least Transaction amount States", width=600, height=400)
    

    fig.update_traces(marker_color = 'purple')
    st.plotly_chart(fig)

#Function to display line chart for Registered users yearwise
def line_chart(state_name):
    state_str = str(state_name)
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    conn = engine.connect()
    query = text('''SELECT Year,SUM(Registered_users) as Total_Registered_users FROM phonepe_project.agg_user_data
                    WHERE State = '{}' GROUP BY Year, State'''.format(state_str))
    mysql_data = pd.read_sql(query, con=conn)
    fig = px.line(mysql_data, x="Year", y="Total_Registered_users", title=f"{state_name} : Registered users count") 
    st.plotly_chart(fig)

#Function to display pie chart
def pie_chart(option):
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    conn = engine.connect()
    query = text('''SELECT Brand,SUM(user_count) as Total_user_count FROM phonepe_project.agg_brand_data
                GROUP BY Brand''')
    query1 = text('''SELECT Year,Brand,SUM(user_count) as Total_user_count FROM phonepe_project.agg_brand_data
                    WHERE Year = '{}' GROUP BY Year,Brand'''.format(option))
    if option == "Aggregated":
        mysql_data = pd.read_sql(query, con=conn)
    else:
        mysql_data = pd.read_sql(query1, con=conn)
    fig = px.pie(mysql_data,names="Brand",values="Total_user_count", width=800, height=600)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig)

# Function to display scatter plot chart
def scatter_chart(year):
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    conn = engine.connect()
    query = text('''SELECT State,Year,District_name,sum(Transaction_count) AS District_txn_count, SUM(Transaction_amount) as District_txn_amount
                    FROM phonepe_project.top_dist_txn where year = '{}' GROUP BY District_name,State'''.format(year))
    mysql_data = pd.read_sql(query, con=conn)
    fig = px.scatter(mysql_data, x= "District_txn_count", y="District_txn_amount",color = "State",hover_data=['District_name'])
    st.plotly_chart(fig)
    

#Function to convert amount to cr,lakh,thousands
def format_cash(amount):
    def truncate_float(number, places):
        return int(number * (10 ** places)) / 10 ** places

    if amount < 1e3:
        return amount

    if 1e3 <= amount < 1e5:
        return str(truncate_float((amount / 1e5) * 100, 2)) + " K"

    if 1e5 <= amount < 1e7:
        return str(truncate_float((amount / 1e7) * 100, 2)) + " L"

    if amount > 1e7:
        return str(truncate_float(amount / 1e7, 2)) + " Cr"

#Function to format numbers with commas
def format_comma(num):
    x = format_number(int(num),locale='en_IN')
    return x

#Function to create table and insert data into mysql
def create_mysql_table():
    mydb = mysql.connector.connect(host="localhost",user="root",password="**pwd**")
    mycursor = mydb.cursor()
    mycursor.execute("CREATE DATABASE phonepe_project")
    mydb = mysql.connector.connect(host="localhost",user="root",password="**pwd**",database = "phonepe_project")
    mycursor = mydb.cursor()
    mycursor.execute('''CREATE TABLE agg_txn_data (State VARCHAR(255),Year INT,Quarter INT,
                        Transaction_type VARCHAR(255),Transaction_count INT,Transaction_amount DECIMAL(19,2))''') 
    mycursor.execute("CREATE TABLE agg_user_data (State VARCHAR(255),Year INT,Quarter INT,Registered_users INT)") 
    mycursor.execute('''CREATE TABLE agg_brand_data (State VARCHAR(255),Year INT,Quarter INT,
                        Registered_users INT,Brand VARCHAR(255),User_count INT,Percentage DECIMAL(10,2))''') 
    mycursor.execute('''CREATE TABLE map_txn_data (State VARCHAR(255),Year INT,Quarter INT,
                        District_name VARCHAR(255),District_users_count INT,District_amount DECIMAL(19,2))''')
    mycursor.execute('''CREATE TABLE map_user_data (State VARCHAR(255),Year INT,Quarter INT,
                        District_name VARCHAR(255),District_users INT)''')
    mycursor.execute('''CREATE TABLE top_dist_txn (State VARCHAR(255),Year INT,Quarter INT,
                        District_name VARCHAR(255),Transaction_count INT,Transaction_amount DECIMAL(19,2))''')
    mycursor.execute('''CREATE TABLE top_postal_txn (State VARCHAR(255),Year INT,Quarter INT,
                        Postcode INT,Transaction_count INT,Transaction_amount DECIMAL(19,2))''')
    mycursor.execute('''CREATE TABLE top_dist_user (State VARCHAR(255),Year INT,Quarter INT,
                        District_name VARCHAR(255),Registered_users INT)''')
    mycursor.execute('''CREATE TABLE top_postal_user (State VARCHAR(255),Year INT,Quarter INT,
                        Postcode INT,Registered_users INT)''')     
    return True

def rename_state_name(df):
    df['State'] = df['State'].replace(['andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar','chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu',\
     'delhi','goa','gujarat','haryana','himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh',\
     'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram','nagaland','odisha','puducherry',\
     'punjab','rajasthan','sikkim','tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'],
                                      ['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar',\
     'Chandigarh','Chhattisgarh','Dadra and Nagar Haveli and Daman and Diu','Delhi','Goa','Gujarat','Haryana',\
     'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Madhya Pradesh','Maharashtra',\
     'Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry','Punjab','Rajasthan','Sikkim','Tamil Nadu',\
     'Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal'])
    return df

def into_sql():
    file_path = "E:\\Winnie Documents\\Guvi\\project\\Phonepe Pulse\\pulse\\data"
    aggregated_txn_path = "\\aggregated\\transaction\\country\\india\\state"
    aggregated_user_path = "\\aggregated\\user\\country\\india\\state"
    map_txn_path = "\\map\\transaction\\hover\\country\\india\\state"
    map_user_path = "\\map\\user\\hover\\country\\india\\state"
    top_txn_path = "\\top\\transaction\\country\\india\\state"
    top_user_path = "\\top\\user\\country\\india\\state"
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    df = extract_agg_txn_data(file_path+aggregated_txn_path)
    agg_txn_data_df = rename_state_name(df) 
    agg_txn_data_df.to_sql('agg_txn_data', con=engine, if_exists='append', index=False)
    df1,df2 = extract_agg_user_data(file_path+aggregated_user_path)
    agg_user_data_df =  rename_state_name(df1)  
    agg_brand_data_df = rename_state_name(df2)
    agg_user_data_df.to_sql('agg_user_data',con=engine,if_exists='append',index=False)
    agg_brand_data_df.to_sql('agg_brand_data',con=engine,if_exists='append',index=False)
    df = extract_map_txn_data(file_path+map_txn_path)
    map_txn_data_df = rename_state_name(df)
    map_txn_data_df.to_sql('map_txn_data',con=engine,if_exists='append',index=False)
    df = extract_map_user_data(file_path+map_user_path)
    map_user_data_df = rename_state_name(df)
    map_user_data_df.to_sql('map_user_data',con=engine,if_exists='append',index=False)
    df1,df2 = extract_top_txn_data(file_path+top_txn_path)
    top_dist_txn_df = rename_state_name(df1) 
    top_postal_txn_df = rename_state_name(df2)
    top_dist_txn_df.to_sql('top_dist_txn',con=engine,if_exists='append',index=False)
    top_postal_txn_df.to_sql('top_postal_txn',con=engine,if_exists='append',index=False)
    df1,df2 = extract_top_user_data(file_path+top_user_path)
    top_dist_user_df = rename_state_name(df1) 
    top_postal_user_df = rename_state_name(df2)
    top_dist_user_df.to_sql('top_dist_user',con=engine,if_exists='append',index=False)
    top_postal_user_df.to_sql('top_postal_user',con=engine,if_exists='append',index=False)
    return True

#function to check existence of database in mysql database
def sql_db_check():
    mydb = mysql.connector.connect(host="localhost",user="root",password="**pwd**")
    mycursor = mydb.cursor()
    try:
        mycursor.execute("USE phonepe_project")
        flag = 1
    except:
        flag = 0
    return flag

#Function call to Create Table and insert data into Mysql database
def sql_data_insertion():
    value = sql_db_check()
    if value == 1:
        flag = 0 # Table already exists
    else:
        sql_tables = create_mysql_table()
        insert_data = into_sql()
        flag = 1 # Table created and data inserted
    return flag
        

#Function to get data from mysql database
def data_from_mysql(table_name,year=2018,quarter=1,State=0):
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    conn = engine.connect()
    if table_name == "agg_txn_data_df":
        query = text(f'''SELECT State,Quarter,Year,SUM(Transaction_count) as Total_txn_count,
        SUM(Transaction_amount) as Total_transaction_amount FROM phonepe_project.agg_txn_data WHERE Year = {year} AND Quarter = {quarter}
        GROUP BY State''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "agg_user_data_df":
        query = text(f'''SELECT * FROM phonepe_project.agg_user_data WHERE Year = {year} and Quarter = {quarter}
                        Order by Registered_users DESC''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "top_dist_user_df":
        query = text(f'''SELECT * FROM phonepe_project.top_dist_user WHERE Year = {year} and Quarter = {quarter}
                        Order by Registered_users DESC''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "top_postal_user_df":
        query = text(f'''SELECT * FROM phonepe_project.top_postal_user WHERE Year = {year} and Quarter = {quarter}
                        Order by Registered_users DESC''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "agg_state_txn_data_df":
        values = {'year': year,'quarter' : quarter,'State':State}
        query = text('''SELECT * FROM phonepe_project.agg_txn_data WHERE Year = :year AND Quarter = :quarter AND State = :State;''')
        mysql_data = pd.read_sql(query, con=conn,params = values)
    if table_name == "agg_state_user_data_df":
        values = {'year': year,'quarter' : quarter,'State':State}
        query = text('''SELECT * FROM phonepe_project.agg_user_data WHERE Year = :year AND Quarter = :quarter AND State = :State''')
        mysql_data = pd.read_sql(query, con=conn,params = values)
    if table_name == "top_state_dist_user_df":
        values = {'year': year,'quarter' : quarter,'State':State}
        query = text('''SELECT * FROM phonepe_project.top_dist_user WHERE Year = :year AND Quarter = :quarter AND State = :State
                        Order by Registered_users DESC''')
        mysql_data = pd.read_sql(query, con=conn,params = values)
    if table_name == "top_postal_state_user_df":
        values = {'year': year,'quarter' : quarter,'State':State}
        query = text(f'''SELECT * FROM phonepe_project.top_postal_user WHERE Year = :year AND Quarter = :quarter AND State = :State
                        Order by Registered_users DESC''')
        mysql_data = pd.read_sql(query, con=conn,params = values)
    if table_name == "agg_txn_display_df":
        query = text(f'''SELECT * FROM phonepe_project.agg_txn_data''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "agg_user_display_df":
        query = text(f'''SELECT * FROM phonepe_project.agg_user_data''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "dist_txn_display_df":
        query = text(f'''SELECT * FROM phonepe_project.top_dist_txn''')
        mysql_data = pd.read_sql(query, con=conn)
    if table_name == "dist_user_display_df":
        query = text(f'''SELECT * FROM phonepe_project.top_dist_user''')
        mysql_data = pd.read_sql(query, con=conn)    
       
    return mysql_data

#Function to display choropleth map
def choropleth_map(table_name,year,quarter):
    df = data_from_mysql(table_name,year,quarter)    
    if table_name == "agg_user_data_df":         
        fig = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Registered_users',
        color_continuous_scale='Magenta',)
        fig.update_geos(fitbounds="locations", visible=False) 
        fig.update_layout(title = f"                                            All India Registered Users for {year} - Quarter-{quarter}",
                          margin=dict(l=0, r=0, b=0, t=20),width=800,height=600,coloraxis_showscale=False)
        
    if table_name == "agg_txn_data_df":
        df['text'] =    "Transaction_amount :" + str(df['Total_transaction_amount']) + '<br>'+ "Transaction_count :" + str(df['Total_txn_count']) 
        fig = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Total_transaction_amount',
        color_continuous_scale='Magenta')
        fig.update_geos(fitbounds="locations", visible=False)   
        fig.update_layout(title = f"                                            All India Total Transaction Amount for {year} - Quarter-{quarter}",
                          margin=dict(l=0, r=0, b=0, t=20),width=800,height=600,coloraxis_showscale=False)                                 
    st.plotly_chart(fig)

#Function to retrieve category values from sql dataframe
def category_values(type,year,quarter):
    engine = create_engine("mysql+pymysql://root:%s@localhost:3306/phonepe_project" % quote('**pwd**'),echo = True)
    conn = engine.connect()
    if type == "Transaction_count":
        query = text(f'''SELECT Year,Quarter,Transaction_type,Sum(Transaction_count) AS category_txn_count FROM phonepe_project.agg_txn_data
        WHERE Year = {year} AND Quarter = {quarter}
        GROUP BY Transaction_type,year,Quarter''')
        mysql_data = pd.read_sql(query, con=conn)
    else:
        query = text(f'''SELECT Year,Quarter,Transaction_type,Sum(Transaction_amount) AS category_txn_amount FROM phonepe_project.agg_txn_data
        WHERE Year = {year} AND Quarter = {quarter}
        GROUP BY Transaction_type,year,Quarter''')
        mysql_data = pd.read_sql(query, con=conn)
    return mysql_data 

#Main Function
# x = sql_data_insertion()
# print(x)


#Streamlit page configuration
st.set_page_config(page_title="Phonepe Data Visualisation",layout="wide", initial_sidebar_state="auto")
st.markdown("<h1 style='text-align: left; color: Purple;'> पे : Phonepe Pulse Data Visualisation</h1>", unsafe_allow_html=True)
#Main Layout with horizontal option bar with 3 options
menu_bar = option_menu(None, ["Home","Data Visualization","Data Reports"], 
    icons=['house', "graph-up",'bar-chart'], 
    menu_icon="cast", default_index=0, orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "#CBC3E3"},
        "icon": {"color": "orange", "font-size": "25px"}, 
        "nav-link": {"font-color":"white","font-size": "25px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "Purple"}
    })   
if menu_bar == "Home":
    col1, col2 = st.columns([2, 2])
    with col1:
        col1_1,col1_2,col1_3 = st.columns([1,2,1])
        with col1_1:
            state_box = st.selectbox(label = "Select an option",options=["All India","State"],label_visibility="hidden")
            if state_box == "State":
                with col1_2:
                    state_option = st.selectbox(label="select an state",options = ['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar',\
                                    'Chandigarh','Chhattisgarh','Dadra and Nagar Haveli and Daman and Diu','Delhi','Goa','Gujarat','Haryana',\
                                    'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','lakshadweep','Ladakh','Madhya Pradesh','Maharashtra',\
                                    'Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry','Punjab','Rajasthan','Sikkim','Tamil Nadu',\
                                    'Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal'],label_visibility="hidden")
        col1_4,col1_5,col1_6,col1_7 = st.columns([1,1,1.03,0.97])
        with col1_4:
            user_txn = st.selectbox("select txn or user",["Transaction","Users"],label_visibility="hidden")   
        with col1_5:
            year = st.selectbox(label = "Select an option",options=["2018","2019","2020","2021","2022","2023"],label_visibility="hidden")
        with col1_6:
            Quarter = st.selectbox("Select Quarter", ["Qtr1(Jan-Mar)","Qtr2(Apr-June)","Qtr3(July-Sep)","Qtr4(Oct-Dec)"],label_visibility="hidden")
        if state_box == "All India" and user_txn == "Transaction":
            with st.container():
                tab1, tab2 = st.tabs(["Transactions", "Values"])
            with tab1:
                df = data_from_mysql("agg_txn_data_df",year,Quarter[3])
                phonepe_txn = format_number(int(df['Total_txn_count'].sum()),locale='en_IN')
                df1 = category_values("Transaction_count",year,Quarter[3])
                st.markdown("<h4 style='text-align: left; color: #28282B;'>All India Phonepe Transactions</h4>", unsafe_allow_html=True)
                st.markdown(f"<h5 style='text-align: left; color: blue ;'>&emsp;&emsp;&emsp;&emsp;&emsp;{phonepe_txn}</h5>", unsafe_allow_html=True)
                df1['category_txn_count'] = df1['category_txn_count'].apply(format_comma)
                category_df = df1[['Transaction_type','category_txn_count']].style.hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html(border=None)
                st.write(category_df,unsafe_allow_html=True)     
              
            with tab2:
                df = data_from_mysql("agg_txn_data_df",year,Quarter[3])
                txn_value = format_cash(df['Total_transaction_amount'].sum())
                df1 = category_values("Transaction_amount",year,Quarter[3])
                st.markdown("<h4 style='text-align: left; color: #28282B;'>&emsp;&emsp;&emsp;Transaction value</h4>", unsafe_allow_html=True)
                st.markdown(f"<h5 style='text-align: left; color: blue ;'>&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;{txn_value}</h5>", unsafe_allow_html=True)
                df1['category_txn_amount'] = df1['category_txn_amount'].apply(format_comma)
                category_df = df1[['Transaction_type','category_txn_amount']].style.hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                st.write(category_df,unsafe_allow_html=True)
        if state_box == "All India" and user_txn == "Users":
            with st.container():
                tab1, tab2, tab3 = st.tabs(["Users", "Districts","Postal"])
                with tab1:
                    df = data_from_mysql("agg_user_data_df",year,Quarter[3])
                    user_txn = format_number(int(df['Registered_users'].sum()),locale='en_IN')
                    st.markdown("<h4 style='text-align: left; color: purple;'>All India Phonepe Registered Users</h4>", unsafe_allow_html=True)
                    st.markdown(f"<h5 style='text-align: left; color: #28282B ;'>&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;{user_txn}</h5>", unsafe_allow_html=True)
                    st.markdown("<h5 style='text-align: left; color: purple;'>Top 10 States</h5>", unsafe_allow_html=True)
                    df['Registered_users'] = df['Registered_users'].apply(format_cash)
                    df1 = df.head(10)
                    user_df = df1[['State','Registered_users']].style.hide(axis='columns').hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                    st.write(user_df,unsafe_allow_html=True)
                with tab2:
                    df = data_from_mysql("top_dist_user_df",year,Quarter[3])
                    df['Registered_users']= df['Registered_users'].apply(format_cash)
                    df1 = df[['District_name','Registered_users']].head(10).apply(lambda x: x.astype(str).str.title())
                    user_df = df1.style.hide(axis='columns').hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                    st.markdown("<h4 style='text-align: left; color: purple;'>Top 10 Districts</h4>", unsafe_allow_html=True)
                    st.write(user_df,unsafe_allow_html=True)
                with tab3:
                    df = data_from_mysql("top_postal_user_df",year,Quarter[3])
                    df['Registered_users']= df['Registered_users'].apply(format_cash)
                    df1 = df[['Postcode','Registered_users']].head(10).apply(lambda x: x.astype(str).str.title())
                    user_df = df1.style.hide(axis='columns').hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                    st.markdown("<h4 style='text-align: left; color: purple;'>Top 10 Postal code</h4>", unsafe_allow_html=True)
                    st.write(user_df,unsafe_allow_html=True)
        if state_box == "State" and user_txn == "Transaction" :
            with st.container():
                tab1, tab2 = st.tabs(["Transactions", "Values"])
            with tab1:
                df = data_from_mysql("agg_state_txn_data_df",year,Quarter[3],state_option)
                phonepe_txn = format_number(int(df['Transaction_count'].sum()),locale='en_IN')
                st.markdown(f"<h4 style='text-align: left; color: #28282B;'>{state_option} Phonepe Transactions</h4>", unsafe_allow_html=True)
                st.markdown(f"<h5 style='text-align: left; color: blue ;'>&emsp;&emsp;&emsp;&emsp;&emsp;{phonepe_txn}</h5>", unsafe_allow_html=True)
                df['Transaction_count'] = df['Transaction_count'].apply(format_comma)
                category_df = df[['Transaction_type','Transaction_count']].style.hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html(border=None)
                st.write(category_df,unsafe_allow_html=True)     
              
            with tab2:
                df = data_from_mysql("agg_state_txn_data_df",year,Quarter[3],state_option)
                txn_value = format_cash(df['Transaction_amount'].sum())
                st.markdown(f"<h4 style='text-align: left; color: #28282B;'>{state_option}&nbsp;Transaction value</h4>", unsafe_allow_html=True)
                st.markdown(f"<h5 style='text-align: left; color: blue ;'>&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;{txn_value}</h5>", unsafe_allow_html=True)
                df['Transaction_amount'] = df['Transaction_amount'].apply(format_cash)
                category_df = df[['Transaction_type','Transaction_amount']].style.hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                st.write(category_df,unsafe_allow_html=True)
        if state_box == "State" and user_txn == "Users" :
             with st.container():
                tab1, tab2 = st.tabs(["Users","Postal"])
                with tab1:
                    df = data_from_mysql("agg_state_user_data_df",year,Quarter[3],state_option)
                    user_txn = format_number(int(df['Registered_users']),locale='en_IN')
                    st.markdown(f"<h4 style='text-align: left; color: purple;'>{state_option} Phonepe Registered Users</h4>", unsafe_allow_html=True)
                    st.markdown(f"<h5 style='text-align: left; color: #28282B ;'>&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;{user_txn}</h5>", unsafe_allow_html=True)
                    st.markdown("<h5 style='text-align: left; color: purple;'>Top 10 Districts</h5>", unsafe_allow_html=True)
                    df1 = data_from_mysql("top_state_dist_user_df",year,Quarter[3],state_option)
                    df1['Registered_users'] = df1['Registered_users'].apply(format_cash)
                    df2 = df1.head(10)
                    user_df = df2[['District_name','Registered_users']].style.hide(axis='columns').hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                    st.write(user_df,unsafe_allow_html=True)
                
                with tab2:
                    df = data_from_mysql("top_postal_state_user_df",year,Quarter[3],state_option)
                    df['Registered_users']= df['Registered_users'].apply(format_cash)
                    df1 = df[['Postcode','Registered_users']].head(10).apply(lambda x: x.astype(str).str.title())
                    user_df = df1.style.hide(axis='columns').hide(axis='columns').hide(axis='index').set_properties(**{'text-align': 'left','background-color': 'white','color': 'black','border-color': 'white'}).to_html()
                    st.markdown(f"<h4 style='text-align: left; color: purple;'>{state_option} Top 10 Postal code</h4>", unsafe_allow_html=True)
                    st.write(user_df,unsafe_allow_html=True)
    
    with col2:
            if user_txn == "Transaction":
                choropleth_map("agg_txn_data_df",int(year),int(Quarter[3]))
            else:
                choropleth_map("agg_user_data_df",int(year),int(Quarter[3]))     


if menu_bar == "Data Visualization":
    ques1 = "Which Transaction type has highest Transaction count?"
    ques2 = "Which Transaction type has highest Transaction amount?"
    ques3 = "Which 10 states has least Transaction count and amount?"
    ques4 = "Yearwise Registered users for each state "
    ques5 = "Which brand mobile users use phonepe most(2018-2022)?"
    ques6 = "Relation between Transaction counts and Transaction amount"

    ques = st.selectbox("select a question to get visualization",options=[ques1,ques2,ques3,ques4,ques5,ques6],label_visibility='hidden')
    if ques == ques1:
        bar1,bar2= st.columns([1,3])
        with bar1:
            year = st.selectbox("select year",options=['2018','2019','2020','2021','2022','2023'],label_visibility='hidden')
        with bar2:       
            bar_chart(year,1)
    if ques == ques2:
        bar1,bar2= st.columns([1,3])
        with bar1:
            year = st.selectbox("select year",options=['2018','2019','2020','2021','2022','2023'],label_visibility='hidden')
        with bar2:       
            bar_chart(year,2)
    if ques == ques3:
        bar1,bar2= st.columns([1.5,3])
        with bar1:
            option = st.selectbox("select option",options=['Transaction Count','Transaction Amount'],label_visibility='hidden')
            year = st.selectbox("select year",options=['2018','2019','2020','2021','2022','2023'],label_visibility='hidden')
        with bar2:
            if option == "Transaction Count":
                bar_chart(year,3)
            if option == "Transaction Amount":
                bar_chart(year,4)
    if ques == ques4:
        bar1,bar2= st.columns([1,3])
        with bar1:
            state = st.selectbox("select option",options = ['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar',\
                                    'Chandigarh','Chhattisgarh','Dadra and Nagar Haveli and Daman and Diu','Delhi','Goa','Gujarat','Haryana',\
                                    'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','lakshadweep','Ladakh','Madhya Pradesh','Maharashtra',\
                                    'Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry','Punjab','Rajasthan','Sikkim','Tamil Nadu',\
                                    'Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal'],label_visibility="hidden")
        with bar2:
            line_chart(state)
    if ques == ques5:
        bar1,bar2= st.columns([1,3])
        with bar1:
            option = st.selectbox("select year",options=['Aggregated','2018','2019','2020','2021'],label_visibility='hidden')
        with bar2:
            st.markdown(f"<h4 style='text-align: left; color: purple;'>&emsp;&emsp;&emsp;&emsp;&emsp;{option} Phonepe users by Mobile Brand</h4>", unsafe_allow_html=True)
            pie_chart(option)
    if ques == ques6:
        bar1,bar2= st.columns([1,3])
        with bar1:
            year = st.selectbox("select year",options=['2018','2019','2020','2021','2022','2023'],label_visibility='hidden')
        with bar2:
            st.markdown(f"<h4 style='text-align: left; color: purple;'>&emsp;&emsp;&emsp;&emsp;&emsp;{year} : Transaction count Vs Transaction amount</h4>", unsafe_allow_html=True)
            scatter_chart(year)

if menu_bar == "Data Reports":
    bar1,bar2= st.columns([1,3])
    with bar1:
        option = st.selectbox("Select dataframe from options to view",options = ['Aggregate_Transaction_data','Aggregate_user_data','District_transaction_data','District_user_data'])
    
    if option == "Aggregate_Transaction_data":
        df = data_from_mysql("agg_txn_display_df")
        st.dataframe(df)
    if option == "Aggregate_user_data":
        df = data_from_mysql("agg_user_display_df")
        st.dataframe(df)
    if option == "District_transaction_data":
        df = data_from_mysql("dist_txn_display_df")
        st.dataframe(df)
    if option == "District_user_data":
        df = data_from_mysql("dist_user_display_df")
        st.dataframe(df)














        






            

            


           
            
        
