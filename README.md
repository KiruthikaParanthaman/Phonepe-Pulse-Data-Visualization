# Phonepe-Pulse-Data-Visualization
This Phonepe Dashboard is inspired from Pulse Dashboard by Phonepe which through its public github repository provides insights about Phonepe users in India from 2018 to 2023

**Problem Statement:**
1. To Extract data from the Phonepe pulse Github repository through scripting and to clone it.
2. Transform the data into a suitable format and perform any necessary cleaning and pre-processing steps.
3. Insert the transformed data into a MySQL database for efficient storage and retrieval.
4. Create a live geo visualization dashboard using Streamlit and Plotly in Python to display the data in an interactive and visually appealing manner.
5. Fetch the data from the MySQL database to display in the dashboard.
6. Provide at least 10 different dropdown options for users to select different facts and figures to display on the dashboard.

**Final Output:**
![phonepe app scrnshot1](https://github.com/KiruthikaParanthaman/Phonepe-Pulse-Data-Visualization/assets/141828622/0b121d0a-1c70-402e-8c76-da1e513af36e)

**Tools used:**
Python Pandas,plotly,matplotlib,sqlalchemy,json,mysql database,streamlit,Jupyter

**Quickguide of application:**

**Home :**
  Home menu lets users to view either All India data or statewise data ,default option is All India.Users can then view Transaction data or Users data yearwise from 2018 to 2023 and Quarterwise(Quarter1(Jan-March),Quarter2(April-June),Quarter3(July-Sep),Quarter4(Oct-Dec).Kindly note that Phonepe Data for Quarter4(Oct-Dec) for 2023 is not available in repository and hence dashboard will not display any details for that particular quarter for 2023.If transaction data is chosen quick gist of All India Phone Transactions count,All India Phone Transactions amount,category wise transaction count and amount is given. If user data is chosen gist of All India registered users,Top 10 states,Top 10 districts,Top 10 postal codes is displayed.Both Transaction data and users data is configured for each state when particular state is selected. Geographical display of Indian map is also provided with colorscaling for quick identification of statewise transaction count,transaction amount and Registered users
  
**Data Visualization:**  
  Data Visualization menu provides insights to data through 6 predeifined options like least 10 states with Transaction count and amount,Transaction type vs transaction count,yearwise registered users for each state,which brand mobile brand users use phonepe most,Relation between transaction count and transaction amount etc., through Bar chart,Pie chart, Line chart and Scatter Plot

**Data Reports:**
  Data Reports let user view aggregated Transaction data, aggregated user data, Top District Transactions data, Top District users data in datafrmae format

**Approach:**

  1. Data was cloned from Phonepe Github repository to local system and necessary files were imported 
  2. Using OS module and user-defined extract data functions json datas were extracted statewise for every year and every quarter from phonepe github repository in local system and was converted to corresponding       dataframes
  3. Extracted datas were stored in phonepe_project Mysql database usingsqlalc hemy and mysql.connector.connect
  4. Geojson datas downloaded from "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson" and necessary cleaning of data         like Renaming the state name corresponding to the geojson file undertaken
  5. Data stored in mysql database is queried and datas retrieved to display the insights like Category wise transaction break-up,Top 10 Transaction State,All India Registerde phonepe users in dashbaord
  6. Using plotly express px.choropleth option India political map was displayed with corredponding transaction/user data
  7. Data insights provided through varied data visualization charts like Bar chart, Line chart, Scatter plot, Pie chart using plotly library
  8. Aggregated transaction,user,district transaction,district user datas can be viewed as dataframe in Data Reports option
  
  
Data : https://github.com/PhonePe/pulse.git
credits : Phonepe




