#!/usr/bin/env python
# coding: utf-8

# # Using Advanced Analytics and Data Science Functions in IBM Netezza
# ## IBM Netezza® Performance Server for IBM Cloud Pak® for Data 
# ##
# #### Package contents:
# #### notebook - Install_and_run_Netezza_Analytics.ipynb
# #### text document - Installing_Netezza_Analytics_INZA.txt
# #### text document - Updating_a_database_for_Netezza_Analytics_INZA.txt
# #### documentation - "IBM Netezza In-Database Analytics Developer's Guide.pdf"
# #### sample data set - 
# ##
# ## The IBM Netezza Analytics (INZA) enable advanced analytics in Netezza (NPS) systems
# ### These functions are accessble via SQL calls and leverage the power of NPS through in-database analytics
# ### This notebook uses python to issue the SQL calls, however all analytics are executed in NPS
# ### Executing in NPS has numerous benefits
# ### - reduces need for large analyics systems to support Jupyter notebooks or python systems 
# ### - nearly eliminates moving data between systems increasing performance data security
# ### - enables data sceince functions on very large datasets, maintaining data security and privacy
# ### - allows for deployment of analytics into NPS for high speed analyics "in-place" secured by operational IDs
# ### The code here can be used with Aginity or any tool/application with a connection to NPS
# ##
# ### This is a collection of scripts notebooks, python, SQL for installing and using the analytics functions
# ###
# ### Contents
# ### A. Required inital steps - (this notebook)  Install_and_run_Netezza_Analytics.ipynb
# ###  1. Installing the packages and and enabling the analytics system
# ###  2. Initializing analytics in a selected database
# ###  3a. Connecting to the database - ODBC, JDBC
# ###  3b. Connetcing to the database - Cloud Pak for Data
# 
# 
# ### B. Using the functions - a simple clustering application end to end (Using_the_INZA_functions)
# ###  4. Load some data and analyze the data
# ###  5. Build a cluster model
# ###  6. Evaluate the model
# ###  7. Score the model and deploy

# In[ ]:


# Install the INZA packages and initialize the database(s)
# 
# 1. Have the NPS Admin install the required packes on the NPS system
# The steps are documents in this document Installing_Netezza_Analytics_INZA.txt
#
# 2. In the database where the data to be analyzed is located execute the scripts to create the metadata and model tables
# The database owner / Admin can use this script Updating_a_database_for_Netezza_Analytics_INZA.txt
#
# to verify the installs were successful, check the database has the views/tables
# for example there should be a view in the database: INZA.V_NZA_MODELS (with no records initially)


# In[ ]:


# Connecting to the NPS as a user via ODBC or JDBC (Wwindows OS)
#
# Establish a connection via ODBC or JDBC. 
# Download the netezza drivers from IBM Fix Central. 
# Create and test the connections

# Get the client software 
# Download from Fix Central https://www.ibm.com/support/fixcentral
# -product: IBM Netezza NPS Software and Clients
# -Installed Version: NPS_7.2.1 (or latest)
# -Platform: Windows
# -Browse for fixes: 
# -Select and download: 7.2.1.10-IM-Netezza-NPS-fp126028 (or latest)
# Unpack and install the drivers

# For Netezza ODBC, use windows ODBC tool to create a connection: port is usually 5480
# For Netezza JDBC, in the driver directory, start Netezza JDBC connection tool: nzjdbc.jar
# dsn_db="YourDB"
# dsn_host="YourHostname or IP"
# dsn_port="5480"
# dsn_uid="User"
# dsn_pw="password"
# jdbc_driver_name = "org.netezza.Driver"
# jdbc_driver_loc = os.path.join(r"‪E:\NZ_JDBC\nzjdbc.jar")


# In[ ]:


# Connecting to the NPS as a user of the Cloud Pak for Data
#
# The DBA and/or Cloud Pak Admin can create the required connection in Cloud Pak for Data Connections
# Once created, use the "paste in the code for pandas data frame" for the connection into the notebook using the 0101 widget in the upper right corner of the notebook
# 
get_ipython().system('[PasteIntoCode.JPG](attachment:PasteIntoCode.JPG)')
# paste in the code for pandas dataframe or paste in the credentials 
#


# In[1]:


# make the connection using ODBC
# to establish the netezza odbc conncetion, the system where python executes needs an ODBC connection created
# import the pandas library for data manipulation and pyodbc for ODBC access to netezza
# note: install the libs into notebooks: !{sys.executable} -m pip install pyodbc
# establish the connection "conn" and test by accessing one of the INZA table/veiws then closing the connection

import pandas as pd
import pyodbc

conn=pyodbc.connect('DSN=NZATSVL;UID=admin;PWD=password') #DSN = the connection in the odbc list
cursor=conn.cursor()
cursor.execute("select * from V_NZA_MODELS")
row = cursor.fetchone()
while row: 
    print(row[1])
    row = cursor.fetchone()
    
#close the connection
conn.close()


# In[2]:


# Netezza connection using jdbc
# using the Jaydebeapi package. On the local system, find where the JDBC drivers were installed
import sys
#!{sys.executable} -m pip install Jaydebeapi3
#!{sys.executable} -m pip install pandas


# In[ ]:


# bring in the JDBC connection lib
import Jaydebeapi, os
###jdbc:netezza://" + server + "/" + dbName ;
connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
url = '{0}:user={1};password={2}'.format(connection_string, dsn_uid, dsn_pw)

print("URL: " + url)
print("Connection String: " + connection_string)

conn = jaydebeapi.connect("org.netezza.Driver", connection_string, {'user': dsn_uid, 'password': dsn_pwd},jars = jdbc_driver_loc)

#now access the database
query1 = "select * from _v_table limit 10"
cursor = connj.cursor()
cursor.execute(query1)


# In[ ]:


# During the installation process the INZA functions were installed into a database
# Now, load some data into the existing database
# 1. restablish the connection if needed
conn=pyodbc.connect('DSN=NZATSVL;UID=admin;PWD=password') #DSN = the connection in the odbc list
cursor=conn.cursor()

# 2. (optional) drop the table if you created already, use the cursor.execute command becasue no data is returned
cursor.execute("drop table price_temp_upload IF EXISTS;")

# 3. create a table by loading external data on your local system energy_price.csv as local_csv_file included in the zip file
# note filename "r" and double back slash is needed because python uses back slash as a special char

local_csv_file = r"'E:\\NetezzaClient\\energy_price.csv'"

# build the create table external command then execute 
str="create table price_temp_upload as select * from external " + local_csv_file + " (temperature real, pressure real, humidity real, wind_speed real, precipitation real, price real, price_hour timestamp) USING ( QUOTEDVALUE 'DOUBLE' NULLVALUE '' CRINSTRING FALSE CTRLCHARS TRUE DELIMITER ','  Y2BASE 2000 ENCODING 'internal' REMOTESOURCE 'ODBC' );"
cursor.execute(str)

# close the connection
conn.close()


# In[9]:


# pull data from Netezza, look at what loaded
# note, the data is retained in netezza, no data is returned, in this case, back to the local notebook

conn=pyodbc.connect('DSN=NZATSVL;UID=admin;PWD=password') #DSN = the connection in the odbc list
cursor=conn.cursor()

cursor.execute('select * from PRICE_TEMP_UPLOAD limit 10')
print("Rows returned = ", cursor.rowcount)
rows = cursor.fetchall()
print("Hour            Temp")
for row in rows:
    print(row.PRICE_HOUR, row.TEMPERATURE)

conn.close()


# In[ ]:


# the purpose of this notebook is to demostrate using the advanced functions in netezza 
#   while keeping the data and analysis inside netezza rather than moving data back and forth between the client and server
#
# pandas is a popular py lib for analyzing data, however, very large datasets are difficlut in pandas as the data is local 
# to pull a data sample from netezza into pandas simply issue the select statement and load data into the "data frame"
# 
# first define your query, if the result set is very large, remember the limit clause
# then use the pandas read.sql function 
# the data is downloaded locally into data frame named: temp_price
# and print data from the data frame

query1 = 'select * from PRICE_TEMP_UPLOAD limit 10'
temp_price = pd.read_sql(conn, query1)
temp_price.head(12)


# In[ ]:


# the INZA functions provde data analysis / statitistical analysis functions 
# a useful function is Moments or a collection of stats about a column
# To use this capability call the procedue and store the output in a table, then query the table

conn=pyodbc.connect('DSN=NZATSVL;UID=admin;PWD=password') #DSN = the connection in the odbc list
cursor=conn.cursor()

query1 = "drop table moments if exists; CALL nza..MOMENTS('intable=PRICE_TEMP_UPLOAD, incolumn=PRICE, outtable=MOMENTS');"
cursor.execute(query1)
#moments = pd.read_sql(conn, 'select * from MOMENTS')
#moments.head()
rows = cursor.fetchall()
print ("COLUMNNAME | COUNTT |     AVERAGE     |    VARIANCE     |     STDDEV      |     SKEWNESS     |     KURTOSIS      | MINIMUM | MAXIMUM")
print(rows[1])

conn.close()


# In[ ]:


# Lets run a model
# Predict a price in the future based on weather, see the next notebook: 

