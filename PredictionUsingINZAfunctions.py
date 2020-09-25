#!/usr/bin/env python
# coding: utf-8

# In[2]:


## Netezza INZA functions
# in-databse analyics allows us to preform anaysis onvery large datasets without bring the dat aback to the cliuent
# We leverage the power of entezza to performen the analysis

#First, establish the netezza odbc conncetion, the system where python executes needs an existing ODBC connection
import pandas as pd
import pyodbc

conn=pyodbc.connect('DSN=NZATSVL;UID=admin;PWD=password') #DSN = the connection in the odbc list
cursor=conn.cursor()
cursor.execute("select * from _v_table limit 10")
row = cursor.fetchone()
while row: 
    print(row[1])
    row = cursor.fetchone()


# In[3]:


query1 = 'select * from CP4D_48CORES.ADMIN.PRICE_TEMP_UPLOAD'
TempPrice = pd.read_sql(query1, conn)
TempPrice.head()


# In[ ]:


## Analyzing the data
# first we want to understand our data, run some descriptive statisctics
# like most analysis platforms there are built in functions for univariate analysis
# the SUMMARY1000 call analyzes all fieldsin a table by default and places the output in a new table, which we then query

cursor.execute("drop table PRICE_TEMP_ANALYSIS if exists") #drop the table if it exists - keep our database clean
cursor.execute("CALL nza..SUMMARY1000('intable=ADMIN.PRICE_TEMP_UPLOAD, outtable=PRICE_TEMP_ANALYSIS');")
pd.read_sql('select * from CP4D_48CORES.ADMIN.PRICE_TEMP_ANALYSIS', conn)


# In[ ]:


## Let's find if the is a relationship between temperature and price in the data
# we are looking for covariance this is the COV function

cursor.execute("drop table PRICE_TEMP_ANALYSIS if exists")

# use the Covariance function, store results in PRICE_TEMP_ANALYSIS
cursor.execute("CALL nza..COV('intable=ADMIN.PRICE_TEMP_UPLOAD, incolumn=TEMPERATURE;PRICE,outtable=PRICE_TEMP_ANALYSIS');")
# bring the results table into the notebook - or just query it directly in Netezza
pd.read_sql('select * from CP4D_48CORES.ADMIN.PRICE_TEMP_ANALYSIS', conn)


# In[ ]:


# clean up the analysis tables
cursor.execute("drop table PRICE_TEMP_NEW if exists")
# the INZA functions usully need a unique ID for each row of data, we use the intrnal ROWID for this
cursor.execute("create table PRICE_TEMP_NEW as select *, DATE(PRICE_HOUR) as DAY, ROWID as ID from ADMIN.PRICE_TEMP_UPLOAD")
pd.read_sql('select * from PRICE_TEMP_NEW limit 10', conn)


# In[ ]:


# from the above, we see the that a negtive relation between the two columns
# now let's predict the price values based on a possible time series

# we now call a timeseries algorithm to create a model, the model name is PRICE_TIME
cursor.execute("CALL nza..TIMESERIES('model=PRICE_TIME, intable=ADMIN.PRICE_TEMP_NEW, by=DAY, time=PRICE_HOUR, target=PRICE' );")


# In[ ]:


# we can list our models here
pd.read_sql("select * from v_nza_models;",con=conn)


# In[ ]:


## model appears 
# the process store a huge amount of data about the models in metadata tables
# see chapter 22 of the document IBM_Netezza_In-Database_Analytics_Developers_Guide.pdf
cursor.execute("CALL nza..PRINT_TIMESERIES('model=PRICE_TIME, history=true');")
# and some simple charts
cursor.execute("CALL nza..PRINT_TIMESERIES('model=PRICE_TIME, history=true, series=sinus,
plot=true');")

