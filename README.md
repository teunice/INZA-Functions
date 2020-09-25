# INZA-Functions
A collection of Jupyter notebooks and readme files for installing and using the INZA function in Netezza.
While the files are in python, the SQL code is embedded and can be cut and pasted into an SQL query window such as Aginity or similar tool.

This entry has the following files:

Installing_Netezza_Analytics_INZA.txt <- how to install the INZA functions, these are the first steps to using the functions

Install_and_run_Netezza_Analytics.ipynb <- Jupyter notebook with python code for INZA functions testing and installing also ->
Install_and_run_Netezza_Analytics.py same as above in python / txt format

energy_price.csv  <- Sample data for testing analysis functions

PredictionUsingINZAfunctions.ipynb <- after running the previous python code, use this for further examples of the functions
PredictionUsingINZAfunctions.py same as above in python / txt format

IBM_Netezza_In-Database_Analytics_Developers_Guide.pdf
The function reference manual

Some notes about running analysis functions in Netezza: The functions cover most aspects of data science analysis problems. While there is not a wide range of algorithms and some popular algorithms (XGBoost) are not available, the functions available are sufficient for most data science problems. The algorithms in some cases are not able to use parallel processing in Netezza, this results in lower than expected performance.
