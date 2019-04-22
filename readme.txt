Trevor Maxfield     maxfit
Greg Stewart        stewag
Eric Xu             xue 
Luis Garcia         garcil2

Final Project Deliverable

See it on github at:
https://github.com/boiled-greggs/chsi_usda_database/tree/master

REQUIREMENTS:
The following packages must be installed (all via pip or pip3):
    DATALOADING:
        pandas
        xlrd
    APPLICATION:
        numpy
        matplotlib
        plotly 
        pip install geopandas==0.3.0
        # Also try 'conda install -c conda-forge geopandas'
        pip install pyshp==1.2.10

        pip install shapely==1.6.3 - might fail, check via
        >>>import shapely
        >>>shapely.__version__
        #should be 1.6.3 or greater	



ALSO: if geopandas isn't working with pip try using conda (if available)

There was a notice about weird things happening with geopandas on Windows and/or
conda installations of python.  If creating choropleth maps of the US 
(option 7 from inside the application) is disallowed because plotly won't import,
see the following resources or the linked dropbox for examples of the output
choroplaths:

https://plot.ly/python/county-choropleth/#fips-and-values
https://geoffboeing.com/2014/09/using-geopandas-windows/

If it doesn't work at all, included is an png file that shows what those maps create.
The user can pick any attribute over the data and application creates a colored heat
map of the US according to the values returned from querying that attribute in the database.  

If plotly cannot be imported the program disallows the query to create the Choropleth maps
of the US and an error message is printed.  The user can still use the rest of the application.

---GRADER: If plotly/geopandas isn't importing easily from pip or conda, the link below hosts a folder 
of 29 example choropleths created by the application on our machines. Some of them are pretty interesting.
Download a given html file and open it in a webbrowser to see the choropleth.

https://www.dropbox.com/sh/h7xicxf77shz0mq/AAAcJXVfG_U-Y2XnM6vJFovBa?dl=0




To initialize the database run the following code (found in db_setup.sql) as a psql admin:

DROP DATABASE IF EXISTS chsi_usda_ers;
CREATE DATABASE chsi_usda_ers;
DROP USER IF EXISTS chsi;
CREATE USER chsi WITH password 'chsi';
GRANT ALL PRIVILEGES ON DATABASE chsi_usda_ers TO chsi; 



load_data.py (and therefore database.py) expects that the database and user exist, then 
create_database.sql is run from the python code to drop/create the relations.

load_data.py and database.py ASSUME that all files from datasets.txt are downloaded to the
current working directory.  
