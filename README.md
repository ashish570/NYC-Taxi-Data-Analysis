<h1> NYC Taxi Data Analytics</h1>
<p align = "center">
<h1>Problem Statement</h1>
<ul><li> Data Location is a web url (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), Data needs to be downloaded to local environment and saved as raw files in cloud location.</li>
<li>  Data has a lot of anomalies, which needs cleanup in terms of datatype definition,duplicate purging, removal of irrelavant data etc.</li>
<li>  The cleaned data in above step needs to be saved in Parquet file format.</li>
<li>  By reading this data stored in parquet file format, insights needs to be driven which constitue of </li>
        <ul>
        <li> finding out DOLocation for highest tip, in proportion to cost of ride, per quarter.</li1>
        <li> finding out at which hour of the day the speed of taxis is highest.</li2></ul>
<li>  Above insights needs to be stored in a relational database</li>
<li>  Rest API would serve the insights to end user from the relational database. </li>
<ul>
</p>

<h1 style="background-color:powderblue;"> Solution Approach : </h1>
<p >I'm considering Microsoft Azure stack services for different compute and storage.Soulution has 4 technical pillars <p>
<ul> <li>Compute : I'm using Databricks's infrastructure for ETL and Insights on pyspark,scala and spark sql</li> 
<li> Raw Storage  : For raw layer I'm using databrick's file system(In a production scenario I'd have choosen ADLS which has a good connectivity with Azure services for ETL and building representational layer on top of the data.</li>
<li> Relational Storage : I've choosen Microsoft SQL Server on Azure for this use case , where I'm hosting my two tables for trip_speed and max_tip.</li>
<li> End User accessibility : I"m making the data available to end user through a rest api endoint hosted on my local machine.</li>
</ul>

I have used databricks community addition for most of the scrathing on dataframes building and data cleansing.
<p "style="text-align: center"> Architecture Diagram 
![architecture diagram](https://github.com/ashish570/ExactAssignment/blob/main/architecture_diagram.png)
</p>

<h1 style="background-color:powderblue;align= 'Left'"> Execution Approach : </h1>


<ul> <li> <b>Exact Assignment - ETL.ipynb</b> : is a ipynb which can be directly imported to databricks cluster and ran after installing wget library</li> 
<li> Once the data is written to rawFiles folder in DBFS , the second notebook <b>Exact Assignment - Insights.ipynb</b> can be run to perform analytics on the data.Make sure you spin a Database and setup a connection for an external SQL SERVER Database to store the query results.</li>
<li> Once the Data is ready in SQL Tables, Code Folder's <b>api.py</b> can be executed which internally calls the db_models.py and Config.py </li>
<li> On the local host the below end points can be hit , which in the backend will hit the sqlserver database and fetch the results.
        <ul><li> /api/tip/2020/01/max</li>
              <li> /api/tips/2020/max </li>
                <li>GET  /api/speed/2020/01/01/max </li></ul>
<li> Fin!</li>              
</ul>

