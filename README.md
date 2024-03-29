ADXHack
=======

Pre-Reqs:
---------

-   Visual Studio Hack

-   Microsoft Edge

-   CSV Files for manual ingestion

-   Review Cost Estimator

    -   <https://dataexplorer.azure.com/AzureDataExplorerCostEstimator.html>

-   Able to Access:

    -   <https://dataexplorer.azure.com/>
	
- Install Kusto-Explorer on machine
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/tools/kusto-explorer

Review:
---------

Azure Data Explorer Built-in ML Capabilites:
- Clustering [autocluster(), diffpatterns(), basket()]
- Regression
- Anomaly Detection
- Forecast

Extensibility:
- Inline Python plugin (preview)
- Python SDK
- Java SDK

Integration:
- KQL Magic for Jupyter
- Spark Connector (preview)
- Data Export


Azure Data Explorer integrates with Azure leveraging SDKs, managed pipelines, connections and plugins along with tools like Azure Data Factory.
An ADX Cluster has 2 main components the Data Management and the Engine enabling the queries.

![](media/Overview.PNG) 


### 1. Environment Setup – Azure Data Explorer
--------------------------------------------

1a.  Create ADX Instance

In the Azure portal, click on create resource, and search for `Azure Data Explorer`

<https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal>

| ![](media/22af5207dd7d18364d2072e08f0c3e08.png) |
|------------------------------------------------------------------------------------------|

1b. Setting up basic configuration

We can change the compute specifications later, but for now let's go with `Standard_D12_v2`

| ![](media/f2d35f27724316d58dccf6359af97268.png) |
|------------------------------------------------------------------------------------------|

1c. Setting data explorer scale

For scale, we can set its Min count to 2 and Max count to 8, ADX will autoscale based on its performance.  There is an extra credit section that has queries for monitoring the performance of the ADX Cluster.

| ![](media/6ee9566eaa3e142e4784c94c1ae60d26.png) |
|--------------------------------------------------------------------------------------------|

1d.  Set the streamining - lower latency on ingest

Use streaming ingestion to load data when you need low latency between ingestion and query. The streaming ingestion operation completes in under 10 seconds, and your data is immediately available for query after completion. This ingestion method is suitable for ingesting a high volume of data, such as thousands of records per second, spread over thousands of tables. Each table receives a relatively low volume of data, such as a few records per second.

Use bulk ingestion instead of streaming ingestion when the amount of data ingested exceeds 4 GB per hour per table.

<https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming>

| ![](media/51e9128b17c3d8a48c8794cda845b438.png) |
|------------------------------------------------------------------------------------------|


1e. Security Configuration

A managed identity from Azure Active Directory allows your cluster to easily access other AAD-protected resources such as Azure Key Vault. The identity is managed by the Azure platform and doesn't require you to provision or rotate any secrets. 

| ![](media/50da04743cd51d1c2094237a9f40fb71.png) |
|------------------------------------------------------------------------------------------|

Review + Create – This will take some time, so we can move on to creating the event hub.

### 2. Environment Setup – Create Event Hub
------------------------------------

In the Azure portal, click on create resource, and search for `Event Hub`

2a. Click on the create button
| ![](media/f0d5dcca885b7f549c6fc222491b5cf3.png) |
|------------------------------------------------------------------------------------------|

2b.  In basics select your sub, resource group, namespace, location, pricing tier

| ![](media/ba022629e8223a7be8d9584da42825dc.png) |
|------------------------------------------------------------------------------------------|

2c.  We need to create an event hub
| ![](media/ee7063cef8628fc1604c14d1de77b941.png) |
|------------------------------------------------------------------------------------------|

2d.  Give the Event hub a name `chr-events` and select `Create`
| ![](media/1b1c2281dc25b75e828e7177dd4b852c.png) |
|------------------------------------------------------------------------------------------|



2e. Over on the left side tab click on the event hubs and we should see it.

|![](media/274944d6f153042ac5ceffb05fee29fd.png) |
|------------------------------------------------------------------------------------------|

### 3. Create ADX Database: chradxdb
-----------------------------

3a.  Clicking on our instance we should be able to hit the **+ Add database** or the **Create Database** button

| ![](media/8f603be88039a17a2dccba762a359cdc.png) |
|------------------------------------------------------------------------------------------|

3b.  Provide: database name and retention period - this is for blob storage and cache

<https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/cachepolicy>

Azure Data Explorer cache provides a granular cache policy that customers can use to differentiate between: hot data cache and cold data cache. Azure Data Explorer cache attempts to keep all data that falls into the hot data cache category, in local SSD (or RAM), up to the defined size of the hot data cache. The remaining local SSD space will be used to hold data that isn't categorized as hot. One useful implication of this design is that queries that load lots of cold data from reliable storage won't evict data from the hot data cache. As a result, there won't be a major impact on queries involving the data in the hot data cache.

| ![](media/c13e9416344aeaf3408c4b43a45e4f18.png) |
|------------------------------------------------------------------------------------------|

### 4.  Query to Create Table
------------------------------------

The information coming in from the event hub will land into an ADX table.  We need to create that table and a mapping from the event hub json data to the table schema.  So first lets create a table `TransactionEvents`

```SQL
.create table  TransactionEvents ( processed: datetime,  transactionType: string,  direction: string,  partner:string,  serverClusterMainNode:string,  errorResolutionType:int ,  purpose: string,  loadNum:string,  shipmentNum: string,  proNum: string )
```

| ![media/c995d59386564f9be05481a8b7436638.png](media/c995d59386564f9be05481a8b7436638.png)|
|------------------------------------------------------------------------------------------|
### Set streaming Policy on table.

https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/streamingingestion-policy

```SQL
.alter table TransactionEvents policy streamingingestion enable
```

### 5. Query Create Mapping
------------------------------------

After the table is created, we can create the json mapping.

```SQL
.create table TransactionEvents ingestion json mapping 'TransactionEventsMapping' '[{"column":"processed", "Properties": {"Path": "\$.processed"}},{"column":"transactionType", "Properties": {"Path":"\$.transactionType"}} ,{"column":"direction", "Properties": {"Path":"\$.direction"}},{"column":"partner", "Properties": {"Path":"\$.partner"}}, {"column":"serverClusterMainNode", "Properties": {"Path":"\$.serverClusterMainNode"}}, {"column":"errorResolutionType", "Properties": {"Path":"\$.errorResolutionType"}}, {"column":"purpose", "Properties": {"Path":"\$.purpose"}}, {"column":"loadNum", "Properties": {"Path":"\$.loadNum"}}, {"column":"shipmentNum", "Properties": {"Path":"\$.shipmentNum"}}, {"column":"proNum", "Properties": {"Path":"\$.proNum"}}]'
```

### 6. Set Permissions
------------------------------------

We need to set permissions.  On the left tab we will see permssions, we can leverage Azure AD to get users to ensure everyone will have access to the resources.  You can do this by an AD group, or for each user.  Recommended for Hack to to provide users with the role `AllDatabasesAdmin`

| ![media/4a032fb62c702a7e55f776bc531ab0eb.png](media/4a032fb62c702a7e55f776bc531ab0eb.png) |
|------------------------------------------------------------------------------------------|


### 7. Create Connection to Event Hub
------------------------------------
Through the Azure Portal we can connect the Event Hub to the ADX table.

<https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-event-hub>

7a. Click on database and select `Data ingestion`

| ![media/a6c947511686bd988fe802c10ef2358e.png](media/a6c947511686bd988fe802c10ef2358e.png) |
|------------------------------------------------------------------------------------------|


7b.  Add a new connection
Here we will select the Event Hub namespace `chr-events` and Event Hub  `chr-events` that we have already created, and the default Consumer Group `$Default`, along iwth the table data format `JSON` and the Mapping `TransactionEventsMapping`

| ![media/c78e2009ecb9e308e7e50d9599119aa5.png](media/c78e2009ecb9e308e7e50d9599119aa5.png) |
|------------------------------------------------------------------------------------------|
| ![media/b7c80f3387f8ea589d31c299cb1b034e.png](media/b7c80f3387f8ea589d31c299cb1b034e.png) |

### 8. Setup Console Application to act as data going into Event Hub
------------------------------------

From cloned repo open the program.cs file and lets review & set a few
properties.

| Variable to set in repo: |
|--------------------------|
| EventHubConnectionString |
| EventHubName             |
| SourceFileLocation       |


We will need to get the connection string from the `Shared access policies` of the event hub.

| ![media/c5bcf30e4a36c194b743037e37d84412.png](media/c5bcf30e4a36c194b743037e37d84412.png) |
|------------------------------------------------------------------------------------------|


### 9. Confirm Event Hub Connection
------------------------------------

| ![media/a500918a098bf6832912915bbccd57aa.png](media/a500918a098bf6832912915bbccd57aa.png) |
|------------------------------------------------------------------------------------------|


### 10.  Update c# application with correct configuration to start sending data
------------------------------------
Here is another repo with a basic sample app <https://github.com/Azure-Samples/event-hubs-dotnet-ingest>
Lets review the console application and its configuration.


### 11.  Check Count in KQL Query
------------------------------------

```SQL
TransactionEvents
| count
```

### 12. Manually ingest batch data
We can right click on the database and select to ingest new data.


| ![media/IngestNewData.png](media/IngestNewData.png) |
|------------------------------------------------------------------------------------------|

We can then manually ingest the data & select `Edit schema` blue button

| ![media/ManualIngest01.PNG](media/ManualIngest01.PNG) |
|------------------------------------------------------------------------------------------|

We can then create a new mapping

| ![media/ManualIngest02.PNG](media/ManualIngest02.PNG) |
|------------------------------------------------------------------------------------------|

| ![media/ManualIngest03.PNG](media/ManualIngest03.PNG) |
|------------------------------------------------------------------------------------------|

### 12. Edit Mapping Import
------------------------------------

|![](media/139985c179036fa81ced07378ae20d40.png) |
|------------------------------------------------------------------------------------------|

### 13.  Install Kusto.Explorer
------------------------------------

<https://docs.microsoft.com/en-us/azure/data-explorer/kusto/tools/kusto-explorer>

Kusto Explorer will actually pick up anomolies without us doing anything

```SQL
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
let dt = 2h;
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, dt) by serverClusterMainNode
| render anomalychart with ( title='Transactions anomalies') 
```

|![](media/KustoExplorer01.PNG)|

**But let's take a step back and start going through some queries and syntax**

### 14.  In Kusto.Explorer or Edge - lets start looking at some data
------------------------------------

```SQL
TransactionEvents
| count 
```

```SQL
TransactionEvents
| take 5
```

```SQL
TransactionEvents
| summarize num=count(), min_t=min(processed), max_t=max(processed)
```


```SQL
\\Look at common patterns in the data
TransactionEvents
| evaluate autocluster()
```
If we only want to select a few columns and leverage autocluster we can project the columns
```SQL
TransactionEvents
| project processed, direction, transactionType, partner
| evaluate autocluster()
```

Using the sliding window we can get counts and distinct counts to get perspective on repeating transactions
```SQL
//sliding window metric
//https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sliding-window-counts-plugin#examples
//count for the day, distinct partners for the day, new partners for the day
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
let window= 1d;
TransactionEvents | evaluate activity_counts_metrics(partner, processed, min_t, max_t, window)
```

Start to look at time chart, start with simply looking at counts
```SQL
//Time Chart by transaction count
let min_t = toscalar(TransactionEvents | summarize min(processed));  
let max_t = toscalar(TransactionEvents | summarize max(processed));  
TransactionEvents
| make-series num=count() on processed from min_t to max_t step 1m
| render timechart with(title="Traffic Information, 1 minute resolution")
```

```SQL
//very basic time chart over 10 minute step
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents 
| make-series transactionCount=count()
 on processed
 from min_t to max_t step 10m
 |render timechart
 with (title = "Serivce exceptions over a week, 10 minute resolution")
 ```
 
Look at information by direction
 
```SQL
//based on count and binned into 1 hour - by direction
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series NumberOfEvent=count() default=0 on processed
 //1 hour bins
 from min_t to max_t step 1h
 //partition key, seperate time series for direction
 by direction
 |render timechart 
 ```
 
 
```SQL
 //Basic Time Chart by direction
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series num=count() default=0 on processed 
in range(min_t, max_t, 1m) 
by direction
| render timechart 
```

```SQL
let min_t = datetime(2020-06-19 00:00:00);
let max_t = datetime(2020-06-19 02:00:00);
let dt = 1m;
TransactionEvents
| make-series num=count() default=0 on processed from min_t to max_t step dt by direction
| extend (baseline, seasonal, trend, residual) = series_decompose(num, -1, 'linefit')
| render timechart 
with (title='traffic, decomposition', ysplit=panels)
```

```SQL
 //series_fir - will be used later.
 //fir = finite impulse response, moving averages, calc change detection
 //https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/series-firfunction
 //calculate the moving average of 5 point and normaize
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series NumberOfEvent=count() default=0
 on processed
 //1 hour bins
 from min_t to max_t step 1h
 //partition key, seperate time series for direction
 by direction
 //moving average number - column to average, filter - coeffients of filter - shape of block for moving average, NormalizeResults, centered average (10, 20, 30, 40, 50) - becomes (0, 0, 10, 20, 30)
 |extend ma_num=series_fir(NumberOfEvent, repeat(1, 5), true, true)
 |render timechart 
``` 

```SQL
//Residuals by transaction type
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, 1m) by transactionType
//series_fir - moving averages
//so we subtract from the data the moving average to get the differences
| extend ma_num=series_fir(num, repeat(1, 5), true, true)
| extend residual_num=series_subtract(num, ma_num) //to calculate residual time series
| where transactionType == "204"   // filter on Win 10 to visualize a cleaner chart 
| render timechart
```

```SQL
//Look at moving average for a partner.
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, 1h) by partner
| extend ma_num=series_fir(num, repeat(1, 5), true, true)
| where partner == "MACROPOINT"
| render timechart 
```
 

```SQL
//Look at 2 most significant decreasing trends for a server Cluster
let min_t = toscalar(TransactionEvents | summarize min(processed));  
let max_t = toscalar(TransactionEvents | summarize max(processed));  
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, 10m) by serverClusterMainNode, direction
| extend (rsquare, slope) = series_fit_line(num)
| top 2 by slope asc 
| render timechart with(title='by ServerCluster and by direction)')
```

```SQL
//Getting ready for anomoly detection
//Decomposition - take a time series and break down into components
//Seasonal
//trend
//residual - input - baseline for anomoly detection
//baseline (predicted value to be used for forecasting)
//create time series per partner.
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
let dt = 2h;
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, dt) by partner
| where partner == "MACROPOINT"
| extend (baseline, seasonal, trend, residual) = series_decompose(num, -1, 'linefit')
| render timechart 
with (title='traffic, decomposition', ysplit=panels)
```

```SQL
//fill gaps in data - with 0.
let min_t = datetime(2020-06-19 00:00:00);
let max_t = datetime(2020-06-19 02:00:00);
let dt = 1m;
TransactionEvents
| make-series num=count() default=0 on processed from min_t to max_t step dt by direction
| extend (baseline, seasonal, trend, residual) = series_decompose(num, -1, 'linefit')
| render timechart 
with (title='traffic, decomposition', ysplit=panels)
```


```SQL
//will only run in kusto.Explorer
//series_decompose_anomalies
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
let dt = 2h;
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, dt) by serverClusterMainNode
| where serverClusterMainNode == 'LIN2PR2SI3'
| render anomalychart with ( title='Transactions, anomalies')
```

<https://docs.microsoft.com/en-us/azure/data-explorer/anomaly-detection>

The function series_decompose_anomalies() finds anomalous points on a set of time series. This function calls series_decompose() to build the decomposition model and then runs series_outliers() on the residual component. series_outliers() calculates anomaly scores for each point of the residual component using Tukey's fence test. Anomaly scores above 1.5 or below -1.5 indicate a mild anomaly rise or decline respectively. Anomaly scores above 3.0 or below -3.0 indicate a strong anomaly.

```SQL
//series_decompose_anomalies
//threshold = 1.5, seasonality auto detect = -1, 
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
let dt = 2h;
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, dt) by serverClusterMainNode
| extend (anomalies, score, baseline) = series_decompose_anomalies(num, 1.5, -1, 'linefit')
| render anomalychart 
with (anomalycolumns=anomalies, title='Transactions, anomalies')
```


```SQL
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
let dt = 2h;
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, dt) by serverClusterMainNode
| extend (anomalies, score, baseline) = series_decompose_anomalies(num, 1.5, -1, 'linefit')
| render anomalychart 
with (anomalycolumns=anomalies, title='Transactions, anomalies')
```



```SQL
//fill gaps in data - with 0.
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series num=count() default=0 on processed in range(min_t, max_t, 1m) by  direction
| where direction == 'I'   //  select a single time series for a cleaner visualization
| extend (anomalies, score, baseline) = series_decompose_anomalies(num, 9.5, -1, 'linefit')
| render anomalychart with(anomalycolumns=anomalies, title='Web app. traffic of a month, anomalies') //use "| render anomalychart with anomalycolumns=anomalies" to render the anomalies as bold points on the series charts.
```



### 15. Dashboard
------------------------------------

We can create a dashboard that can be shared with your team.
Click on the `New dashboard` selection

| ![](media/Dashboard01.PNG) |
|---------------------------------------|

We will need to add a datasource

| ![](media/Dashboard02.PNG) |
|---------------------------------------|

Click on the Data sources

| ![](media/Dashboard03.PNG) |
|---------------------------------------|

Click the `+ Data source`

| ![](media/Dashboard05.PNG) |
|---------------------------------------|

Grab your cluster id and paste it in
Be sure to put in the database and give it a name

| ![](media/Dashboard06.PNG) |
|---------------------------------------|

Now we can put in some KQL
| ![](media/Dashboard07.PNG) |
|---------------------------------------|

We can hit `Apply changes`
| ![](media/Dashboard08.PNG) |
|---------------------------------------|



We can also apply Auto refresh to dashboard

| ![](media/Dashboard09.PNG) |
|---------------------------------------|

Also be sure to set `Dashboard permissions`
| ![](media/Dashboard10.PNG) |
|---------------------------------------|

| ![](media/Dashboard11.PNG) |
|---------------------------------------|



### 16. Power BI Dashboard
------------------------------------

<https://docs.microsoft.com/en-us/azure/data-explorer/power-bi-connector>

The link above shows how to create a connection - be sure to use a direct connection (Azure Data Explorer runs query)

| ![](media/PowerBI.PNG) |
|---------------------------------------|

We can setup a connection to power bi with a specific query

| ![](media/PowerBI00.PNG) |
|---------------------------------------|

```SQL
TransactionEvents
|summarize by direction, serverClusterMainNode, errorResolutionType
```

The steps below walk us through setting up the connection using a query established in Kusto Explorer



| ![](media/PowerBI01.PNG) |
|---------------------------------------|

| ![](media/PowerBI02.PNG) |
|---------------------------------------|

| ![](media/PowerBI03.PNG) |
|---------------------------------------|

| ![](media/PowerBI04.PNG) |
|---------------------------------------|

| ![](media/PowerBI05.PNG) |
|---------------------------------------|

| ![](media/PowerBI06.PNG) |
|---------------------------------------|



### 17. Things of interesting
------------------------------------

- Why did the traffic go down to 0 for these servers?  

```SQL

//Time Series Analysis - getting the biggest decline for a server.  Why is the service count gone down to 0?
let min_t = toscalar(TransactionEvents | summarize min(processed));
let max_t = toscalar(TransactionEvents | summarize max(processed));
TransactionEvents
| make-series transCount = count() on processed from min_t to max_t step 1h by direction, serverClusterMainNode, errorResolutionType
| extend series_fit_line(transCount)
| extend series_fit_2lines(transCount)
| top 2 by series_fit_2lines_transCount_left_slope asc
| project direction, serverClusterMainNode, errorResolutionType, processed, transCount
| render timechart with(title="Service Traffic for 2 instances")
```
|![](media/TimeSeriesAnalysis01.PNG) |

### 18. Extra Credit - Inline Python
------------------------------------

**Required to enable python plugin**
<https://docs.microsoft.com/en-us/azure/data-explorer/language-extensions>

|![](media/pythonplugin01.PNG) |
|---------------------------------------|

Below we will do a polynomial fit on the data leveraging numpy polyfit


|![](media/pythonplugin02.PNG) |
|---------------------------------------|

```SQL
let series_fit_poly = (tbl:(*), col: string, degree: int)
{
let kwargs = pack('col', col, 'degree', degree);
tbl
| evaluate python(typeof(*, fnum: dynamic),
	'\n'
	'col = kargs["col"]\n'
	'degree = kargs["degree"]\n'
	'\n'
	'def fit(s, deg):\n'
	'    x = np.arange(len(s))\n'
	'    coeff = np.polyfit(x, s, deg)\n'
	'    p = np.poly1d(coeff)\n'
	'    z = p(x)\n'
	'    return z\n'
	'\n'
	'result = df\n'
	'result["fnum"] = df[col].apply(fit, deg=degree)\n'
	'\n'
, kwargs)
};

let max_t = datetime(2020-06-21);
TransactionEvents
| make-series num=count() on processed from max_t-1d to max_t step 5m by serverClusterMainNode
|extend series_fit_line(num)
|invoke series_fit_poly('num', 5)
|render timechart 

```




### 19. Extra Credit - Cluster Diagnostics
------------------------------------

|![](media/ClusterDiagnostics01.png) |
```SQL

// Cluster Diagnostics
.show diagnostics
| extend Passed= (IsHealthy) and not(IsScaleOutRequired)
| extend Summary = strcat('Cluster is ', iif(Passed, '', 'NOT'), 'healthy.'),
         Details=pack('MachinesTotal', MachinesTotal, 'DiskCacheCapacity', round(ClusterDataCapacityFactor,1))
| project Action = 'Cluster Diagnostics', Category='Info', Summary, Details;

// Permission check
.show principal roles 
| where Role in ('Admin', 'Monitor')
| summarize DBs=count(), Details=make_list(Scope)
| extend Summary = iif(DBs > 0, strcat('Diagnose will examine usage of ', DBs, ' database(s).'), 'User does not have permissions to get detailed diagnostics information: requires Monitor or Admin rights.')
| project Action = 'Permission check', Category='Authorization', Summary, Details;

// CPU utilization by workload type
.show commands-and-queries
| where StartedOn > ago(24h) 
| as UsageData
// Totals
| summarize TotalCpu=sum(TotalCpu)
// Calculate top-5 principals
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by User | top 5 by TotalCpu | extend user_tuple = pack('Principal', User, 'CPU', TotalCpu, 'Count', Count))
// Top-10 heaviest queries-or-commands
| union (UsageData | top 10 by TotalCpu | project query_cpu_tuple = pack('CommandType', CommandType, 'ClientActivityId', ClientActivityId, 'TotalCpu', TotalCpu, 'Principal', Principal, 'MemoryPeak', MemoryPeak))
// Group-by operation type
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by CommandType | as hint.materialized=true Operations | extend operation_cpu_tuple = pack('CommandType', CommandType, 'CPU', TotalCpu, 'Count', Count))
| summarize TotalCpu=sum(TotalCpu), Top5Consumers=make_list(user_tuple), 
            Top10Queries = make_list(query_cpu_tuple), Operations=make_list(operation_cpu_tuple)
| extend TopOperationConsumer = toscalar(Operations | top 3 by TotalCpu | project op=strcat(CommandType, ' (', Count, '): ', round(TotalCpu/1h, 1), 'h') | summarize array_strcat(make_list(op), '\n')) 
| project Summary = strcat('CPU consumed past 24h: ', round(TotalCpu/1h, 1), ' hours.\n'
                    'Top 3 consumers:\n', TopOperationConsumer),
         Details=pack('Operations', Operations, 'Top5Consumers', Top5Consumers, 'Top10Queries', Top10Queries)
| project Action = 'CPU utilization by workload type', Category='CPU', Summary, Details;

// CPU utilization (commands)
.show commands
| where StartedOn > ago(24h) 
| extend MemoryPeak = tolong(ResourcesUtilization['MemoryPeak'])
| as UsageData
// Totals
| summarize TotalCpu=sum(TotalCpu)
// Calculate top-5 principals
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu), Operations=make_set(CommandType) by User | top 5 by TotalCpu | as hint.materialized=true Principals | extend user_tuple = pack('User', User, 'CPU', TotalCpu, 'Count', Count, 'Operations', Operations))
// Top-10 heaviest commands
| union (UsageData | top 10 by TotalCpu | project query_cpu_tuple = pack('CommandType', CommandType, 'ClientActivityId', ClientActivityId, 'TotalCpu', TotalCpu, 'Principal', Principal, 'MemoryPeak', MemoryPeak))
// Group-by operation type
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by CommandType | as hint.materialized=true Operations | extend operation_cpu_tuple = pack('CommandType', CommandType, 'CPU', TotalCpu, 'Count', Count))
| summarize TotalCpu=sum(TotalCpu), Top5Consumers=make_list(user_tuple), 
            Top10Commands = make_list(query_cpu_tuple), Operations=make_list(operation_cpu_tuple)
| extend TopOperationConsumer = toscalar(Operations | top 3 by TotalCpu | project op=strcat(CommandType, ' (', Count, '): ', round(TotalCpu/1h, 1), 'h') | summarize array_strcat(make_list(op), '\n'))
| extend TopUserConsumer = toscalar(Principals | top 3 by TotalCpu | project op=strcat(User, ' (', Count, '): ', round(TotalCpu/1h, 1), 'h') | summarize array_strcat(make_list(op), '\n'))
| project Summary = strcat('CPU consumed by commands past 24h: ', round(TotalCpu/1h, 1), ' hours.\n'
                    'Top 3 operations:\n', TopOperationConsumer, '\n',
                    'Top 3 principals:\n', TopUserConsumer, '\n'),
         Details=pack('Operations', Operations, 'Top5Consumers', Top5Consumers, 'Top10Commands', Top10Commands)
| project Action = 'CPU utilization (commands)', Category='CPU', Summary, Details;

// CPU utilization (queries)
.show queries
| where StartedOn > ago(24h) 
| as UsageData
// Totals
| summarize TotalCpu=sum(TotalCpu)
// Calculate top-5 principals
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by User | top 5 by TotalCpu | as hint.materialized=true Principals | extend user_tuple = pack('User', User, 'CPU', TotalCpu, 'Count', Count))
// Top-10 heaviest commands
| union (UsageData | top 10 by TotalCpu | project query_cpu_tuple = pack('ClientActivityId', ClientActivityId, 'TotalCpu', TotalCpu, 'Principal', coalesce(User, Principal), 'MemoryPeak', MemoryPeak))
| summarize TotalCpu=sum(TotalCpu), Top5Consumers=make_list(user_tuple), 
            Top10Queries = make_list(query_cpu_tuple)
| extend TopUserConsumer = toscalar(Principals | top 3 by TotalCpu | project op=strcat(User, ' (', Count, '): ', round(TotalCpu/1h, 1), 'h') | summarize array_strcat(make_list(op), '\n'))
| project Summary = strcat('CPU consumed by queries past 24h: ', round(TotalCpu/1h, 1), ' hours.\n'
                    'Top 3 principals:\n', TopUserConsumer, '\n'),
         Details=pack('Top5Consumers', Top5Consumers, 'Top10Queries', Top10Queries)
| project Action = 'CPU utilization (queries)', Category='CPU', Summary, Details;

// Concurrency of commands
.show commands
| where StartedOn > ago(24h) 
| extend User=coalesce(User, Principal)
| as UsageData
// Totals
| summarize Total=count()
// Calculate top-5 principals by count
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by User, CommandType | top 5 by Count | as hint.materialized=true Principals | extend user_tuple = pack('User', User, 'CPU', TotalCpu, 'Count', Count, 'CommandType', CommandType))
// Detect spikes
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by User, CommandType, bin(StartedOn, 1min) | top 10 by Count | as hint.materialized=true Spikes | extend spike_tuple = pack('User', User, 'CPU', TotalCpu, 'Count', Count, 'StartedOn', StartedOn, 'CommandType', CommandType))
| summarize Total=sum(Total), Top5Consumers=make_list(user_tuple), 
            Top10Spikes = make_list(spike_tuple)
| extend TopUserConsumer = toscalar(Principals | top 3 by TotalCpu | project op=strcat(User, ' (', Count, '): ', round(TotalCpu/1h, 1), 'h CPU-time') | summarize array_strcat(make_list(op), '\n'))
| extend TopSpikes = toscalar(Spikes | top 3 by Count | project op=strcat(User, ' (', Count, ' ', CommandType, ') at ', format_datetime(StartedOn, 'yyyy-MM-dd HH:mm')) | summarize array_strcat(make_list(op), '\n'))
| project Summary = strcat('Commands run past 24 hours: ', Total, '.\n',
                    'Top 3 principals by count of commands:\n', TopUserConsumer, '\n',
                    'Top 3 command spikes (1min buckets):\n', TopSpikes),
         Details=pack('Top5Consumers', Top5Consumers, 'Top10Spikes', Top10Spikes)
| project Action = 'Concurrency of commands', Category='Concurrency', Summary, Details;

// Concurrency of queries
.show queries
| where StartedOn > ago(24h) 
| extend User=coalesce(User, Principal)
| as UsageData
// Totals
| summarize Total=count()
// Calculate top-5 principals by count
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by User | top 5 by Count | as hint.materialized=true Principals | extend user_tuple = pack('User', User, 'CPU', TotalCpu, 'Count', Count))
// Detect spikes
| union (UsageData | summarize Count=count(), TotalCpu=sum(TotalCpu) by User, bin(StartedOn, 1min) | top 10 by Count | as hint.materialized=true Spikes | extend spike_tuple = pack('User', User, 'CPU', TotalCpu, 'Count', Count, 'StartedOn', StartedOn))
| summarize Total=sum(Total), Top5Consumers=make_list(user_tuple), 
            Top10Spikes = make_list(spike_tuple)
| extend TopUserConsumer = toscalar(Principals | top 3 by TotalCpu | project op=strcat(User, ' (', Count, '): ', round(TotalCpu/1h, 1), 'h CPU-time') | summarize array_strcat(make_list(op), '\n'))
| extend TopSpikes = toscalar(Spikes | top 3 by Count | project op=strcat(User, ' (', Count, ') at ', format_datetime(StartedOn, 'yyyy-MM-dd HH:mm')) | summarize array_strcat(make_list(op), '\n')) 
| project Summary = strcat('Queries run past 24 hours: ', Total, '.\n',
                    'Top 3 principals by count of queries:\n', TopUserConsumer, '\n',
                    'Top 3 query spikes (1min buckets):\n', TopSpikes),
         Details=pack('Top5Consumers', Top5Consumers, 'Top10Spikes', Top10Spikes)  
| project Action = 'Concurrency of queries', Category='Concurrency', Summary, Details;

// Ingestion failures
.show ingestion failures
| where FailedOn > ago(1d)
| where FailureKind == 'Permanent'
| as T
| summarize FailedOperations=dcount(OperationId) by ErrorCode
| extend tuple = pack(ErrorCode, FailedOperations)
| union (T | summarize TotalFailures = dcount(OperationId))
| summarize TotalFailures=sum(TotalFailures), Failures=make_dictionary(tuple) 
| project Summary=strcat('Failed ingestions past 24h: ', TotalFailures), Details=Failures
| project Action = 'Ingestion failures', Category='Ingestion', Summary, Details;

```
