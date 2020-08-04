ADXHack
=======

**Pre-Reqs:**

-   Visual Studio Hack

-   Microsoft Edge

-   CSV Files for manual ingestion

-   Review Cost Estimator

    -   <https://dataexplorer.azure.com/AzureDataExplorerCostEstimator.html>

-   Able to Access:

    -   <https://dataexplorer.azure.com/>

Environment Setup – Azure Data Explorer
---------------------------------------

1.  Create ADX Instance

<https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal>

| [./media/image1.png](./media/image1.png) |
|------------------------------------------|


Figure 1.1

![](media/f2d35f27724316d58dccf6359af97268.png)

Figure 1.2

| [./media/image3.png](./media/image3.png) |
|------------------------------------------|


Figure 1.3

<https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming>

![](media/51e9128b17c3d8a48c8794cda845b438.png)

Figure 1.4

![](media/50da04743cd51d1c2094237a9f40fb71.png)

Figure 1.5

Review + Create – This will take some time.

**Create Event Hub**

![](media/f0d5dcca885b7f549c6fc222491b5cf3.png)

![](media/ba022629e8223a7be8d9584da42825dc.png)

![](media/ee7063cef8628fc1604c14d1de77b941.png)

![](media/1b1c2281dc25b75e828e7177dd4b852c.png)

“Create”

Left Side

![](media/274944d6f153042ac5ceffb05fee29fd.png)

**Create ADX Database: chradxdb**

![](media/8f603be88039a17a2dccba762a359cdc.png)

![](media/c13e9416344aeaf3408c4b43a45e4f18.png)

**Query to Create Table**

| `.create table  TransactionEvents ( processed: datetime,  transactionType: string,  direction: string,  partner:string,  serverClusterMainNode:string,  errorResolutionType:int ,  purpose: string,  loadNum:string,  shipmentNum: string,  proNum: string )` |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|


![](media/c995d59386564f9be05481a8b7436638.png)

**Query Create Mapping**

| .create table TransactionEvents ingestion json mapping 'TransactionEventsMapping' '[{"column":"processed", "Properties": {"Path": "\$.processed"}},{"column":"transactionType", "Properties": {"Path":"\$.transactionType"}} ,{"column":"direction", "Properties": {"Path":"\$.direction"}},{"column":"partner", "Properties": {"Path":"\$.partner"}}, {"column":"serverClusterMainNode", "Properties": {"Path":"\$.serverClusterMainNode"}}, {"column":"errorResolutionType", "Properties": {"Path":"\$.errorResolutionType"}}, {"column":"purpose", "Properties": {"Path":"\$.purpose"}}, {"column":"loadNum", "Properties": {"Path":"\$.loadNum"}}, {"column":"shipmentNum", "Properties": {"Path":"\$.shipmentNum"}}, {"column":"proNum", "Properties": {"Path":"\$.proNum"}}]' |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|


**Set Permissions**

![](media/4a032fb62c702a7e55f776bc531ab0eb.png)

**Create Connection**

<https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-event-hub>

Click on database and select ‘Data ingestion’

![](media/a6c947511686bd988fe802c10ef2358e.png)

`Add a new connection`

![](media/c78e2009ecb9e308e7e50d9599119aa5.png)

![](media/b7c80f3387f8ea589d31c299cb1b034e.png)

**Setup Console Application to act as data going into Event Hub**

From cloned repo open the program.cs file and lets review & set a few
properties.

| Variable to set in repo: |
|--------------------------|
| EventHubConnectionString |
| EventHubName             |
| SourceFileLocation       |

EventHubConnectionString

![](media/c5bcf30e4a36c194b743037e37d84412.png)

![](media/a500918a098bf6832912915bbccd57aa.png)

EventHubName

SourceFileLocation

**Confirm Event Hub Connection**

**Check Count in KQL Query**

TransactionEvents

\| count

1.  KQL Commands

| Inline Ingestion (push)    | .ingest line    |   |
|----------------------------|-----------------|---|
| Ingest from Query          | .set            |   |
|                            | .append         |   |
|                            | .set-or-append  |   |
|                            | .set-or-replace |   |
| Ingest from storage (pull) | .ingest into    |   |
|                            |                 |   |
|                            |                 |   |
