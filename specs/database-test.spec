Database/ETL Testing
=====================
Created by sriramiyengar on 12/08/2020

This is an executable specification file which follows markdown syntax.
Every heading in this file denotes a scenario. Every bulleted point denotes a step.
     
Verify Table Count macthes in Source and Target Database (Data Completeness Testing - Data Profile)
----------------
* Verify Count of Column between "select count(*) from table1;" and "select count(*) from table1;"


Verify Column Sum is correct in source and target (Data Profile Testing)
--------------------------------------------------
* Verify Sum of Columns between <SourceColumn> column from <table1> in Source and <TargetColumn> column from <table2> in Target


Verify Column Length between two tables as per requirement is same (MetaData Testing)
---------------------------------------------
* Verify if Column Size of "table1" in Source is matching with "table1" in Target


Verify if Data is Accurate (Data Accuracy Testing)
----------------------------------------------------
* Verify if Data of columns <sourceColumns> from <sourceTable> in Source is same as <targetColumns> from <targetTable> in Target


Verify the nullable contraint (Constraint Check)
--------------------------------------------------
* verify if <targetColumn> from <targetTable> in target is not nullable