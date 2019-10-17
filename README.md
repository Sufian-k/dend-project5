<h1>Project 4: Data Lake</h1>


<h2>Project Discription</h2>

<p>
In this project, a music streaming startup, Sparkify, want to move their processes and data from data warehouse onto data lake. They require a data engineer to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

There are two datasets, song data and log data, in the S3. The song dataset continue metadata about a song and the artist of that song. The log dataset consists of log files based on the songs in the song dataset.
</p>

<h2>The Analysis Schema</h2>

<p> 
From song and log datasets you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table:</p>
<ul>
<li>songplays - records in event data associated with song plays i.e. records with page NextSong</li>
</ul>

<p>Dimension Tables:</p>
    
<ul>
<li>users - users in the app</li>
<li>songs - songs in music database</li>
<li>artists - artists in music database</li>
<li>time - timestamps of records in songplays broken down into specific units</li>
</ul>

<p>
The figure below shows the schema.
</p>

![Tux, the Linux mascot](schema.png)

<h2>Project Components</h2>

<p>
The project consists of two files:

The AWS Credential, dwh.cfg, include the required information used to connect the project to the AWS.

Python script in etl.py. incloude the ETL pipeline process from loading the data from S3, process it and create the analytics tables schema, then load the analytics tables to S3 bucket.
</p>

<h2>Running the Project</h2>

<p>
To run the project, first the cridintioal info should be added to dwh.cfg then in the script file, etl.py, the output distination S3 bucket should be assign to the data_output variable in main. When everything ready, run the etl.py script and the process wll be done automatically.
</p>
