# mssqltobigquery

Description:
mssqltobigquery is a command line tool for Windows that can be used to bulk load or for scheduled execution from one or more MSSQL or SQL Azure tables and ingest the data into BigQuery via Google Storage.

How it Works:
Using Google's wraper APIs for .NET combined with Microsoft's high perfomance SQL drivers for .NET, MSSQLToBigQuery executes the specified SQL Select command and via DataReader iterates over the returned rowset generating an export in CSV or JSON format spanning one or more files (see app.config). The resulting file(s) are then uploaded to Google Storage and finally bulk inserted into BigQuery


Q&A 

Q:Why go through Google Storage?
A:Although Google is now offering row by row insert functionally on the BigQuery API, they still list this as experimental. Also the transaction fees are much higher inserting row by row direct to BigQuery. 

Q:Can MSSQLTOBigQuery be used with Windows scheduler or other scheduler appliations
A:Yes. See command line arguments in Program.cs

Q: Will this run on .NET Core on Linux?
A: No. Some of the functionality utilized required me to make the full .NET Framework a dependency

Q: Will this run on Mono?
A: I haven't tried, but I imagine a dependncy would be missing on Mono given the third-party library stack utilized... If you try it and it works please let me know so I can update this answer.

Q: Will you be adding any new features?
A: I have no plans to enhance mssqltobigquery at this time or in the forseeable future. It was developed to help me solve a speicfic problem and to get my head around the Google APIs for .NET


