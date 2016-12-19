using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using log4net.Config;

/// <summary>
/// 
/// Application: SQLToBigQuery.exe
/// Version:     1.0
/// Date:        11-11-2016
/// Author:      Jeremy Deats
/// 
/// Purpose:     SQLToBigQuery is a Windows console application that will iterate through all rows in a specified 
///              SQL Server table and upload the data in CSV format to the specified Google Storage account. 
///              Following the upload to Google Storage, SQLTOBigQuery will iteratively import each CSV file into
///              the specified BigQuery table using mapped BigQuery to SQL Server column types inferred by analying
///              the SQL table schema.
///              
///              Optional execution parameters include specifing a date range on the SQL table data relative to the 
///              current datetime and specifing number of rows per CSV file when transforming.
///              
/// Diclaimer/License Notice: 
/// 
///             This program is free software: you can redistribute it and/or modify
///             it under the terms of the GNU General Public License as published by
///             the Free Software Foundation, either version 3 of the License, or
///             (at your option) any later version.
///
///             This program is distributed in the hope that it will be useful,
///             but WITHOUT ANY WARRANTY; without even the implied warranty of
///             MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
///             GNU General Public License for more details.

namespace SQLToBigQuery
{

    public class Program
    {
        //Declare an instance for log4net
        private static readonly ILog _Log =
              LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static DateTime _StartDate;
        private static DateTime _EndDate;
        private static bool _UseTimeRange = false;

       

        static void Main(string[] args)
        {
            // set default max row limit to 999 billion rows
            double rowLimit = 99999999999999;
            string fileNamePrefix;

            SQLToBigQueryConverter converter;

            Log("-------------------------------------------------------------------------------");
            Log("SQLToBigQuery v1.0");
            Log("-------------------------------------------------------------------------------");
            // examime args[] and condition as needed  /////////////////////////////////////////////////////////////
            if (args.Length > 0)
            {
                try
                {
                    if ((args[0] == "-help") || (args[0] == "help"))
                    {
                        Log("\r\nExample usage:");
                        Log("Query for data from last thirty days:\r\nSQLToBigQuery.exe -30 0\r\n");
                        Log("Query for data from the past year up to previous week:\r\nSQLToBigQuery.exe -365 -7\r\n");
                        Log("\r\nNote: You must specify SQL time column in app.config");
                        Log("<add key=\"SQL_TimeColumn\" value=\"your_datetime_fieldname_here\"/>\r\n");

                        Console.ReadLine();
                        return;
                    }
                    int startOffset = Convert.ToInt32(args[0]);
                    int endOffset = Convert.ToInt32(args[1]);

                    _StartDate = System.DateTime.Now.AddDays(startOffset);
                    _EndDate = System.DateTime.Now.AddDays(endOffset);
                    _UseTimeRange = true;

                    if (String.IsNullOrEmpty(Utility.CValueStr("SQL_TimeColumn")))
                    {
                        throw new Exception("SQL_TimeColumn must be specified in config when time window attributes are provided");
                    }
                } catch (Exception e)
                {
                    Log("Error parsing config", e);
                    return;
                }
            }

            // Instance SQLTtoBigQueryConverter object and set time range params if provided  ////////////////////////
            try
            {
                    converter = new SQLToBigQueryConverter(
                    Utility.CValueStr("Google_ServiceAccountEmail"),
                    Utility.CValueStr("Google_ServiceAccountCredentialFile"),
                    Utility.CValueStr("Google_ServiceAccountCredentialPassword"),
                    Utility.CValueStr("Google_CredentialFile"),
                    Utility.CValueStr("SQL_ConnStr")
                    );

                    if (_UseTimeRange)
                    {
                        converter._StartDateRange = _StartDate;
                        converter._EndDateRange = _EndDate;
                        converter._SQLTimeColumn = Utility.CValueStr("SQL_TimeColumn");
                    }

                Log("Loaded GoogleService Account and OAuth 2.0 API credentials.");

            } catch (Exception e)
            {
                Log("Failed to load Google credentials", e);
                return;
            }

            // map column types for SQL Server to BigQuery ///////////////////////////////////////////////////////////
            try
            {
                if (String.IsNullOrEmpty(Utility.CValueStr("Google_BigQueryColumnMap")))
                {
                    converter.MapColumnTypes(Utility.CValueStr("SQL_SrcTable"), ColumnMapSource.SQLTableInfer);
                }
                else
                {
                    converter.MapColumnTypes(Utility.CValueStr("Google_BigQueryColumnMap"),
                        ColumnMapSource.SpecifiedMapFromConfig);
                }
                Log("Mapped SQL Table columns to Google BigQuery columns.");

            } catch (Exception e)
            {
                Log("Failed to map SQL server to Google BigQuery column types", e);
                return;
            }

            // Upload SQL table to Google Storage ////////////////////////////////////////////////////////////////////
            try
            {
                string sqlSelectText = "";
                if (String.IsNullOrEmpty(Utility.CValueStr("SQL_SelectStatement")))
                {
                    sqlSelectText = "SELECT * FROM " + Utility.CValueStr("SQL_SrcTable");
                }
                else
                {
                    sqlSelectText = Utility.CValueStr("SQL_SelectStatement");
                }

                // if row limit specified then overwrite default
                if (!(String.IsNullOrEmpty(Utility.CValueStr("SQL_RowLimitPerFile"))))
                {
                    rowLimit = Utility.CValueDouble("SQL_RowLimitPerFile");
                }

                fileNamePrefix = Utility.CValueStr("Google_StorageFileName");
                if (Utility.CValueStr("Google_UseSessionIDForFileName") == "true")
                {
                    fileNamePrefix += Utility.GetSessionUniqueID();
                }
              
                Log("Uploading SQL table to Google Storage");
                converter.UploadTableToStorage(
                   Utility.CValueStr("Google_StorageAppName"),
                   Utility.CValueStr("Google_StorageBucket"),
                   Utility.CValueStr("SQL_SrcTable"),
                  fileNamePrefix,
                   rowLimit, true,
                   sqlSelectText,
                   Utility.CValueStr("Google_StorageDelimiter"),
                   Utility.CValueStr("Google_StorageFileExt"));
            } catch (Exception e)
            {
                Log("Failed to upload SQL table to Google Storage", e);
                return;
            }


            // if configured, create BigQuery table  /////////////////////////////////////////////////////////////////
            try
            {
                // Creating the table is optional (by config value). This is so we can use this application
                // as a scheduled job to append to an existing BigQuery table
                if (Utility.CValueStr("Google_CreateBigQueryTable") == "true")
                {
                    converter.CreateBigQueryTable(
                       Utility.CValueStr("Google_BigQueryProjectID"),
                       Utility.CValueStr("Google_BigQueryDatasetName"),
                       Utility.CValueStr("Google_BigQueryTableName"));

                    Log("Created BigQuery Table " + Utility.CValueStr("Google_BigQueryTableName"));
                }
            } catch (Exception e)
            {
                Log("Failed to create BigQuery table", e);
                return;
            }

            // Import all CSV files generated from SQL table data from Google Storage into BigQuery  //////////////////
            try
            {
                Log("Scheduling BigQuery import jobs for table " + Utility.CValueStr("Google_BigQueryTableName"));
                converter.ImportAllFilesFromStorageToBigQuery(
                      Utility.CValueStr("Google_StorageAppName"),
                      Utility.CValueStr("Google_BigQueryProjectID"),
                      Utility.CValueStr("Google_StorageBucket"),
                      fileNamePrefix,
                      Utility.CValueStr("Google_BigQueryDatasetName"),
                      Utility.CValueStr("Google_BigQueryTableName"), true,
                      Utility.CValueStr("Google_StorageFileExt"));
            } catch (Exception e)
            {
                Log("Failed to import file(s) from Google Storage to BigQuery", e);
                return;
            }

            if (Utility.CValueStr("WaitForKeyPressToExit") == "true")
            {
                Console.WriteLine("\r\nPress any key to continue");
                Console.ReadLine();
            }
      
        }
        
        // Log wrappers ///////////////////////////////////////////////////////////////////////////////////
        static void Log(string message, Exception exception)
        {
            Log(message, exception);
        }
        static void Log(string message)
        {
            Console.WriteLine(message);
            _Log.Info(message);
        }

    }
}
