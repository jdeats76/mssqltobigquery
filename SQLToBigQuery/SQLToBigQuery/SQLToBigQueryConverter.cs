using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Google.Apis;
using Google.Bigquery.V2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using System.IO;
using System.Data.SqlClient;

using Google.Apis.Bigquery.v2.Data;
using System.Security.Cryptography.X509Certificates;
using Google.Storage.V1;
using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;


namespace SQLToBigQuery
{
    public enum ColumnMapSource
    {
        SQLTableInfer,
        SpecifiedMapFromConfig
    }

    public class SQLToBigQueryConverter
    {
        // PROPERTIES
        public string _SQLConnectionString { get; set; }
        public string _SQLTimeColumn { get; set; }
        public DateTime _StartDateRange { get; set; }
        public DateTime _EndDateRange { get; set; }
       

        // PRIVATE VARIABLES
        private Dictionary<int, BQTypeMapItem> _DataTypeMap;
        private int _LastUploadFileCount;
        private ServiceAccountCredential _ServiceAccountCred;
        private GoogleCredential _GoogleAPICredential;

      
        // METHODS
        // Returns Google StorageClient instance
        public StorageService CreateStorageClient(string applicationName)
        {
            var credentials = _ServiceAccountCred;
            var serviceInitializer = new BaseClientService.Initializer()
            {
                ApplicationName = applicationName,
                HttpClientInitializer = credentials
            };

            return new StorageService(serviceInitializer);
        }

        // Constructor. Requires credential params
        public SQLToBigQueryConverter(string accountEmail, string certFilePath, string certFilePassword,
            string apiKeyJSONFilePath, string sqlConnectionStr)
        {
            _SQLConnectionString = sqlConnectionStr;

            // Set ServiceAccountCredential class variable. This is required for working with Google Storage API
            string serviceAccountEmail = accountEmail;
            X509Certificate2 certificate = new X509Certificate2(certFilePath, certFilePassword,
                X509KeyStorageFlags.Exportable);

            _ServiceAccountCred = new ServiceAccountCredential(
               new ServiceAccountCredential.Initializer(serviceAccountEmail)
               {
                   // Assign full control scope enumerations
                   Scopes = new[] { StorageService.Scope.DevstorageFullControl,
                   BigqueryService.Scope.Bigquery
                        }
               }.FromCertificate(certificate));

            // Set GoogleCredential class variable. This will be used for BigQuery API
            var fs = new FileStream(apiKeyJSONFilePath, FileMode.Open);
            try
            {
                _GoogleAPICredential = GoogleCredential.FromStream(fs);
            } catch (Exception e)
            {
                string mm = e.Message;
            }
            _GoogleAPICredential = _GoogleAPICredential;
        }

        // Creates a new empty table in Google BigQuery
        public void CreateBigQueryTable(string projectName, string dataSetName, string tableName)
        {
            if (_DataTypeMap == null)
            {
                throw new Exception("DataTypeMap can not be null. Call UploadTableToStorage() or MapColumnTypes() method first");
            }

            BigqueryClient client = BigqueryClient.Create(projectName, _GoogleAPICredential);

            // Build the schema with Google's schema object and our DataTypeMap
            TableSchemaBuilder sBuilder = new TableSchemaBuilder();
            for (int i = 0; i < _DataTypeMap.Keys.Count; i++)
            {
                sBuilder.Add(_DataTypeMap[i].ColumnName, _DataTypeMap[i].BQColumnType);
            }

            // Create the dataset if it doesn't exist.
            BigqueryDataset dataset = client.GetOrCreateDataset(dataSetName);
            BigqueryTable table = dataset.GetOrCreateTable(tableName, sBuilder.Build());
        }

        // Using the _LastUploadedFileCount and provided filenme infer all file names in storage 
        // Iterate through list calling ImportFileFromStorageToBigQuery for each one
        public void ImportAllFilesFromStorageToBigQuery(string applicationName, 
            string bigQueryProjectName, string bucketName,
            string fileName, string dataSetName, string tableName, bool logToConsole, string fileExt)
        {
            BigqueryClient client = BigqueryClient.Create(bigQueryProjectName, _GoogleAPICredential);
        
            for (int i = 0; i < _LastUploadFileCount; i++)
            {
                string filePath = fileName + i.ToString() + fileExt;
                int retryCount = 0;
                bool retriesDone = false;

                Utility.Wait(10000); // 10 seconds
              
                ImportFileFromStorageToBigQuery(client, bigQueryProjectName, bucketName,
                    filePath, dataSetName, tableName);
                if (logToConsole)
                {
                    Console.WriteLine("....Importing file " + filePath +
                        " to BigQuery dataset: " + dataSetName + " table:" + tableName);
                }
            }
        }

        // Checks storage bucket to verify that uploaded file has finished processing and is available.
        public bool IsUploadedFileAvailable(string applicationName, string bucket, string fileName)
        {
            bool result = false;
            StorageService storage = CreateStorageClient(applicationName);
            // test if file exist

           try
            {   
                var rlist = storage.Objects.List(bucket).Execute();
           
                foreach (var i in rlist.Items)
                {
                    if (i.Name == fileName)
                    {
                        result = true;
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                string m = e.Message;
            }


            return result;
        }

        // Import CSV file from Google Storage bucket into existing Google BigQuery data table 
        private void ImportFileFromStorageToBigQuery(BigqueryClient client, string projectName,
            string bucketName, string fileName, string dataSetName, string tableName)
        {
            StorageClient gcsClient = StorageClient.Create(_GoogleAPICredential);

            using (var stream = new MemoryStream())
            {
                gcsClient.DownloadObject(bucketName, fileName, stream);

                // This uploads data to an existing table. If the upload will create a new table
                // or if the schema in the CSV isn't identical to the schema in the table,
                // create a schema to pass into the call instead of passing in a null value.
                BigqueryJob job = null;
                try
                {
                    job = client.UploadCsv(dataSetName, tableName, null, stream);
                }
                catch (Exception e)
                {
                    string m = e.Message;
                }
                // Use the job to find out when the data has finished being inserted into the table,
                // report errors etc.

                // Wait for the job to complete.
                try
                {
                    job.Poll();
                } catch (Exception e) {
                    string m = e.Message;
                }
            }
        }

        private void WriteToGoogleStorage(StringBuilder sb, StorageService storage, string fileName, 
            int fileIndexer, string fileExt, string bucketName, bool logToConsole)
        {
            // convert StringBuilder to a stream
            var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
            string iterationFileName = fileName + fileIndexer.ToString() + fileExt;

            // stream the content into Google Storage
            storage.Objects.Insert(
                bucket: bucketName,
                stream: uploadStream,
                contentType: "text/plain",
                body: new Google.Apis.Storage.v1.Data.Object() { Name = iterationFileName }
            ).Upload();

            if (logToConsole)
            {
                Console.WriteLine("....Uploaded " + iterationFileName +
                    " to Google Storage bucket " +
                    bucketName);
            }
            
        }

        // Iterates through a stream of (all) rows in the specified SQL Server Data table
        // transforms the data to CSV format and streams the CSV to into a series of files
        // in the specified Google Storage bucket  
        public void UploadTableToStorage(string applicationName, string bucketName,
            string tableName, string fileName, double maxRowsPerFile, bool logToConsole, string sqlSelectText,
            string fieldDelimter, string fileExt)
        {
            StringBuilder sb = new StringBuilder();
            StorageService storage = CreateStorageClient(applicationName);

            if (_DataTypeMap == null)
            { 
                throw new Exception("MapColumnTypes() must be called prior to UploadTableToStorage()");
            }

            _LastUploadFileCount = 0;
            string cmdText = sqlSelectText;

            // if time range filter parameters have been provided to the class, apply filter/where clause
            if (!(String.IsNullOrEmpty(_SQLTimeColumn)))
            {
                cmdText += " WHERE " + _SQLTimeColumn + " > '"
                    + _StartDateRange.ToString("yyyy-MM-dd HH:mm:ss") + "' AND " + _SQLTimeColumn + " < '"
                    + _EndDateRange.ToString("yyyy-MM-dd HH:mm:ss") + "'";
            }

            using (var con = new SqlConnection(_SQLConnectionString))    // create SQL connection
            {
                using (var sqlCommand = new SqlCommand(cmdText, con))    // create SQL command
                {
                    con.Open();  // open SQL connection
                    double indexer = 0;
                    int fileIndexer = 0;

                    using (var reader = sqlCommand.ExecuteReader())      
                    {
                        while (reader.Read())  // iterate through SQL rows using reader (stream) object
                        {
                            for (int i = 0; i < reader.FieldCount; i++)  // iterate through all fields in a given row
                            {
                               if (i == 41)
                                {
                                    int xx = 0;
                                }
                                // apply formatting by called the FormatForCSV method in the corresponding map
                                sb.Append(_DataTypeMap[i].FormatForCSV(reader[i]));
                                if (i < reader.FieldCount - 1)
                                {
                                    sb.Append(fieldDelimter); // add delimiter for next CSV column
                                }
                            }
                            sb.Append("\r\n"); // new CSV row
                            indexer++;

                         
                            // write next file and reset indexer
                            if (indexer > maxRowsPerFile - 1)
                            {
                                WriteToGoogleStorage(sb, storage, fileName, fileIndexer, fileExt, bucketName,
                                    logToConsole);

                                sb = new StringBuilder();
                     
                                fileIndexer++;
                                _LastUploadFileCount++;
                                indexer = 0;
                            }
                        }

                        if (sb.Length > 0) // only process if we have entries
                        {
                            // write last file
                            WriteToGoogleStorage(sb, storage, fileName, fileIndexer, fileExt, bucketName,
                                      logToConsole);
                            _LastUploadFileCount++;
                        }
                    }
                }
            }
        }

   
        // Map SQL column types to BigQuery column types. The Map is used for generating CSV and also for
        // creating BigQuery table
        public void MapColumnTypes(string source, ColumnMapSource sourceType)
        {
            if (sourceType == ColumnMapSource.SpecifiedMapFromConfig)
            {
                MapColumnTypesFromConfig(source);
            }
            if (sourceType == ColumnMapSource.SQLTableInfer)
            {
                MapColumnTypes(source);
            }

        }

        // Map SQL Server column types using delimited list in colummName|Value,columnName|Value... format
        private void MapColumnTypesFromConfig(string content)
        {
            _DataTypeMap = new Dictionary<int, BQTypeMapItem>();
            string[] columns = content.Split(new char[] { ',' });
            int indexer = 0;
            foreach (string column in columns)
            {
                string[] nameTypeSplit = column.Split(new char[] { '|' });
                AddToMap(indexer, nameTypeSplit[1], nameTypeSplit[0]);
                indexer++;
            }

            return;
                
        }

        private void AddToMap(int indexer, string columnType, string columnName)
        {
            switch (columnType)
            {
                case "numeric":
                    _DataTypeMap.Add(indexer,
                        new BQTypeMapItem
                        {
                            BQColumnType = BigqueryDbType.Float,
                            ColumnName = columnName,
                            SQLColumnType = "numeric"
                        });
                    break;
                case "int":
                    _DataTypeMap.Add(indexer,
                         new BQTypeMapItem
                         {
                             BQColumnType = BigqueryDbType.Integer,
                             ColumnName = columnName,
                             SQLColumnType = "int"
                         });
                    break;
                case "decimal":
                    _DataTypeMap.Add(indexer,
                       new BQTypeMapItem
                       {
                           BQColumnType = BigqueryDbType.Float,
                           ColumnName = columnName,
                           SQLColumnType = "decimal"
                       });
                    break;
                case "nvarchar":
                    _DataTypeMap.Add(indexer,
                       new BQTypeMapItem
                       {
                           BQColumnType = BigqueryDbType.String,
                           ColumnName = columnName,
                           SQLColumnType = "nvarchar"
                       });
                    break;
                case "varchar":
                    _DataTypeMap.Add(indexer,
                      new BQTypeMapItem
                      {
                          BQColumnType = BigqueryDbType.String,
                          ColumnName = columnName,
                          SQLColumnType = "varchar"
                      });
                    break;
                case "text":
                    _DataTypeMap.Add(indexer,
                       new BQTypeMapItem
                       {
                           BQColumnType = BigqueryDbType.String,
                           ColumnName = columnName,
                           SQLColumnType = "text"
                       });
                    break;
                case "char":
                    _DataTypeMap.Add(indexer,
                        new BQTypeMapItem
                        {
                            BQColumnType = BigqueryDbType.String,
                            ColumnName = columnName,
                            SQLColumnType = "char"
                        });
                    break;
                case "date":
                    _DataTypeMap.Add(indexer,
                        new BQTypeMapItem
                        {
                            BQColumnType = BigqueryDbType.Timestamp,
                            ColumnName = columnName,
                            SQLColumnType = "date"
                        });
                    break;
                case "datetime":
                    _DataTypeMap.Add(indexer,
                        new BQTypeMapItem
                        {
                            BQColumnType = BigqueryDbType.Timestamp,
                            ColumnName = columnName,
                            SQLColumnType = "datetime"
                        });
                    break;
                case "datetime2":
                    _DataTypeMap.Add(indexer,
                        new BQTypeMapItem
                        {
                            BQColumnType = BigqueryDbType.Timestamp,
                            ColumnName = columnName,
                            SQLColumnType = "datetime2"
                        });
                    break;
            }
        }

        // Map SQL Server table column types to Google Big Query types. 
        private void MapColumnTypes(string tableName)
        {
            _DataTypeMap = new Dictionary<int, BQTypeMapItem>();
            using (var con = new SqlConnection(_SQLConnectionString))
            {
                using (var schemaCommand = new SqlCommand("SELECT column_name,data_type,character_maximum_length " +
                    " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'", con))
                {
                    con.Open();
                    int indexer = 0;
                    using (var reader = schemaCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            AddToMap(indexer, reader[1].ToString(), reader[0].ToString());
                         
                            indexer++;
                        }
                    }
                }
            }
        }
    }
}
