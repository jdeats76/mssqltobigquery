using Google.Bigquery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLToBigQuery
{
    public class BQTypeMapItem
    {
        // PROPERTIES
        public BigqueryDbType BQColumnType { get; set; }
        public string SQLColumnType { get; set; }
        public string ColumnName { get; set; }

        // PRIVATE MEMBERS

        // METHODS
        // format item from object
        public string FormatForCSV(object value)
        {
            return FormatForCSV(value.ToString());
        }

        // format item from string
        public string FormatForCSV(string value)
        {
            // note: the "case" types below map to SQL Server data types. For Oracle, DB2, MySQL, etc...
            // some of these would need to be changed
            switch (SQLColumnType)
            {

                case "nvarchar":
                    value = "\"" + value + "\"";
                    break;
                case "varchar":
                    value = "\"" + value + "\"";
                    break;
                case "text":
                    value = "\"" + value + "\"";
                    break;
                case "char":
                    value = "\"" + value + "\"";
                    break;
                case "date":
                    value = "\"" + DateTime.Parse(value).ToString("yyyy-MM-dd HH:mm:ss") + "\"";
                    break;
                case "datetime":
                    value = "\"" + DateTime.Parse(value).ToString("yyyy-MM-dd HH:mm:ss") + "\"";
                    break;
                case "datetime2":
                    value = "\"" + DateTime.Parse(value).ToString("yyyy-MM-dd HH:mm:ss") + "\"";
                    break;
            }
            return value;
        }
    }
}
