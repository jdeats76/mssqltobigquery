using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace SQLToBigQuery
{
    public static class Utility
    {
        private static string _Guid;
        // abstracts configuration configuration. Fetches a string value from config.
        public static string CValueStr(string configKey)
        {
            return ConfigurationSettings.AppSettings[configKey].ToString();
        }

        // abstracts configuration. Fetches a string value from config.
        public static int CValueInt(string configKey)
        {
            return Convert.ToInt32((ConfigurationSettings.AppSettings[configKey].ToString()));
        }

        public static double CValueDouble(string configKey)
        {
            return Convert.ToDouble((ConfigurationSettings.AppSettings[configKey].ToString()));
        }

        public static string GetSessionUniqueID()
        {
            if (_Guid == null)
            {
                _Guid = Guid.NewGuid().ToString();
            }
            return _Guid;

        }

        public static void Wait(int milliseconds)
        {
            System.Threading.Thread.Sleep(milliseconds);
        }
    }
}
