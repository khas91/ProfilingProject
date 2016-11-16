using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataProfiler
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");
            SqlDataReader reader;
            StreamReader file = new StreamReader("..\\..\\..\\EnumerableValues.csv");
            String line, currentDataElement, value;
            Dictionary<Tuple<String, String, String>, float> valuePercentageMap = new Dictionary<Tuple<string, string, string>, float>();
            Dictionary<String, List<String>> valueLists = new Dictionary<string, List<string>>();
            SqlCommand comm;

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                
                throw;
            }

            while (!file.EndOfStream)
            {
                line = file.ReadLine();
                String[] columns = line.Split(new char[]{','});
                String term;
                float percentage;
                currentDataElement = columns[0];
                value = columns[1];
                 
                comm = new SqlCommand("SELECT                                                                                                         "
	                                  +"     sdb.[TERM-ID]                                                                                            "
	                                  +"     ,SUM(CASE WHEN sdb.[" + currentDataElement + "] = '" + value + "' THEN 1 ELSE 0 END) AS 'Count'          "
                                      + "     ,CASE WHEN COUNT(sdb.[" + currentDataElement + "]) = 0 THEN 0                                           "
                                      + "     ELSE CAST(SUM(CASE WHEN sdb.[" + currentDataElement + "] = '" + value + "' THEN 1 ELSE 0 END) AS FLOAT) "
                                      + " / CAST(COUNT(sdb.[" + currentDataElement + "]) AS FLOAT) END AS 'Percentage'                                "
                                      +" FROM                                                                                                         "
	                                  +"     StateSubmission.[dbo].[SDB_RType1_Fall_EOT] sdb                                                          "
                                      +" GROUP BY                                                                                                     "
	                                  +"     sdb.[TERM-ID]", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    term = reader["TERM-ID"].ToString();
                    Tuple<String, String, String> key = new Tuple<string, string, string>(term, currentDataElement, value);
                    percentage = float.Parse(reader["Percentage"].ToString());

                    valuePercentageMap.Add(key, percentage);
                }

                reader.Close();
            }
            ;
        }
    }
}
