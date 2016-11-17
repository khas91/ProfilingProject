using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataProfiler
{
    class DataElementProfiler
    {
        static void Main(string[] args)
        {
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");
            SqlDataReader reader;
            StreamReader file = new StreamReader("..\\..\\..\\EnumerableValues.csv");
            StreamWriter output = new StreamWriter("..\\..\\..\\ValueStatistics.csv");
            String line, currentDataElement, value, database, recordType, submissionType;
            Dictionary<Tuple<String, String, String, String, String>, float> valuePercentageMap =
                new Dictionary<Tuple<string, string, string, string, string>, float>();
            Dictionary<Tuple<String, String, String, String, String>, int> valueCountMap = 
                new Dictionary<Tuple<string, string, string, string, string>, int>();
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
                int count;
                database = columns[0];
                recordType = columns[1];
                currentDataElement = columns[2];
                value = columns[3];

                if (!valueLists.ContainsKey(currentDataElement))
                {
                    valueLists.Add(currentDataElement, new List<string>());
                }

                valueLists[currentDataElement].Add(value);
                 
                comm = new SqlCommand("SELECT                                                                                                         "
	                                  +"     sdb.[" + (recordType == "1" ? "TERM-ID" : "DE1028") + "]                                                 "
                                      +"     ,sdb.SubmissionType                                                                                      "
	                                  +"     ,SUM(CASE WHEN sdb.[" + currentDataElement + "] = '" + value + "' THEN 1 ELSE 0 END) AS 'Count'          "
                                      +"     ,CASE WHEN COUNT(sdb.[" + currentDataElement + "]) = 0 THEN 0                                            "
                                      +"     ELSE CAST(SUM(CASE WHEN sdb.[" + currentDataElement + "] = '" + value + "' THEN 1 ELSE 0 END) AS FLOAT)  "
                                      +"  / CAST(COUNT(sdb.[" + currentDataElement + "]) AS FLOAT) END AS 'Percentage'                                "
                                      +" FROM                                                                                                         "
	                                  +"     StateSubmission.[" + database + "].[RecordType" + recordType + "] sdb                                    "
                                      +" GROUP BY                                                                                                     "
                                      +"     sdb.[" + (recordType == "1" ? "TERM-ID" : "DE1028") + "]                                                "
                                      +"     ,sdb.SubmissionType", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    term = reader[(recordType == "1" ? "TERM-ID" : "DE1028")].ToString();
                    submissionType = reader["SubmissionType"].ToString();
                    Tuple<String, String, String, String, String> key = new Tuple<string, string, string, string, string>
                        (database, term, submissionType, currentDataElement, value);
                    percentage = float.Parse(reader["Percentage"].ToString());
                    count = int.Parse(reader["Count"].ToString());

                    valuePercentageMap.Add(key, percentage);
                    valueCountMap.Add(key, count);
                }

                reader.Close();
            }

            file.Close();
            conn.Close();

            Tuple<String, String, String, String, String>[] keys = new Tuple<string, string, string, string, string>[valueCountMap.Keys.Count];
            valueCountMap.Keys.CopyTo(keys, 0);

            foreach (Tuple<String,String,String,String,String> key in keys)
            {
                output.WriteLine(key.Item1 + "," + key.Item2 + "," + key.Item3 + "," + key.Item4 + "," + key.Item5 + ","
                    + valuePercentageMap[key] + "," + valueCountMap[key]);
            }

            output.Close();
        }
    }
}
