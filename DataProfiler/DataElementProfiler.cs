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
            String line, currentDataElement, value, database, recordType, submissionType, term;
            Dictionary<Tuple<String, String, String, String, String, String>, float> valuePercentageMap =
                new Dictionary<Tuple<string, string, string, string, string, string>, float>();
            Dictionary<Tuple<String, String, String, String, String, String>, int> valueCountMap = 
                new Dictionary<Tuple<string, string, string, string, string, string>, int>();
            Dictionary<String, List<String>> valueLists = new Dictionary<string, List<string>>();
            Dictionary<Tuple<int, String, String, String>, float> valueStdDevMap = 
                new Dictionary<Tuple<int, string, string, string>, float>();
            Dictionary<Tuple<int, String, String, String>, float> valueMeanMap =
                new Dictionary<Tuple<int, string, string, string>, float>();
            SqlCommand comm;
            String[] columns;
            float percentage;
            int count;

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
                columns = line.Split(new char[]{','});
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
                    Tuple<String, String, String, String, String, String> key = new Tuple<string, string, string, string, string, string>
                        (database, recordType, term, submissionType, currentDataElement, value);
                    percentage = float.Parse(reader["Percentage"].ToString());
                    count = int.Parse(reader["Count"].ToString());

                    valuePercentageMap.Add(key, percentage);
                    valueCountMap.Add(key, count);
                }

                reader.Close();
            }

            file.Close();
            conn.Close();

            Tuple<String, String, String, String, String, String>[] keys = new Tuple<string, string, string, string, string, string>[valueCountMap.Keys.Count];
            valueCountMap.Keys.CopyTo(keys, 0);

            output.WriteLine("Database,Record Type,Term,Submission Type,Data Element,Value,Percentage of Submission,Count");
            
            foreach (Tuple<String,String, String,String,String,String> key in keys)
            {
                output.WriteLine(key.Item1 + "," + key.Item2 + "," + key.Item3 + "," + key.Item4 + "," + key.Item5 + ","
                    + key.Item6 + "," + valuePercentageMap[key] + "," + valueCountMap[key]);
            }

            output.Close();

            file = new StreamReader("..\\..\\..\\ValueStatistics.csv");
            output = new StreamWriter("..\\..\\..\\AggregateStatistics.csv");
                   
            List<float> summerPercentages = new List<float>();
            List<float> fallBOTPercentages = new List<float>();
            List<float> fallEOTPercentages = new List<float>();
            List<float> springBOTPercentages = new List<float>();
            List<float> springEOTPercentages = new List<float>();

            String dataElement;
            String curValue = null;

            output.WriteLine("Database,Record Type,Data Element,Value,Term,SubmissionType,Average,Standard Deviation");

            file.ReadLine();

            while (!file.EndOfStream)
            {
                line = file.ReadLine();
                columns = line.Split(new char[] { ',' });
                database = columns[0];
                recordType = columns[1];
                term = columns[2];
                submissionType = columns[3];
                currentDataElement = columns[4];
                value = columns[5];
                percentage = float.Parse(columns[6]);
                count = int.Parse(columns[7]);

                if (curValue != null && curValue != value)
                {
                    if (summerPercentages.Count != 0)
                        output.WriteLine(database + "," + recordType + "," + currentDataElement + "," + curValue + ",1,E," + summerPercentages.Average() + "," + stdDev(summerPercentages));
                    if (fallBOTPercentages.Count != 0)
                        output.WriteLine(database + "," + recordType + "," + currentDataElement + "," + curValue + ",2,B," + fallBOTPercentages.Average() + "," + stdDev(fallBOTPercentages));
                    if (fallEOTPercentages.Count != 0)
                        output.WriteLine(database + "," + recordType + "," + currentDataElement + "," + curValue + ",2,E," + fallEOTPercentages.Average() + "," + stdDev(fallEOTPercentages));
                    if (springBOTPercentages.Count != 0)
                        output.WriteLine(database + "," + recordType + "," + currentDataElement + "," + curValue + ",3,B," + springBOTPercentages.Average() + "," + stdDev(fallBOTPercentages));
                    if (springEOTPercentages.Count != 0)
                        output.WriteLine(database + "," + recordType + "," + currentDataElement + "," + curValue + ",3,E," + springEOTPercentages.Average() + "," + stdDev(springEOTPercentages));

                    summerPercentages.Clear();
                    fallBOTPercentages.Clear();
                    fallEOTPercentages.Clear();
                    springBOTPercentages.Clear();
                    springEOTPercentages.Clear();
                }

                dataElement = currentDataElement;
                curValue = value;

                if (term[0] == '1')
                {
                    summerPercentages.Add(percentage);
                }
                else if (term[0] == '2' && submissionType == "E")
                {
                    fallEOTPercentages.Add(percentage);
                }
                else if (term[0] == '2' && submissionType == "B")
                {
                    fallBOTPercentages.Add(percentage);
                }
                else if (term[0] == '3' && submissionType == "E")
                {
                    springEOTPercentages.Add(percentage);
                }
                else
                {
                    springBOTPercentages.Add(percentage);
                }
            }

            file.Close();
            output.Close();
        }

        static double stdDev(List<float> values)
        {
            double average = values.Average();

            double sumOfSquaredOfDifferences = values.Select(val => (val - average) * (val - average)).Sum();

            return Math.Sqrt(sumOfSquaredOfDifferences / values.Count);
        }
    }
}
