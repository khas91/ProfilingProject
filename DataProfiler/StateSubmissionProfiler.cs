using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using Microsoft.SqlServer.Dts.Runtime;
using System.Text.RegularExpressions;



namespace StateSubmissionProfiler
{
    class StateSubmissionProfiler
    {
        static void Main(string[] args)
        {
            
            StreamReader inputFile = new StreamReader("..\\..\\..\\EnumerableValues.csv");
            StreamWriter output = new StreamWriter("..\\..\\..\\ValueStatistics.csv");
            Dictionary<Tuple<String, String, String, String, String, String>, float> percentages = new Dictionary<Tuple<string, string, string, string, string,string>, float>();
            Dictionary<Tuple<String, String, String, String, String, String>, int> counts = new Dictionary<Tuple<string, string, string, string, string, string>, int>();
            Dictionary<Tuple<String, String>, String> dataElementDescriptions = new Dictionary<Tuple<String, String>, string>();
            Dictionary<Tuple<String, String, String>, String> valueDescriptions = new Dictionary<Tuple<string, string, string>, string>();
            List<Tuple<String, String, String, String, String, String>> values = new List<Tuple<string, string, string, string, string, string>>();
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");
            SqlDataReader reader;
            SqlCommand comm;

            try
            {
                conn.Open();
            }
            catch (System.Exception)
            {
                
                throw;
            }

            while (!inputFile.EndOfStream)
            {
                Regex columnRegEx = new Regex("(?<=^|,)(\"(?:[^\"]|\"\")*\"|[^,]*)");

                String line = inputFile.ReadLine();
                MatchCollection matches = columnRegEx.Matches(line);
                String database = matches[0].Value;
                String recordType = matches[1].Value;
                String elementNum = matches[2].Value;
                String value = matches[3].Value;
                String dataElementName = matches[4].Value;
                String valueDesc = matches[5].Value;

                Tuple<String, String> elementKey = new Tuple<string, string>(database, elementNum);
                Tuple<String, String, String> valueKey = new Tuple<string, string, string>(database, elementNum, value);

                if (!dataElementDescriptions.ContainsKey(elementKey))
                {
                    dataElementDescriptions.Add(elementKey, dataElementName);
                }

                if (!valueDescriptions.ContainsKey(valueKey))
                {
                    valueDescriptions.Add(valueKey, valueDesc);
                }

                comm = new SqlCommand("SELECT                                                                                                            "
	                                  +"      TERM                                                                                                       "
	                                  +"      ,SubmissionType                                                                                            "
	                                  +"      ,CASE                                                                                                      "
		                              +"          WHEN SUM(CASE WHEN " + elementNum + " = '" + value + "' THEN 1 ELSE 0 END) = 0 THEN 0                  "
		                              +"          ELSE CAST(SUM(CASE WHEN " + elementNum + " = '" + value + "' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(TERM)"
	                                  +"      END AS 'Percentage'                                                                                        "
                                      +"      ,SUM(CASE WHEN " + elementNum + " = '" + value + "' THEN 1 ELSE 0 END) AS 'Count'                          "
                                      +"  FROM                                                                                                           "
	                                  +"      StateSubmission.dbo." + database + "RT" + recordType                                                       
                                      +"  GROUP BY                                                                                                       "
	                                  +"      TERM,SubmissionType", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String term = reader["TERM"].ToString();
                    String submissionType = reader["SubmissionType"].ToString();
                    float percentage = float.Parse(reader["Percentage"].ToString());
                    int count = int.Parse(reader["Count"].ToString());

                    Tuple<String, String, String, String, String, String> key 
                        = new Tuple<string,string,string,string,string,string>(database, recordType, term, submissionType, elementNum, value);

                    percentages.Add(key, percentage);
                    counts.Add(key, count);
                    values.Add(key);
                }

                reader.Close();
            }

            inputFile.Close();

            output.WriteLine("Database,RecordType,Term,Submission Type,Element Number,Element Description,Value,Value Description,Percentage,Count");

            foreach (Tuple<String, String, String, String, String, String> value in values)
            {
                Tuple<String, String> elementKey = new Tuple<string, string>(value.Item1, value.Item5);
                Tuple<String, String, String> valueKey = new Tuple<string, string, string>(value.Item1, value.Item5, value.Item6);

                output.WriteLine(value.Item1 + "," + value.Item2 + "," + value.Item3 + "," + value.Item4 + "," + value.Item5 + "," + dataElementDescriptions[elementKey] + ","
                    + value.Item6 + "," + valueDescriptions[valueKey] + "," + percentages[value] + "," + counts[value]);
            }
        }
    }
}

