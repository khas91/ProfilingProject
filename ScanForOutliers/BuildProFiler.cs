
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace BuildProFiler
{
    public class BuildProFiler
    {
        public static int Main(string[] args)
        {
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");
            StreamWriter output = new StreamWriter("..\\..\\..\\Outliers.csv");
            SqlCommand comm;
            SqlDataReader reader;
            Dictionary<Tuple<String, String, String,String>, Tuple<float, float>> statsDict = new Dictionary<Tuple<string, string, string, string>, Tuple<float, float>>();
            Dictionary<Tuple<String, String, String, String>, float> outliers = new Dictionary<Tuple<string, string, string, string>, float>();
            List<Tuple<String, String, String, String>> values = new List<Tuple<string, string, string, string>>();
            String term, submissionType;

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                throw;
            }

            comm = new SqlCommand("SELECT DISTINCT TERM, Submission_Type FROM [State_Report_Data].[dbo].[SDBRT1]", conn);
            reader = comm.ExecuteReader();

            if (reader.Read())
            {
                term = reader["TERM"].ToString().Substring(0, 1);
                submissionType = reader["Submission_Type"].ToString();
            }
            else
            {
                return -1;
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                                 "
                                  + "      *                                                              "
                                  + "  FROM                                                               "
                                  + "      [State_Report_Data].[dbo].[AggregateStatisticsBySubmissionType]", conn);
            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                Tuple<String, String, String, String> key = new Tuple<string, string, string, string>(reader["Element Number"].ToString(), reader["Value"].ToString(),
                    reader["DB"].ToString(), reader["RecordType"].ToString());

                if (reader["Term"].ToString() == term && reader["Submission Type"].ToString() == submissionType)
                {
                    statsDict.Add(key, new Tuple<float, float>((float.Parse(reader["Average Percentage"].ToString()) - 2 * float.Parse(reader["Standard Dev"].ToString())) < 0
                        ? 0 : (float.Parse(reader["Average Percentage"].ToString()) - 2 * float.Parse(reader["Standard Dev"].ToString())),
                        float.Parse(reader["Average Percentage"].ToString()) + 2 * float.Parse(reader["Standard Dev"].ToString())));

                    values.Add(key);
                }
            }

            reader.Close();

            output.WriteLine("Database,Record Type,Element Number,Value,Percentage,Lower Bound,Upper Bound");

            foreach (Tuple<String, String, String, String> value in values)
            {
                comm = new SqlCommand("SELECT                                                                                                                          "
                                      + "      CAST(SUM(CASE WHEN " + value.Item1 + " = '" + value.Item2 + "' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(" + value.Item1 + ")"
                                      + " AS Percentage                                                                                                                "
                                      + "  FROM                                                                                                                        "
                                      + "      [State_Report_Data].[dbo].[" + value.Item3 + "RT" + value.Item4 + "]", conn);
                reader = comm.ExecuteReader();

                if (reader.Read())
                {
                    float percent;

                    if (!float.TryParse(reader["Percentage"].ToString(), out percent))
                    {
                        reader.Close();
                        continue;
                    }
                        

                    if (percent < statsDict[value].Item1 || percent > statsDict[value].Item2)
                    {
                        outliers.Add(value, percent);
                        output.WriteLine(value.Item3 + "," + value.Item4 + "," + value.Item1 + "," + value.Item2 + "," + percent + "," + statsDict[value].Item1 + "," + statsDict[value].Item2);
                    }
                }

                reader.Close();
            }

            output.Close();

            return 0;
                   
        }
    }
}