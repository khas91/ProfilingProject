
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
            StreamWriter outlierFile = new StreamWriter("..\\..\\..\\Outliers.csv");
            StreamWriter buildProfile = new StreamWriter("..\\..\\..\\BuildProfile.csv");
            SqlCommand comm;
            SqlDataReader reader;
            Dictionary<Tuple<String, String, String, String>, Tuple<float, float, float, float>> statsDict = new Dictionary<Tuple<string, string, string, string>, Tuple<float, float, float, float>>();
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
                    float averagePercentage = float.Parse(reader["Average Percentage"].ToString());
                    float standardDev = float.Parse(reader["Standard Dev"].ToString());
                    float median = float.Parse(reader["Median"].ToString());
                    float MAD = float.Parse(reader["MedianAbsoluteDeviation"].ToString());

                    statsDict.Add(key, new Tuple<float, float, float, float>(averagePercentage, standardDev, median, MAD));
                    
                    values.Add(key);
                }
            }

            reader.Close();

            outlierFile.WriteLine("Database,Record Type,Element Number,Value,Percentage,Lower Bound,Upper Bound,Median Lower Bound,Median Upper Bound");
            buildProfile.WriteLine("Database,Record Type,Element Number,Value,Percentage,Lower Bound,Upper Bound,Median Lower Bound,Median Upper Bound");

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

                   

                    float average = statsDict[value].Item1;
                    float stdDev = statsDict[value].Item2;
                    float median = statsDict[value].Item3;
                    float MAD = statsDict[value].Item4;

                    float geometricLowerBound = (average - 2 * stdDev) < 0 ? 0 : (average - 2 * stdDev);
                    float geometricUpperBound = (average + 2 * stdDev) > 1 ? 1 : (average + 2 * stdDev);
                    float medianLowerBound = (float)((median - 5.19 * MAD) < 0 ? 0 : (median - 5.19 * MAD));
                    float medianUpperBound = (float)((median + 5.19 * MAD) > 1 ? 1 : (median + 5.19 * MAD));
                    
                    buildProfile.WriteLine(String.Join(",", value.Item3, value.Item4, value.Item1, value.Item2, percent, geometricLowerBound, geometricUpperBound, medianLowerBound, medianUpperBound));

                    if ((percent < geometricLowerBound || percent > geometricUpperBound) && (percent < medianLowerBound || percent > medianUpperBound))
                    {
                        outliers.Add(value, percent);
                        outlierFile.WriteLine(String.Join(",",value.Item3,value.Item4,value.Item1,value.Item2,percent, geometricLowerBound, geometricUpperBound, medianLowerBound, medianUpperBound));
                    }
                }

                reader.Close();
            }

            outlierFile.Close();
            buildProfile.Close();

            return 0;
                   
        }
    }
}