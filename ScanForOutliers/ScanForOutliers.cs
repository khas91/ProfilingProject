using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ScanForOutliers
{
    class ScanForOutliers
    {
        static void Main(string[] args)
        {
            StreamReader file = new StreamReader("..\\..\\..\\AggregateStatistics.csv");
            StreamWriter output = new StreamWriter("..\\..\\..\\Outliers.csv");
            SqlConnection conn = new SqlConnection("Server=vulcan;database=State_Report_Data;Trusted_Connection=yes");

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                
                throw;
            }

            SqlCommand comm = new SqlCommand("SELECT DISTINCT LEFT(DE1028, 1) AS 'Term' FROM State_Report_Data.dbo.sdb_rtype_1", conn);
            SqlDataReader reader = comm.ExecuteReader();
            reader.Read();
            String term = reader["Term"].ToString();
            reader.Close();
            comm = new SqlCommand("SELECT DISTINCT Submission_Type FROM State_Report_Data.dbo.sdb_rtype_1", conn);
            reader = comm.ExecuteReader();
            reader.Read();
            String submissionType = reader["Submission_Type"].ToString();
            reader.Close();
            String line, database, recordType, dataElement, dataElementShort, value, fileTerm, fileSubmissionType;
            String[] columns;
            
            float min_bound;
            float max_bound;
            float percentage;

            file.ReadLine();
            output.WriteLine("Database,Record Type,Data Element,Value,Current Percentage,Minimum Bound,Maximum Bound");

            while (!file.EndOfStream)
            {
                line = file.ReadLine();
                columns = line.Split(new char[] { ',' });
                database = columns[0];
                recordType = columns[1];
                dataElementShort = ((recordType == "7" || recordType == "6")
                    && database == "PDB" ? "PDB_" : "") + columns[2].Substring(0, 2) == "DE" ? columns[2].Substring(0, 6) : ("DE" + columns[2].Substring(0, 4));
                dataElementShort = (database == "APR" ? "APR_" : "") + dataElementShort;
                dataElement = columns[2];
                value = columns[3];
                fileTerm = columns[4];
                fileSubmissionType = columns[5];

                if (term != fileTerm || fileSubmissionType != submissionType)
                {
                    continue;
                }

                comm = new SqlCommand("SELECT                                                                                                                "
                                    + "       agg.[Data Element]                                                                                             "
                                    + "       ,agg.Value                                                                                                     "
                                    + "       ,CASE WHEN COUNT(" + dataElementShort + ") = 0 THEN 0                                                          "
                                    + "       ELSE CAST(SUM(CASE WHEN " + dataElementShort + " = '" + value + "' THEN 1 ELSE 0 END) AS FLOAT)                "
                                    + "       / CAST(COUNT(" + dataElementShort + ") AS FLOAT) END AS 'Current Percentage',                                  "
                                    + "       agg.Average                                                                                                    "
                                    + "       ,agg.[Standard Deviation]                                                                                      "
                                    + "       ,agg.Average - 2 * agg.[Standard Deviation] AS 'Min Bound'                                                     "
                                    + "       ,agg.Average + 2 * agg.[Standard Deviation] AS 'Max Bound'                                                     "
                                    + "   FROM                                                                                                               "
                                    + "       State_Report_Data.dbo." + database + "_rtype_" + recordType
                                    + "       INNER JOIN State_Report_Data.dbo.AggregateStatisticsByValue agg ON agg.[Data Element] = '" + dataElement + "'  "
                                    + "						                                                AND agg.SubmissionType = '" + submissionType + "'"
                                    + "						                                                AND agg.Term = '" + term + "'                    "
                                    + "						                                                AND agg.Value = '" + value + "'                  "
                                    + "   GROUP BY                                                                                                           "
                                    + "       agg.Average                                                                                                    "
                                    + "       ,agg.[Standard Deviation]                                                                                      "
                                    + "       ,agg.[Data Element]                                                                                            "
                                    + "       ,agg.Value", conn);

                reader = comm.ExecuteReader();

                if (reader.Read())
                {
                    percentage = float.Parse(reader["Current Percentage"].ToString());
                    min_bound = float.Parse(reader["Min Bound"].ToString());
                    max_bound = float.Parse(reader["Max Bound"].ToString());

                    if (percentage > max_bound || percentage < min_bound)
                    {
                        output.WriteLine(database + "," + recordType + "," + dataElementShort + "," + value + "," + percentage + "," + min_bound + "," + max_bound);
                    }

                    
                }

                reader.Close();
            }

            output.Close();
            file.Close();
        }
    }
}
