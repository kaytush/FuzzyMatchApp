using FuzzySharp;
using FuzzySharp.SimilarityRatio;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Numerics;

namespace FuzzyMatchingApp
{
    class Program
    {
        static void Main(string[] args)
        {
            // Database connection strings
            string connectionString = "Data Source=localhost;Initial Catalog=TrialSanctionListDemo;Persist Security Info=True;User ID=irs;Password=irs";
            string sourceTableName = "AMLWorksheet00";
            string targetTableName = "Sanction";
            string resultTableName = "MatchedRecords";

            // Establish database connection
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // Get the records from the source table
                List<Record> sourceRecords = GetRecordsFromTable(connection, sourceTableName);

                // Get the records from the target table
                List<Record> targetRecords = GetRecordsFromTable(connection, targetTableName);

                // Create a new table to store the matched records
                //CreateTable(connection, resultTableName);

                // Iterate over each record in the source table
                int matchedRecordCount = 0; // Counter variable for matched records
                // Perform the match process
                foreach (Record sourceRecord in sourceRecords)
                {
                    foreach (Record targetRecord in targetRecords)
                    {
                        // Join all the fields for source and target records
                        string sourceFullName = $"{sourceRecord.PARTY_FIRST_NAME}{sourceRecord.PARTY_MIDDLE_NAME}{sourceRecord.PARTY_LAST_NAME}";
                        string targetFullName = $"{targetRecord.PARTY_FIRST_NAME}{targetRecord.PARTY_MIDDLE_NAME}{targetRecord.PARTY_LAST_NAME}";

                        // Perform fuzzy matching on the joined full names
                        int matchScore = Fuzz.Ratio(sourceFullName, targetFullName);
                        //int matchScore = Fuzz.Ratio(targetFullName, sourceFullName);
                        Console.WriteLine($"Match score is: {matchScore}");
                        // If the match score is above a certain threshold, consider it a match
                        if (matchScore >= 30)
                        {
                            // Separate the fields for the matched records
                            string sourceFirstName = sourceRecord.PARTY_FIRST_NAME;
                            string sourceMiddleName = sourceRecord.PARTY_MIDDLE_NAME;
                            string sourceLastName = sourceRecord.PARTY_LAST_NAME;

                            string targetFirstName = targetRecord.PARTY_FIRST_NAME;
                            string targetMiddleName = targetRecord.PARTY_MIDDLE_NAME;
                            string targetLastName = targetRecord.PARTY_LAST_NAME;

                            // Insert the separated fields into the result table
                            InsertMatchedRecord(connection, resultTableName, sourceFirstName, sourceMiddleName, sourceLastName, targetFirstName, targetMiddleName, targetLastName);
                            matchedRecordCount++; // Increment the matched record count
                        }
                    }
                }
                // Display the count of targetRecords
                Console.WriteLine($"Number of records in targetRecords: {targetRecords.Count}");
                // Display the count of matched records
                Console.WriteLine($"Number of matched records: {matchedRecordCount}");

            }

            Console.WriteLine("Matching process completed successfully!");
            Console.ReadLine();
        }

        static List<Record> GetRecordsFromTable(SqlConnection connection, string tableName)
        {
            List<Record> records = new List<Record>();

            // Query to fetch records from the specified table
            string query = $"SELECT PARTY_FIRST_NAME, PARTY_MIDDLE_NAME, PARTY_LAST_NAME FROM {tableName}";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        // Create a new record object and populate its properties
                        Record record = new Record
                        {
                            PARTY_FIRST_NAME = GetNullableString(reader, 0),
                            PARTY_MIDDLE_NAME = GetNullableString(reader, 1),
                            PARTY_LAST_NAME = GetNullableString(reader, 2)
                        };

                        records.Add(record);
                    }
                }
            }

            return records;
        }

        static string GetNullableString(SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        static void CreateTable(SqlConnection connection, string tableName)
        {
            // Query to create the result table
            string query = $@"CREATE TABLE {tableName} (
                                PARTY_FIRST_NAME_1 nvarchar(255),
                                PARTY_MIDDLE_NAME_1 nvarchar(255),
                                PARTY_LAST_NAME_1 nvarchar(255),
                                PARTY_FIRST_NAME_2 nvarchar(255),
                                PARTY_MIDDLE_NAME_2 nvarchar(255),
                                PARTY_LAST_NAME_2 nvarchar(255)
                            )";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        static void InsertMatchedRecord(SqlConnection connection, string tableName, string sourceFirstName, string sourceMiddleName, string sourceLastName, string targetFirstName, string targetMiddleName, string targetLastName)
        {
            // Query to insert the matched record into the result table
            string query = $@"INSERT INTO {tableName} (PARTY_FIRST_NAME_1, PARTY_MIDDLE_NAME_1, PARTY_LAST_NAME_1, PARTY_FIRST_NAME_2, PARTY_MIDDLE_NAME_2, PARTY_LAST_NAME_2)
                      VALUES (@PartyFirstName1, @PartyMiddleName1, @PartyLastName1, @PartyFirstName2, @PartyMiddleName2, @PartyLastName2)";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                // Set parameter values
                command.Parameters.AddWithValue("@PartyFirstName1", sourceFirstName != null ? sourceFirstName : (object)DBNull.Value);
                command.Parameters.AddWithValue("@PartyMiddleName1", sourceMiddleName != null ? sourceMiddleName : (object)DBNull.Value);
                command.Parameters.AddWithValue("@PartyLastName1", sourceLastName != null ? sourceLastName : (object)DBNull.Value);
                command.Parameters.AddWithValue("@PartyFirstName2", targetFirstName != null ? targetFirstName : (object)DBNull.Value);
                command.Parameters.AddWithValue("@PartyMiddleName2", targetMiddleName != null ? targetMiddleName : (object)DBNull.Value);
                command.Parameters.AddWithValue("@PartyLastName2", targetLastName != null ? targetLastName : (object)DBNull.Value);

                command.ExecuteNonQuery();
            }
        }


    }

    class Record
    {
        public string PARTY_FIRST_NAME { get; set; }
        public string PARTY_MIDDLE_NAME { get; set; }
        public string PARTY_LAST_NAME { get; set; }
    }
}
