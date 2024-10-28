using Microsoft.Data.Sqlite;
using SQLitePCL;
using System;
using System.Data;
using System.Data.SqlClient;
namespace SqlToSqliteMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            SQLitePCL.Batteries.Init();
            // Connection strings
            string sqlServerConnectionString = "server=SQL5111.site4now.net; database=db_a66689_imedicine; Persist Security Info=True;User ID=db_a66689_imedicine_admin; Password=Root@pass1;Connection Timeout=330;";
            string sqliteConnectionString = @"Data Source=D:\New folder (2)\Angular 18\Database\IMedicine_db.db;";

            // Specify the table to migrate
            string table1 = "Brandinfo";
            string table2 = "Genericname";
            MigrateTable(sqlServerConnectionString, sqliteConnectionString, table1);
            MigrateTable(sqlServerConnectionString, sqliteConnectionString, table2);
        }

        static void MigrateTable(string sqlServerConnStr, string sqliteConnStr, string tableName)
        {
            using (SqlConnection sqlServerConnection = new SqlConnection(sqlServerConnStr))
            {
                sqlServerConnection.Open();

                // Fetch data from SQL Server
                SqlCommand sqlCommand = new SqlCommand($"SELECT * FROM {tableName}", sqlServerConnection);
                SqlDataReader reader = sqlCommand.ExecuteReader();

                using (SqliteConnection sqliteConnection = new SqliteConnection(sqliteConnStr))
                {
                    sqliteConnection.Open();

                    // Create the SQLite table if it doesn't exist
                    string createTableQuery = CreateTableSql(tableName, reader);
                    using (SqliteCommand createTableCommand = new SqliteCommand(createTableQuery, sqliteConnection))
                    {
                        createTableCommand.ExecuteNonQuery();
                    }

                    // Prepare the commands for insert and update
                    string insertCommandText = CreateInsertSql(tableName, reader);
                    string updateCommandText = CreateUpdateSql(tableName, reader);

                    while (reader.Read())
                    {
                        // Check if the record exists in SQLite
                        var parameters = new object[reader.FieldCount];
                        reader.GetValues(parameters);
                        bool exists = CheckIfExists(sqliteConnection, tableName, parameters[0]); // Assuming the first column is the key

                        if (exists)
                        {
                            // Update the record
                            using (SqliteCommand updateCommand = new SqliteCommand(updateCommandText, sqliteConnection))
                            {
                                for (int i = 0; i < parameters.Length; i++)
                                {
                                    updateCommand.Parameters.AddWithValue($"@param{i}", parameters[i] ?? DBNull.Value);
                                }
                                updateCommand.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            // Insert the record
                            using (SqliteCommand insertCommand = new SqliteCommand(insertCommandText, sqliteConnection))
                            {
                                for (int i = 0; i < parameters.Length; i++)
                                {
                                    insertCommand.Parameters.AddWithValue($"@param{i}", parameters[i] ?? DBNull.Value);
                                }
                                insertCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }

            Console.WriteLine($"Data from '{tableName}' migrated successfully!");
        }

        static bool CheckIfExists(SqliteConnection sqliteConnection, string tableName, object primaryKeyValue)
        {
            using (var checkCommand = new SqliteCommand($"SELECT COUNT(1) FROM {tableName} WHERE id = @id", sqliteConnection)) // Change 'id' to your actual primary key column name
            {
                checkCommand.Parameters.AddWithValue("@id", primaryKeyValue);
                long count = (long)checkCommand.ExecuteScalar();
                return count > 0;
            }
        }

        static string CreateTableSql(string tableName, SqlDataReader reader)
        {
            string createTable = $"CREATE TABLE IF NOT EXISTS {tableName} (";

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                createTable += $"{columnName} TEXT"; // Assuming all columns are TEXT. Modify as needed.
                if (i < reader.FieldCount - 1)
                    createTable += ", ";
            }

            createTable += ");";
            return createTable;
        }

        static string CreateInsertSql(string tableName, SqlDataReader reader)
        {
            string insertSql = $"INSERT INTO {tableName} (";
            string values = "VALUES (";

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                insertSql += $"{columnName}";
                values += $"@param{i}";

                if (i < reader.FieldCount - 1)
                {
                    insertSql += ", ";
                    values += ", ";
                }
            }

            insertSql += ") " + values + ");";
            return insertSql;
        }

        static string CreateUpdateSql(string tableName, SqlDataReader reader)
        {
            string updateSql = $"UPDATE {tableName} SET ";
            for (int i = 1; i < reader.FieldCount; i++) // Start from 1 to exclude the primary key
            {
                string columnName = reader.GetName(i);
                updateSql += $"{columnName} = @param{i}";

                if (i < reader.FieldCount - 1)
                    updateSql += ", ";
            }
            updateSql += $" WHERE id = @param0"; // Change 'id' to your actual primary key column name
            return updateSql;
        }
    }
}

