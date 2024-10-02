using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using Microsoft.Data.SqlClient;
using System.IO;
using Npgsql;
using VI.Anonimizator;
using UpdateCorreios;

class Program
{
    static void Main()
    {
        string accessFilePath = @"banco.mdb";
        string tempDb = "correios_base";
        //string sqlConnectionString = "Data Source=.;Integrated Security=True;TrustServerCertificate=True;";
        string sqlConnectionString = $"Data Source=.;Initial Catalog={tempDb};Integrated Security=True;TrustServerCertificate=True;";
        //string csvFilePath = @"resultados.csv";
        string pgConnectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=root;";


        List<string> tablesToImport = new List<string> {
            "LOG_LOGRADOURO",
            "LOG_BAIRRO",
            "LOG_LOCALIDADE",
            "LOG_CPC",
            "LOG_GRANDE_USUARIO",
            "LOG_UNID_OPER"
        };

        using (var oleDbConnection = new OleDbConnection($@"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={accessFilePath};"))
        {
            oleDbConnection.Open();
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                // Cria o banco de dados
                //CreateTemporaryDatabase(sqlConnection, tempDb);

                //sqlConnection.ChangeDatabase(tempDb);

                foreach (var table in tablesToImport)
                {
                    // Cria a tabela no SQL Server
                    CreateSqlTableFromAccess(oleDbConnection, sqlConnection, table);

                    // Importa os dados para a tabela SQL
                    ImportData(oleDbConnection, sqlConnection, table);
                }

                var results = SelectFromView(sqlConnection);

                // Envia os resultados para o PostgreSQL
                SendResultsToPostgreSQL(results, pgConnectionString);
            }
        }
    }

    static void CreateTemporaryDatabase(SqlConnection sqlConnection, string dbName)
    {
        using (SqlCommand cmd = new SqlCommand($"CREATE DATABASE [{dbName}]", sqlConnection))
        {
            cmd.ExecuteNonQuery();
        }
    }

    static void CreateSqlTableFromAccess(OleDbConnection accessConnection, SqlConnection sqlConnection, string tableName)
    {
        // Schemma das tabelas do access
        var schemaTable = accessConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
        if (schemaTable == null) return;

        string createTableQuery = $"CREATE TABLE [{tableName}] (";

        // Pega as colunas
        using (var command = new OleDbCommand($"SELECT * FROM [{tableName}]", accessConnection))
        using (var reader = command.ExecuteReader())
        {
            DataTable schema = reader.GetSchemaTable();
            foreach (DataRow row in schema.Rows)
            {
                string columnName = row["ColumnName"].ToString();
                string dataType = row["DataType"].ToString();

                // Mapeia o tipo de dado do Access para o SQL Server
                string sqlType = MapAccessTypeToSql(dataType);
                createTableQuery += $"[{columnName}] {sqlType,-20}, ";


            }
        }

        createTableQuery = createTableQuery.TrimEnd(',', ' ') + ")";
        using (SqlCommand cmd = new SqlCommand($"USE [{sqlConnection.Database}]; {createTableQuery}", sqlConnection))
        {
            cmd.ExecuteNonQuery();
        }
    }

    static string MapAccessTypeToSql(string accessType)
    {
        return accessType switch
        {
            "System.Int32" => "INT",
            "System.String" => "NVARCHAR(255)",
            "System.DateTime" => "DATETIME",
            "System.Boolean" => "BIT",
            "System.Double" => "FLOAT",
            _ => "NVARCHAR(255)"
        };
    }

    static void ImportData(OleDbConnection accessConnection, SqlConnection sqlConnection, string tableName)
    {
        string selectQuery = $"SELECT * FROM [{tableName}]";
        using (OleDbCommand accessCommand = new OleDbCommand(selectQuery, accessConnection))
        using (OleDbDataAdapter adapter = new OleDbDataAdapter(accessCommand))
        {
            DataTable dataTable = new DataTable();
            adapter.Fill(dataTable);

            // Insere os dados na tabela SQL
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
            {
                bulkCopy.DestinationTableName = $"[{tableName}]";
                bulkCopy.WriteToServer(dataTable);
            }
        }
    }

    static List<string[]> SelectFromView(SqlConnection sqlConnection)
    {
        var results = new List<string[]>();

        using (SqlCommand cmd = new SqlCommand(SharedUtils.mainQuery, sqlConnection))
        {
            cmd.CommandTimeout = 360;
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    // Armazena os resultados em uma lista
                    results.Add(new string[]
                    {
                        reader["cep"].ToString(),
                        reader["pais"].ToString(),
                        reader["uf"].ToString(),
                        reader["cod_cidade"].ToString(),
                        reader["cidade"].ToString(),
                        reader["bairro"].ToString(),
                        reader["rua"].ToString()
                    });
                }
            }
        }
        return results;
    }

    static void WriteResultsToCsv(List<string[]> results, string csvFilePath)
    {
        using (StreamWriter writer = new StreamWriter(csvFilePath))
        {
            // Escreve o cabeçalho
            writer.WriteLine("cep,pais,uf,cod_cidade,cidade,bairro,rua");
            // Escreve os resultados
            foreach (var row in results)
            {
                writer.WriteLine(string.Join(",", row));
            }
        }
    }

    static void SendResultsToPostgreSQL(List<string[]> results, string pgConnectionString)
    {
        var encrypter = new PBKDF2("@ACCERA#INDIRETO$INTEGRATIONS%");

        string tablePg = "dim_postal";

        using (var pgConnection = new NpgsqlConnection(pgConnectionString))
        {
            pgConnection.Open();

            // Cria a tabela se não existir
            string createTableQuery = @$"
                        CREATE TABLE IF NOT EXISTS {tablePg} (
                        cep VARCHAR(20),
                        pais VARCHAR(3),
                        uf VARCHAR(2),
                        cod_cidade VARCHAR(50),
                        cidade VARCHAR(255),
                        bairro VARCHAR(255),
                        rua VARCHAR(255),
                        rua_hash VARCHAR(255)
                )";

            using (var createTableCommand = new NpgsqlCommand(createTableQuery, pgConnection))
            {
                createTableCommand.ExecuteNonQuery();
            }

            // Truncate a tabela antes de inserir os novos dados
            using (var truncateCommand = new NpgsqlCommand($"TRUNCATE TABLE {tablePg} RESTART IDENTITY", pgConnection))
            {
                truncateCommand.ExecuteNonQuery();
            }

            // Insere os dados na tabela PostgreSQL
            using (var insertCommand = new NpgsqlCommand($"INSERT INTO {tablePg} (cep, pais, uf, cod_cidade, cidade, bairro, rua, rua_hash) VALUES (@cep, @pais, @uf, @cod_cidade, @cidade, @bairro, @rua, @rua_hash)", pgConnection))
            {
                foreach (var row in results)
                {
                    insertCommand.Parameters.Clear();
                    insertCommand.Parameters.AddWithValue("@cep", row[0]);
                    insertCommand.Parameters.AddWithValue("@pais", row[1]);
                    insertCommand.Parameters.AddWithValue("@uf", row[2]);
                    insertCommand.Parameters.AddWithValue("@cod_cidade", row[3]);
                    insertCommand.Parameters.AddWithValue("@cidade", row[4]);
                    insertCommand.Parameters.AddWithValue("@bairro", row[5]);
                    insertCommand.Parameters.AddWithValue("@rua", row[6]);
                    insertCommand.Parameters.AddWithValue("@rua_hash", encrypter.GetHashBase64String(row[6]));

                    insertCommand.ExecuteNonQuery();
                }
            }
        }
    }
}
