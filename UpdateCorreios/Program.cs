using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using Microsoft.Data.SqlClient;
using System.IO;
using Npgsql;
using VI.Anonimizator;
using UpdateCorreios;
using System.Net;
using System.Text;

class Program
{
    static void Main()
    {
        string accessFilePath = @"banco.mdb";
        string tempDb = "correios_base";
        //string sqlConnectionString = $"Data Source=.;Initial Catalog={tempDb};Integrated Security=True;TrustServerCertificate=True;";
        string sqlConnectionString = $"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog={tempDb};Integrated Security=True;TrustServerCertificate=True;";
        string csvFilePath = @"resultados.csv";
        //string pgConnectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=root;";
        string pgConnectionString = "Host=dws-hwc-accera-prd-la-south-02-03.dws.myhuaweiclouds.com;Port=8000;Database=dws_vi;Username=ironswan;Password=wzM8dFhQqPKWgDG7ZcamTk;Encoding=UTF8;";


        List<string> tablesToImport = new List<string> {
            "LOG_LOGRADOURO",
            "LOG_BAIRRO",
            "LOG_LOCALIDADE",
            "LOG_CPC",
            "LOG_GRANDE_USUARIO",
            "LOG_UNID_OPER"
        };

        //BackupTable(pgConnectionString);

        using (var oleDbConnection = new OleDbConnection($@"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={accessFilePath};"))
        {
            oleDbConnection.Open();

            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                // Cria o banco de dados
                //CreateTemporaryDatabase(sqlConnection, tempDb);

                //sqlConnection.ChangeDatabase(tempDb);

                //foreach (var table in tablesToImport)
                //{
                //    // Cria a tabela no SQL Server
                //    CreateSqlTableFromAccess(oleDbConnection, sqlConnection, table);

                //    // Importa os dados para a tabela SQL
                //    ImportData(oleDbConnection, sqlConnection, table);
                //}
                //

                var results = SelectFromView(sqlConnection);

                WriteResultsToCsv(results, csvFilePath);

                // Envia os resultados para o PostgreSQL
                SendResultsToPostgreSQL(csvFilePath, pgConnectionString);
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

    static void ImportDataToSqlServer(OleDbConnection accessConnection, SqlConnection sqlConnection, string tableName)
    {
        string selectQuery = SharedUtils.mainQuery;
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
                        reader["rua"].ToString(),
                        string.Empty
                    });
                }
            }
        }
        return results;
    }

    static void WriteResultsToCsv(List<string[]> results, string csvFilePath)
    {
        var encrypter = new PBKDF2("@ACCERA#INDIRETO$INTEGRATIONS%");

        using (StreamWriter writer = new StreamWriter(csvFilePath))
        {

            //writer.WriteLine("codigo,pais,uf,cod_cidade,cidade,bairro,rua");

            foreach (var row in results)
            {
                row[7] = (!string.IsNullOrEmpty(row[6]))?encrypter.GetHashBase64String(row[6]) : row[7];

                var line = string.Join(";", row);

                var tam = line.Split(";").Length;
                if (tam != 8)
                {
                    var teste = "FODEU";
                }

                writer.WriteLine(line.Replace("\"", ""));
            }
        }
    }

    static void BackupTable(string pgConnectionString)
    {
        string tablePgBackup = "public.dim_postal_backup";
        string tablePg = "public.dim_postal";

        using (var pgConnection = new NpgsqlConnection(pgConnectionString))
        {
            pgConnection.Open();

            using (var writer = new StreamWriter("backup.csv"))
            {
                using (var selectCommand = new NpgsqlCommand($"SELECT codigo, pais, uf, CAST(cod_cidade AS VARCHAR(255)), cidade, bairro, rua, rua_hash FROM {tablePg}", pgConnection))
                {
                    using (NpgsqlDataReader reader = selectCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var line = $"{reader["codigo"]};{reader["pais"]};{reader["uf"]};{reader["cod_cidade"]};{reader["cidade"]};{reader["bairro"]};{reader["rua"]};{reader["rua_hash"]}";
                            writer.WriteLine(line.Replace("\"","\"\""));
                        }
                    }
                }
            }

            using (var writer = pgConnection.BeginTextImport($"COPY {tablePgBackup} (codigo, pais, uf, cod_cidade, cidade, bairro, rua, rua_hash) FROM STDIN WITH CSV delimiter ';'"))
            {
                using (var reader = new StreamReader("backup.csv"))
                {
                    writer.Write(reader.ReadToEnd());
                }
            }
        }
    }

    static void SendResultsToPostgreSQL(string csv,string pgConnectionString)
    {
        string tablePg = "public.dim_postal";

        using (var pgConnection = new NpgsqlConnection(pgConnectionString))
        {
            pgConnection.Open();

            using (var cmd = new NpgsqlCommand($"TRUNCATE TABLE {tablePg}",pgConnection))
            {
                cmd.ExecuteNonQuery();
            }

            using (var writer = pgConnection.BeginTextImport($"COPY {tablePg} (codigo, pais, uf, cod_cidade, cidade, bairro, rua, rua_hash) FROM STDIN WITH CSV delimiter ';'"))
            {
                using (var reader = new StreamReader(csv))
                {
                    writer.Write(reader.ReadToEnd());
                }
            }
        }
    }
}
