using ClosedXML.Excel;
using CsvHelper;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Quartz.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MySQLSync;

public class SyncProcessor
{
    private SyncConfig config;
    public SyncProcessor(SyncConfig config)
    {
        this.config = config;
    }

    #region read and write CSV file

    /// <summary>persist the `data`(<see cref="DataTable"/> type) into CSV file</summary>
    void persistDataTableIntoCSVFile(string filePath, DataTable data)
    {
        if (File.Exists(filePath)) File.Delete(filePath);
        using var writer = new StreamWriter(filePath);
        using (var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture))
        {
            //csv.WriteRecords(data.AsEnumerable());
            var columns = data.Columns;
            // Write columns
            foreach (DataColumn column in columns)
            {
                csv.WriteField(column.ColumnName);
            }
            csv.NextRecord();

            // Write row values
            foreach (DataRow row in data.Rows)
            {
                for (var i = 0; i < columns.Count; i++)
                {
                    csv.WriteField(row[i]);
                }
                csv.NextRecord();
            }
            csv.Flush();
        }
    }

    /// <summary>read the source table as <see cref="DataTable"/> from CSV file</summary>
    DataTable readDataFromCSVFile(string filePath)
    {
        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, System.Globalization.CultureInfo.InvariantCulture);
        // do any configuration to `CsvReader` before creating CsvDataReader
        using var dr = new CsvDataReader(csv);
        DataTable dt = new DataTable();
        dt.Load(dr);
        return dt;
    }

    #endregion

    #region read and write Excel file

    /// <summary>persist the `data`(<see cref="DataTable"/> type) into excel file</summary>
    void persistDataTableIntoExcelFile(string filePath, DataTable data)
    {
        if (data != null && data.Rows.Count > 0)
        {
            using XLWorkbook wb = new XLWorkbook();
            //Add DataTable in worksheet
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            wb.Worksheets.Add(data, fileName.Substring(0, Math.Min(fileName.Length, 31)));
            wb.SaveAs(filePath);
        }
    }

    /// <summary>read the source table as <see cref="DataTable"/> from excel file</summary>
    DataTable readDataFromExcelFile(string filePath)
    {
        using XLWorkbook wb = new XLWorkbook(filePath);
        //var sheet = wb.Worksheet(0); excel index starts with 1
        var sheet = wb.Worksheet(1);

        DataTable dt = new DataTable();

        bool initialized = false;
        foreach (var row in sheet.Rows())
        {
            if (!initialized)
            {
                foreach (IXLCell cell in row.Cells())
                {
                    dt.Columns.Add(cell.Value.ToString());
                }
                initialized = true;
            }
            else
            {
                //Adding rows to DataTable.
                dt.Rows.Add();
                foreach (IXLCell cell in row.Cells())
                {
                    dt.Rows[dt.Rows.Count - 1][cell.Address.ColumnNumber - 1] = cell.Value.ToString();
                }
            }
        }

        return dt;
    }

    #endregion

    #region MySQL query

    void truncateDataTableInMySQL(string connectionString, string table)
    {
        using var connection = new MySqlConnection(connectionString);
        connection.Open();
        using MySqlCommand cmd = connection.CreateCommand();
        cmd.CommandText = $"TRUNCATE TABLE {table};";
        cmd.ExecuteNonQuery();
    }

    /// <summary>export the source table as <see cref="DataTable"/> from SQL Server</summary>
    DataTable exportDataFromMySQL(string connectionString, string table)
    {
        using var connection = new MySqlConnection(connectionString);
        connection.Open();
        using MySqlCommand cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {table};";
        using MySqlDataReader reader = cmd.ExecuteReader();
        DataTable dt = new DataTable();
        dt.Load(reader);
        return dt;
    }

    /// <summary>persist the `data`(<see cref="DataTable"/> type) into the destination (SQL Server) table</summary>
    void persistDataTableIntoMySQL(string connectionString, string table, DataTable data)
    {
        if (data == null || data.Rows.Count == 0)
            return;
        // reference: https://mysqlconnector.net/api/mysqlconnector/mysqlbulkcopytype/

        // open the connection
        string securityConfig = "AllowLoadLocalInfile=True";
        using var connection = new MySqlConnection(connectionString.Contains(securityConfig, StringComparison.OrdinalIgnoreCase) ? connectionString : $"{connectionString};{securityConfig}");
        connection.Open();

        // bulk copy the data
        var bulkCopy = new MySqlBulkCopy(connection);
        bulkCopy.DestinationTableName = table;
        var result = bulkCopy.WriteToServer(data);

        // check for problems
        if (result.Warnings.Count != 0)
        {
            /* handle potential data loss warnings */
            throw new ApplicationException("save data into MySQL failed");
        }
    }

    #endregion

    /// <summary>main business logic</summary>
    public void Process()
    {
        foreach (var table in config.TableList)
        {
            var filePath = Path.Combine(config.FileFolder, $"{table}_{DateTime.Now.ToString("yyyyMMdd")}.{config.FileExtension}");
            if (config.Mode == SyncMode.Export)
            {
                Console.WriteLine($"exporting [{table}]");
                // exporting data from source table
                DataTable data = exportDataFromMySQL(config.SourceDB, table);
                if (data == null || data.Rows.Count == 0)
                {
                    Console.WriteLine("empty source table, continue...");
                }
                else
                {
                    Console.WriteLine($"saving data into file [{filePath}]");
                    // persist the data into csv/excel file
                    if (config.FileFormat == SyncFileFormat.CSV)
                    {
                        persistDataTableIntoCSVFile(filePath, data);
                    }
                    else
                    {
                        persistDataTableIntoExcelFile(filePath, data);
                    }
                    Console.WriteLine($"Done [{table}]");
                }
            }
            else if (config.Mode == SyncMode.Import)
            {
                // importing data into destination table
                if (!File.Exists(filePath))
                {
                    Console.WriteLine($"exported file does NOT exist [{filePath}]");
                }
                else
                {
                    Console.WriteLine($"reading data from file [{filePath}]");
                    DataTable data = config.FileFormat == SyncFileFormat.CSV ? readDataFromCSVFile(filePath) : readDataFromExcelFile(filePath);
                    if (data == null || data.Rows.Count == 0)
                    {
                        Console.WriteLine("empty csv file, continue...");
                    }
                    else
                    {
                        Console.WriteLine($"importing into [{table}]");
                        // CAUTION: truncate destination table first
                        truncateDataTableInMySQL(config.DestinationDB, table);
                        // write the data into destination table
                        persistDataTableIntoMySQL(config.DestinationDB, table, data);
                        Console.WriteLine($"Done [{table}]");
                    }
                }
            }
        }
        Console.WriteLine("✔️✔️ Winner Winner, Chicken Dinner!");
        Console.WriteLine("You may close the window to exit, or leave it alone and wait for next execution.");
    }
}
