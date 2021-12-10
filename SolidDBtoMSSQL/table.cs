using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Data;

namespace SolidDBtoMSSQL
{
    /// <summary>
    /// Table is used to store information about each table to be copied including conversion info between SolidDB and MSSQL
    /// </summary>
    public class table
    {
        private Dictionary<string, string[]> conversionTable = new Dictionary<string, string[]>() {
            {"BIGINT", new string[]{"BIGINT", "System.Int32"}  },
            {"BINARY", new string[]{"BINARY(50)", "System.Int32" } },
            {"CHAR", new string[]{"CHAR(10)", "System.String" } },
            {"DATE", new string[]{"DATE", "System.DateTime" } },
            {"DECIMAL", new string[]{"FLOAT", "System.Double" } },
            {"DOUBLE PRECISION", new string[]{"FLOAT", "System.Double" } },
            {"INTEGER", new string[]{"INT", "System.Int32" } },
            {"LONG VARBINARY", new string[]{"VARBINARY(MAX)", "System.String" } },
            {"LONG VARCHAR", new string[]{"VARCHAR(MAX)", "System.String" } },
            {"LONG WVARCHAR", new string[]{"WVARCHAR(MAX)", "System.String" } },
            {"NUMERIC", new string[]{"INT", "System.Int32" } },
            {"REAL", new string[]{"INT", "System.Int32" } },
            {"SMALLINT", new string[]{"INT", "System.Int32" } },
            {"TIME", new string[]{"TIME(7)", "System.String" } },
            {"TIMESTAMP", new string[]{"DATETIME", "System.DateTime" } },
            {"VARBINARY", new string[]{"VARBINARY(MAX)", "System.String" } },
            {"VARCHAR", new string[]{"VARCHAR(MAX)", "System.String" } },
            {"WCHAR", new string[]{"WVARCHAR(MAX)", "System.String" } },
            {"WVARCHAR", new string[]{"WVARCHAR(MAX)", "System.String" } }
        };
        public string name { get; set; }
        public bool pk { get; set; }
        public List<string> keys { get; set; }
        public List<string> columns = new List<string>();
        public List<string> datatypes = new List<string>();
        public List<string> olddatatypes = new List<string>();
        public string pkName { get; set; }
        public string dateString { get; set; }
        public DataTable returnedData { get; set; }
        
        /// <summary>
        /// Primary constructor for table class.
        /// </summary>
        /// <param name="n">Name of the table</param>
        /// <param name="p">Bool of whether there is a single primary key, or a combo</param>
        /// <param name="k">List of the primary keys. (List of single item if one key)</param>
        /// <param name="d">Date string to query this table. (Can be blank if not applicable)</param>
        public table(string n, bool p, List<string> k, string d, Dictionary<string,string> config)
        {
            // Set up my variables.
            this.name = n;
            this.pk = p;
            this.keys = k;
            this.dateString = d;

            // Set name of primary Key
            if(pk == true)
            {
                this.pkName = keys[0];
            }
            else
            {
                foreach (string item in keys){
                    pkName += item + '_';
                }
                this.pkName = pkName.TrimEnd('_');
            }

            // Get the columns
            OdbcConnection odbc = new OdbcConnection($"DSN={config["SOLIDDSN"]};UID={config["SOLIDUID"]};pwd={config["SOLIDPWD"]};");
            OdbcCommand getColumns = new OdbcCommand($"SELECT COLUMN_NAME, DATA_TYPE FROM _SYSTEM.COLUMNS where TABLE_NAME = '{n}'", odbc);
            odbc.Open();
            using (OdbcDataReader reader = getColumns.ExecuteReader())
            {
                while (reader.Read())
                {
                    this.columns.Add(reader.GetValue(0).ToString());
                    this.datatypes.Add(conversionTable[reader.GetValue(1).ToString()][0]);
                    this.olddatatypes.Add(reader.GetValue(1).ToString());
                }
                reader.Close();
            }
            odbc.Close();


        }

        /// <summary>
        /// Does the table already exist? Returns bool
        /// </summary>
        /// <param name="config">Config, passed from Service1.cs</param>
        public bool tableExists(Dictionary<string, string> config)
        {
            SqlConnection exconn = new SqlConnection($"server={config["MSSQLServer"]};uid={config["MSSQLUid"]};pwd={config["MSSQLPwd"]};database={config["MSSQLDatabase"]}");
            SqlCommand excomm = new SqlCommand($"SELECT 1 from dbo.{this.name}", exconn);
            exconn.Open();
            try
            {
                SqlDataReader reader = excomm.ExecuteReader();
                reader.Read();
                exconn.Close();
                return true;
            }
            catch (Exception e)
            {
                exconn.Close();
                return false;
            }
        }

        /// <summary>
        /// If the table didn't exist, make the tables
        /// </summary>
        /// <param name="config">Config, passed from Service1.cs</param>
        public void makeTable(Dictionary<string, string> config)
        {
            string createString = "";
            if (!pk) { createString += $"{pkName} varchar(900) PRIMARY KEY,"; }
            for (int i = 0; i < columns.Count(); i++)
            {
                createString += $"{columns[i]} {datatypes[i]} NULL,";
            }
            createString = createString.TrimEnd(',');
            SqlConnection mkconn = new SqlConnection($"server={config["MSSQLServer"]};uid={config["MSSQLUid"]};pwd={config["MSSQLPwd"]};database={config["MSSQLDatabase"]}");
            SqlCommand mkcomm = new SqlCommand($"CREATE TABLE dbo.{this.name} ( {createString} )", mkconn);
            SqlCommand mktmpcomm = new SqlCommand($"CREATE TABLE dbo.[{this.name+"_TMP"}] ( {createString} )", mkconn);
            SqlCommand makeTrigger = new SqlCommand($"create trigger {this.name}trgr on dbo.{this.name}_TMP " +
                $"after insert " +
                $"as " +
                $"begin " +
                $"set nocount on; " +
                $"DELETE from {this.name} where exists (SELECT * FROM dbo.{this.name}_TMP where {this.name}.{this.pkName} = {this.name}_TMP.{this.pkName}); " +
                $"INSERT INTO {this.name} SELECT * FROM dbo.{this.name}_TMP; " +
                $"DELETE FROM {this.name}_TMP; " + 
                $"end ", mkconn);
            mkconn.Open();
            mkcomm.ExecuteNonQuery();
            new Service1().WriteToFile($"{this.name} table created!");
            mktmpcomm.ExecuteNonQuery();
            new Service1().WriteToFile($"{this.name}_TMP table created!");
            makeTrigger.ExecuteNonQuery();
            new Service1().WriteToFile($"{this.name}trgr trigger created!");
            mkconn.Close();
        }

        /// <summary>
        /// Gather data for this table.
        /// </summary>
        /// <returns>Runs as task, returns task.</returns>
        public Task<int> GetDataAsync(Dictionary<string,string> config)
        {
            return Task.Run(() =>
            {
                new Service1().WriteToFile($"Gathering {this.name}");

                // Prepare variables.
                DataTable dt = new DataTable();
                dt.TableName = this.name;
                string columnString = "";

                // Create comma separated string of columns for queries. (DOES NOT INCLUDE CREATED PKs)
                for (int i = 0; i < columns.Count; i++)
                {
                    columnString += columns[i] + ",";
                    DataColumn c = new DataColumn();
                    c.DataType = System.Type.GetType(conversionTable[olddatatypes[i]][1]);
                    c.ColumnName = columns[i];
                    dt.Columns.Add(c);
                }
                columnString = columnString.TrimEnd(',');

                // Create/add a column for created PK's (NOT IN STRING AT THIS POINT)
                if (!pk)
                {
                    dt.Columns.Add(this.pkName, System.Type.GetType("System.String"));
                }

                // Make the Centurion connection and run the query.
                OdbcConnection odbc = new OdbcConnection($"DSN={config["SolidDNS"]};UID={config["SolidUID"]};PWD={config["SolidPWD"]};");
                OdbcCommand odbcomm = new OdbcCommand($"SELECT {columnString} FROM {this.name} {this.dateString}", odbc);
                odbc.Open();
                try
                {
                    using (OdbcDataReader reader = odbcomm.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // Create the temporary data row that will be added to datatable.
                            DataRow row = dt.NewRow();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (reader.GetValue(i).ToString() == "")
                                {
                                    continue;
                                }
                                row[this.columns[i]] = reader.GetValue(i).ToString();
                            }
                            // If there is no existing Primary Key, create one.
                            if (!pk)
                            {
                                string pkstring = "";
                                foreach (var key in keys)
                                {
                                    pkstring += row[key].ToString() + '_';
                                }
                                pkstring = pkstring.TrimEnd('_');
                                row[pkName] = pkstring;
                            }
                            // DataRow added to DataTable dt
                            dt.Rows.Add(row);
                        }
                        reader.Close();
                        odbc.Close();
                        // Mention retrieved rows.
                        new Service1().WriteToFile($"{this.name} retrieved {dt.Rows.Count} rows");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    Console.WriteLine(this.name + " " + columnString);
                }

                this.returnedData = dt;

                return 1;
            });
        }
        
    }
}