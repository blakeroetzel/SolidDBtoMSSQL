using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace SolidDBtoMSSQL
{
    class RunProgram
    {
        public RunProgram()
        {
            
        }
        /// <summary>
        /// Main program, runs from OnStart in Service1.cs
        /// </summary>
        public async Task goAsync(Dictionary<string, string> config)
        {
            // Standard interval
            var date = DateTime.Now.AddDays(-2);

            // Create collection of tables
            List<table> tables = new List<table>();
            Dictionary<string, DataTable> returnedDT = new Dictionary<string, DataTable>();

            // Populate Collection
            // Add new tables here and they will be copied (Delete mine or it won't work for you.

            tables.Add(new table("ABAN_STAFF_EFF", false, new List<string> { "Interval", "Split_Number" }, DateString("INTERVAL", date), config));
            tables.Add(new table("AGENT_VIEW", false, new List<string> { "INTERVAL", "AGENT_ID", "SPLIT_NUMBER" }, DateString("INTERVAL", date), config));
            tables.Add(new table("CALL", true, new List<string> { "CID" }, DateString("START_TIME", date), config));
            tables.Add(new table("CALL_CENTER_ARCHIVES", false, new List<string> { "CONVID", "SEGMENT" }, DateString("CALL_DATE", date), config));
            tables.Add(new table("INTERVAL_SPLIT_STATS", false, new List<string> { "SPLIT_NUMBER", "INTERVAL", "MEDIA_TYPE", "SERVER_ID" }, DateString("INTERVAL", date), config));
            tables.Add(new table("CALL_CENTER_SPLITS", true, new List<string> { "SPLIT_NUMBER" }, "", config));
            tables.Add(new table("CALL_CENTER_MEDIAS", true, new List<string> { "MEDIA_TYPE" }, "", config));
            tables.Add(new table("DAILY_SPLIT_SERVICE_LEVEL", false, new List<string> { "SPLIT_NUMBER", "MEDIA_TYPE", "INTERVAL_DATE" }, DateString("INTERVAL_DATE", date), config));
            tables.Add(new table("INTERVAL_SKILL_STATS", false, new List<string> { "SPLIT_NUMBER", "SKILL_NUMBER", "INTERVAL", "MEDIA_TYPE", "SERVER_ID" }, DateString("INTERVAL", date), config));
            tables.Add(new table("CALL_CENTER_SKILLS", true, new List<string> { "SKILL_NUMBER" }, "", config));
            tables.Add(new table("INTERVAL_AGENT_STATS", false, new List<string> { "AGENT_ID", "SPLIT_NUMBER", "INTERVAL", "MEDIA_TYPE", "SERVER_ID" }, DateString("INTERVAL", date), config));
            tables.Add(new table("DAILY_AGENT_IDLE_STATS", false, new List<string> { "AGENT_ID", "SPLIT_NUMBER", "INTERVAL", "IDLE_CODE", "SERVER_ID" }, DateString("INTERVAL", date), config));
            tables.Add(new table("CRM_TYPES", true, new List<string> { "CRM_TYPE" }, "", config));
            tables.Add(new table("CALL_CENTER_AGENTS", true, new List<string> { "AGENT_ID" }, "", config));
            tables.Add(new table("IDLE_REASONS", true, new List<string> { "IDLE_REASON" }, "", config));
            tables.Add(new table("EVENT_LOG", false, new List<string> { "EVENT_DATE", "BANK", "CHANNEL", "SPLIT_NUMBER", "AGENT_ID", "EVENT_TYPE", "EVENT_DATA", "EVENT_SOURCE" }, DateString("EVENT_DATE", date), config));
            tables.Add(new table("ABANDON_CALL", false, new List<string> { "CONVID", "SEGMENT" }, DateString("CALL_DATE", date), config));
            tables.Add(new table("SERVICE", true, new List<string> { "SID" }, DateString("START_TIME", date), config));
            tables.Add(new table("CONVERSATION", false, new List<string> { "CONVID", "SEGMENT" }, DateString("START_TIME", date), config));
            tables.Add(new table("CONVERSATION_DETAIL", false, new List<string> { "CONVID", "SEGMENT", "SEQUENCE" }, DateString("TIMESTAMP", date), config));
            tables.Add(new table("EVENT_DESCRIPTIONS", true, new List<string> { "EVENT_TYPE" }, "", config));
            tables.Add(new table("CRM_TICKETS", true, new List<string> { "TICKET_ID" }, "", config));
            tables.Add(new table("TICKET_ACTIVITIES", false, new List<string> { "TICKET_ID", "ACTIVITY_ID" }, DateString("DATE_OPENED", date), config));
            tables.Add(new table("EVDES", false, new List<string> { "SERVICE", "EVENT_ORDER", }, "", config));
            tables.Add(new table("EVSUM", false, new List<string> { "SUMID", "EVENT_NUMBER" }, "", config));
            tables.Add(new table("EVENTS", false, new List<string> { "SID", "EVENT_NUMBER" }, "", config));
            tables.Add(new table("SERVICE", true, new List<string> { "SID" }, DateString("START_TIME", date), config));


            // List of tasks (Data all pulls at once asynchronously)
            Task[] tasks = { };
            IEnumerable<Task<int>> getDataQuery =
                from table in tables
                select table.GetDataAsync(config);


            // Execute the tasks and wait for them to be done.
            List<Task<int>> getData = getDataQuery.ToList();
           while (getData.Any())
            {
                Task<int> finishedTask = await Task.WhenAny(getData);
                getData.Remove(finishedTask);
            }

            // If table doesn't exist, make it exist.
            foreach (var table in tables)
            {
                if (!table.tableExists(config))
                {
                    // Create new table, it doesn't exist yet.
                    table.makeTable(config);
                }
            }

            // Bulk copy data for each table.
            SqlConnection bulkconn = new SqlConnection($"server={config["MSSQLServer"]};uid={config["MSSQLUid"]};pwd={config["MSSQLPwd"]};database={config["MSSQLDatabase"]}");
            bulkconn.Open();
            foreach (var table in tables) {
                if (table.returnedData == null)
                {
                    Console.WriteLine($"Skipping {table.name}. No data returned.");
                    new Service1().WriteToFile($"Skipping {table.name}. No data returned.");
                    continue;
                }
                using (var bulkCopy = new SqlBulkCopy($"server={config["MSSQLServer"]};uid={config["MSSQLUid"]};pwd={config["MSSQLPwd"]};database={config["MSSQLDatabase"]}", SqlBulkCopyOptions.FireTriggers))
                {
                    foreach (DataColumn col in table.returnedData.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                    bulkCopy.BulkCopyTimeout = 600;
                    bulkCopy.DestinationTableName = (table.name+"_TMP");
                    try
                    {
                        bulkCopy.WriteToServer(table.returnedData);
                    } catch (Exception e)
                    {
                        new Service1().WriteToFile(e.ToString());
                    }
                }
                Console.WriteLine($"Wrote {table.returnedData.Rows.Count} rows to {table.name}");
                new Service1().WriteToFile($"Wrote {table.returnedData.Rows.Count} rows to {table.name}");
            }
            bulkconn.Close();

            new Service1().WriteToFile("Done.");
            tables.Clear();
            returnedDT.Clear();

        }

        /// <summary>
        /// Creates the string to pass to the DB based on passed params.
        /// </summary>
        /// <param name="colName">Column name of date field in target table.</param>
        /// <param name="date">The DateTime to be formatted into the string.</param>
        /// <returns>Formatted string containing a where statement for the time range.</returns>
        string DateString(string colName, DateTime date)
        {
            return $"WHERE {colName} >= '{date.ToString("yyyy-MM-dd")}'";
        }
    }
}