using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;


namespace Omniture
{
    // class to potentially hold a list of dates to process
    class ProcessDates
    {
        public DateTime dateFrom;
        public DateTime dateTo;

        public ProcessDates(DateTime dtFrom, DateTime dtTo)
        {
            dateFrom = dtFrom;
            dateTo = dtTo;
        }
    }

    // Class used to hold report requests from the SQL table
    class ReportRequest
    {
        public string suiteID;
        public string dateFrom;
        public string dateTo;
        public string dateGran;
        public string[] elements;
        public string[] segments;
        public string[] metrics;
        public string[] segnames;
        public string desttable;
        public int multiusetbl;
        public bool createdtbl;
        public DataTable dt;
        public List<ProcessDates> dates;
        public bool Done;

        public void DataTableReset()
        {
            if (dt.Rows.Count > 0 || dt.Columns.Count > 1)
            {
                dt.Reset();
                dt.Columns.Add("SumaryDate", System.Type.GetType("System.DateTime"));       // always add a SummaryDate column to the table as the first column
            }
        }

        public ReportRequest(string _SuiteID, string _dateFrom, string _dateTo, string _dateGran, string[] _elements, string[] _segments, string[] _segnames, string[] _metrics, string _destTable, int _multiusetbl)
        {
            suiteID = _SuiteID;
            dateFrom = _dateFrom;
            dateTo = _dateTo;
            dateGran = _dateGran;
            elements = _elements;
            segments = _segments;
            segnames = _segnames;
            metrics = _metrics;
            desttable = _destTable;
            multiusetbl = _multiusetbl;
            createdtbl = false;
            dates = new List<ProcessDates>(10);
            Done = false;
            dt = new DataTable(desttable);                                              // create a DataTable with the appropriate name to hold the returned report
            dt.Columns.Add("SumaryDate", System.Type.GetType("System.DateTime"));       // always add a SummaryDate column to the table as the first column
        }
    }

    class SqlServer
    {
        static SqlConnection cnSql = null;
        static string strCon;

        #region SQL Server inpt and output functions

        // Open a SQL Connection
        public static void Initialize(string constr)
        {
            strCon = constr;                                                            // save the connection string for potential deletes in the tables

            // Try to open a connection to the source/destination SQL Server
            try
            {
                cnSql = new SqlConnection(constr);
                cnSql.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error connecting to SQL Server database using connection string: " + constr + "\r\nException: " + e.Message);
                return;                                                                                                                 // give up, can't continue without a SQL connection
            }
        }


        // iterates through all of the enabled rows in the Report Definition table and adds each request to the ReportRequest list
        public static List<ReportRequest> GetReportRequests(string repreqtbl)
        {
            string sqlcmd = "select reportSuiteID, dateFrom, dateTo, dateGranularity, elements, segments, segmentNames, metrics, DestinationTable, (select count(*) from " + repreqtbl +
                                " r2 where Enabled=1 and r1.DestinationTable=r2.DestinationTable and r1.ExecutionOrder > r2.ExecutionOrder) from " + repreqtbl + " r1 where Enabled=1 order by ExecutionOrder";
            List<ReportRequest> reports = new List<ReportRequest> { };

            SqlCommand cmd = new SqlCommand(sqlcmd, cnSql);
            cmd.CommandTimeout = 0;
            cmd.CommandType = CommandType.Text;
            SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleResult);
            while (dr.Read())
                reports.Add(new ReportRequest(dr.GetString(0), dr.GetString(1), dr.GetString(2), dr.GetString(3), dr.GetString(4).Split(','), (dr.IsDBNull(5) ? null : dr.GetString(5).Split(',')), (dr.IsDBNull(6) ? null : dr.GetString(6).Split(',')), dr.GetString(7).Split(','), dr.GetString(8), dr.GetInt32(9)));
            dr.Close();
            return reports;
        }

        // A rather complicated function to retrieve the date(s) for the next Omniture report request (actually set in the referenced ReportRequest object).
        //  the function is complicated by the possibility of having to potentially create the target table and possibly clean up past days where transfers were incomplete and do associated table cleanup etc.
        public static bool GetNextRequestDate(ref ReportRequest r)
        {
            if (r.Done) return false;                                                           // was the last date (range) requested the last required? If so, signal that we are done.

            if (r.dates != null && r.dates.Count > 0)                                           // not the first call? If not then we will have one or more date ranges to process in the request
            {
                r.DataTableReset();                                                             // clear previous request DataTable
                DateTime from = r.dates[0].dateFrom;                                            // get date range "from Date" that we are currently working on
                r.dateFrom = from.ToString("yyyy-MM-dd");                                       // set next request "from date"
                DateTime to = r.dates[0].dateTo;                                                // get date range "to Date"
                TimeSpan ts = to.Subtract(from);                                                // get the difference between from and to dates
                if (ts.TotalDays <= 14)                                                         // does it fit within our 2 week processing window?
                {
                    r.dateTo = to.ToString("yyyy-MM-dd");                                       // set "to" date
                    r.dates.RemoveAt(0);                                                        // we are finished this date range - so remove it from the list
                    if (r.dates.Count == 0)                                                     // are there no more date ranges to process?
                    {
                        r.Done = true;                                                          // if no more date range requests to do, then we are done this request
                        Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Requesting final data chunk for table: " + r.desttable + " for dates from: " + r.dateFrom + " to: " + r.dateTo);
                        return true;
                    }
                }
                else
                {
                    r.dateTo = from.AddDays(14).ToString("yyyy-MM-dd");                         // do longer requests in 2 week chunks
                    r.dates[0].dateFrom = from.AddDays(15);                                     // save new "from date" for the next chunk
                }
                Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Requesting data chunk for table: " + r.desttable + " for dates from: " + r.dateFrom + " to: " + r.dateTo);
                return true;
            }

            SqlCommand cmd = null;
            string chk = "select count(*) from sys.tables where name='" + r.desttable + "'";    // simple check for table existence - yes, it doesn't accomodate different schemas
            try
            {
                cmd = new SqlCommand(chk, cnSql);
                cmd.CommandTimeout = 0;
                cmd.CommandType = CommandType.Text;
                SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SingleResult);
                if (dr.Read() && dr.GetInt32(0) == 0) r.createdtbl = true;                      // destination table does not exist
                dr.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception on check of destination table " + r.desttable + " existence - \r\nException: " + e.Message);
            }
            if (r.createdtbl)                                                                   // table doesn't exist - so create it
            {
                // build a SQL statement to create the table
                StringBuilder createdest = new StringBuilder("CREATE TABLE " + r.desttable + " ( SummaryDate datetime not null", 500);
                for (int i = 0; i < r.elements.Length; i++) createdest.Append(", " + r.elements[i] + " nvarchar(150) not null");
                if (r.segments != null) for (int i = 0; i < r.segments.Length; i++) createdest.Append(", segment" + (i + 1).ToString() + " nvarchar(150) null");
                for (int i = 0; i < r.metrics.Length; i++) createdest.Append(", " + r.metrics[i] + " int not null");
                createdest.Append(" )");
                cmd.CommandText = createdest.ToString();                                        // set the command text to the create statement
                try
                {
                    cmd.ExecuteNonQuery();                                                      // execute the create table command
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception on create of output Data Table for " + r.desttable + "\r\nException: " + ex.Message + "\r\nSQL: " + createdest);
                }
            }
            string dateFrom = r.dateFrom;                                                       // get from date from the request
            if (dateFrom.StartsWith("-"))                                                       // is date relative - calculate relative date
            {
                double offset = Convert.ToDouble(dateFrom);
                DateTime dt = DateTime.Today;
                dt = dt.AddDays(offset);
                dateFrom = dt.ToString("yyyy-MM-dd");
            }
            string dateTo = r.dateTo;                                                           // get the to date from the request
            if (r.dateFrom == dateTo) dateTo = dateFrom;                                        // if end is same as start, use the start value (which may have been calculated)
            else if (dateTo.StartsWith("-"))                                                    // if relative date, calculate relative date
            {
                double offset = Convert.ToDouble(dateTo);
                DateTime dt = DateTime.Today;
                dt = dt.AddDays(offset);
                dateTo = dt.ToString("yyyy-MM-dd");
            }
            DateTime dtFrom = DateTime.Parse(dateFrom);
            DateTime dtTo = DateTime.Parse(dateTo);
            SqlConnection cnDel = null;                                                         // create a separate connection to do any table cleanup (deletes) on
            try
            {
                cnDel = new SqlConnection(strCon);                                              // create a connection for deletes
                cnDel.Open();                                                                   // open connection
            }
            catch (Exception e)
            {
                Console.WriteLine("Unable to open a database connection to delete the target day's data in table: " + r.desttable + "\r\nException: " + e.Message);
            }
            SqlCommand cmdDel = new SqlCommand("delete from " + r.desttable + " where DATEADD(dd, DATEDIFF(dd, 0, SummaryDate), 0) = cast('" + dateTo + " 00:00:00' as datetime)", cnDel);
            cmdDel.CommandTimeout = 0;
            cmdDel.CommandType = CommandType.Text;
            if (r.multiusetbl == 0)                                                             // if we use the same destination table more than once for multiple requests, only delete the current day once (the first time!)
            {
                try
                {
                    cmdDel.ExecuteNonQuery();                                                   // delete the "dateTo's" data so we don't end up with duplicates if run multiple times in a day
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception deleting the target day's data in table: " + r.desttable + "\r\nException: " + e.Message);
                }
            }
            if (r.createdtbl || !(r.dateGran.ToUpper() == "HOUR" || r.dateGran.ToUpper() == "DAY")) // we created the table during this execution (or granulatity we don't handle), so don't check history
            {
                TimeSpan ts = dtTo.Subtract(dtFrom);
                if (ts.TotalDays <= 14)                                                         // handle chunking the request into 2 week chunks - note the granularity other than hour and day isn't well supported
                {
                    Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Requesting final data chunk for table: " + r.desttable + " for dates from: " + r.dateFrom + " to: " + r.dateTo);
                    r.Done = true;                                                              // single request and done
                    return true;
                }
                r.dateTo = dtFrom.AddDays(14).ToString("yyyy-MM-dd");                           // do longer requests in chunks of 14 days
                ProcessDates p = new ProcessDates(dtFrom.AddDays(15), dtTo);                    // save from and to in request for next chunk
                Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Requesting data chunk for table: " + r.desttable + " for dates from: " + r.dateFrom + " to: " + r.dateTo);
                cmdDel.Connection = null;
                cnDel.Close();
                cmd.Connection = null;
                return true;
            }
            else                                                                                // destination table exists, check if we have missed anything based on what is in table or based on dates
            {
                if (r.dateGran.ToUpper() == "HOUR")
                {
                    StringBuilder sb = new StringBuilder("with hourtbl as ( SELECT distinct Year(SummaryDate) as 'Year', Month(SummaryDate) as 'Month', Day(SummaryDate) as 'Day', DatePart(hh, SummaryDate) as 'Hour' FROM " + r.desttable, 500);
                    if (r.segments != null && r.segments.Length > 0)
                    {
                        sb.Append(" where");
                        for (int i = 0; i < r.segments.Length; i++) sb.Append(" segment" + (i+1).ToString() + " = '" + r.segnames[i] + "'");
                    }
                    sb.Append(" GROUP BY SummaryDate ), datetbl as ( select[Year], [Month], [Day], count(*) as 'Hours' from hourtbl group by[Year], [Month], [Day] ) " + 
                              "select DateFromParts(t.[year], Month(t.[date]), t.[dayofmonth]), d.[Hours] from dimTime t left outer join datetbl d on(d.[Year]= t.[year] and d.[Month]= Month(t.[date]) and d.[Day]= t.[dayofmonth]) "+
                              "where t.[date] >= cast('" + dateFrom + " 00:00:00' as datetime) and t.[date] < cast('" + dateTo + " 00:00:00' as datetime) and ISNULL(d.[Hours], 0) != 24");
                    cmd.CommandText = sb.ToString();
                }
                else // if (r.dateGran.ToUpper() == "DAY")
                {
                    StringBuilder sb = new StringBuilder("with datetbl as ( select distinct Year(SummaryDate) as 'Year', Month(SummaryDate) as 'Month', Day(SummaryDate) as 'Day' from " + r.desttable, 500);
                    if (r.segments != null && r.segments.Length > 0)
                    {
                        sb.Append(" where");
                        for (int i = 0; i < r.segments.Length; i++) sb.Append(" segment" + (i + 1).ToString() + " = '" + r.segnames[i] + "'");
                    }
                    sb.Append(" ) select DateFromParts(t.[year], Month(t.[date]), t.[dayofmonth]) as 'Date', NULL from dimTime t left outer join datetbl d on(d.[Year] = t.[year] and d.[Month] = Month(t.[date]) and d.[Day] = t.[dayofmonth]) " +
                                " where t.[date] >= cast('" + dateFrom + " 00:00:00' as datetime) and t.[date] < cast('" + dateTo + " 00:00:00' as datetime) and d.[Year] is null");
                    cmd.CommandText = sb.ToString();
                }
                cmd.CommandType = CommandType.Text;
                SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleResult);             // execute query and get a data reader for result set
                DateTime from = dtTo;                                                           // it is possible, even likely that we don't find any missing records, so need to support fall through case (single day: yesterday request)
                DateTime last = DateTime.MaxValue;
                while (dr.Read())                                                               // process any resultset rows
                {
                    DateTime dtCol = dr.GetDateTime(0);                                         // get date column (first column) from result set
                    if (!dr.IsDBNull(1))                                                        // if the second column is not null we have some cleanup to do for the day
                    {
                        try
                        {
                            cmdDel.CommandText = "delete from " + r.desttable + " where DATEADD(dd, DATEDIFF(dd, 0, SummaryDate), 0) = cast('" + dtCol.ToString("yyyy/MM/dd") + " 00:00:00' as datetime)";
                            cmdDel.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception on sql cmd: " + cmdDel.CommandText + "\r\n Exception: " + e.Message);
                        }
                    }
                    if (last == DateTime.MaxValue) from = dtCol;                                // first record - so only set from variable
                    else
                    {
                        if (dtCol.Subtract(last).TotalDays > 1)                                 // more than one day between previous and current dates?
                        {
                            r.dates.Add(new ProcessDates(from, last));                          // add a new date range request to wrap up previous data missing range
                            from = dtCol;                                                       // start a new date range
                        }
                    }
                    last = dtCol;                                                               // set last variable
                }
                TimeSpan ts = (last != DateTime.MaxValue) ? dtTo.Subtract(last) : new TimeSpan(1, 0, 0, 0);
                if (r.dates.Count == 0 && ts.TotalDays == 1)                                    // usual case of no missing records - or perhaps missing previous sequential day(s), just do the "to" date (and maybe a few previous days)
                {
                    r.dateFrom = from.ToString("yyyy-MM-dd");                                   // include any missing previous day(s), if sequential, in a single request
                    r.dateTo = dateTo;
                    r.Done = true;
                    Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Requesting final data chunk for table: " + r.desttable + " for dates from: " + r.dateFrom + " to: " + r.dateTo);
                }
                else
                {
                    r.dates.Add(new ProcessDates(from, last));                                  // add a new date range request for the last record(s)
                    if (last != dtTo) r.dates.Add(new ProcessDates(dtTo, dtTo));                // add a new date range request for the single day regular request
                    GetNextRequestDate(ref r);                                                  // call this function to get the first date (range) to process (instead of replicating same logic again)
                }
                dr.Close();                                                                     // clean up open objects
                cmd.Connection = null;
                cmdDel.Connection = null;
                cnDel.Close();
                return true;
            }
        }


        // Writes the passed in DataTable out to the destination SQL Server table using the very fast bulk copy API - the code checks if the table exists first and creates if it is doesn't exist
        public static void WriteDataTable(ReportRequest r)
        {
            try
            {
                SqlBulkCopy bc = new SqlBulkCopy(cnSql);
                bc.DestinationTableName = r.desttable;
                bc.WriteToServer(r.dt);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception on write of DataTable to SQL Server: " + e.Message);
            }
        }

        // Closes down the SQL Connection
        public static void Shutdown()
        {
            try
            {
                cnSql.Close();
            }
            catch { }
        }

        #endregion

    }
}
