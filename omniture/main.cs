using System;
using System.Net;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net.Http;
using System.IO;
using System.Security.Cryptography;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Configuration;

namespace Omniture
{
    #region Omniture JSON Request Type Definitions

    class metricdef
    {
        public string id;

        public metricdef(string _id)
        {
            id = _id;
        }
    }

    class elementdef
    {
        public string id;
        public string classification;

        public elementdef (string _id, string _classification)
        {
            id = _id;
            classification = _classification;
        }
    }

    class segmentdef
    {
        public string id;
        public string element;
        public string classification;

        public segmentdef (string _id, string _element, string _classification)
        {
            id = _id;
            element = _element;
            classification = _classification;
        }
    }

    class OmnitureReportDescription
    {
        public string reportSuiteID;
        public string dateFrom;
        public string dateTo;
        public string dateGranularity;
        public metricdef[] metrics;
        public elementdef[] elements;
        public segmentdef[] segments;
        public string locale;
    }

    class OmnitureReportQueueRequest
    {
        public OmnitureReportDescription reportDescription;

        public OmnitureReportQueueRequest(string reportSuiteID, string dateFrom, string dateTo, string dateGranularity, string[] elements, string[] metrics, string[] segments)
        {
            reportDescription = new OmnitureReportDescription();
            reportDescription.reportSuiteID = reportSuiteID;
            reportDescription.dateFrom = dateFrom;
            reportDescription.dateTo = dateTo;
            reportDescription.dateGranularity = dateGranularity;
            int len;
            if ((len = elements.Length) > 0)
            {
                reportDescription.elements = new elementdef[len];
                for (int i = 0; i < len; i++) reportDescription.elements[i] = new elementdef(elements[i], null);
            }
            if ((len = metrics.Length) > 0)
            {
                reportDescription.metrics = new metricdef[len];
                for (int i = 0; i < len; i++) reportDescription.metrics[i] = new metricdef(metrics[i]);
            }
            if (segments != null && (len = segments.Length) > 0)
            {
                reportDescription.segments = new segmentdef[len];
                for (int i = 0; i < len; i++) reportDescription.segments[i] = new segmentdef(segments[i], null, null);
            }
            reportDescription.locale = "en-us";
        }
    }

    #endregion

    class ReportRequest
    {
        public int elements;
        public string jsonrequest;
        public string destination;
        public string createdest;

        public ReportRequest(int elementcount, string json, string desttable, string createstmt)
        {
            elements = elementcount;
            jsonrequest = json;
            destination = desttable;
            createdest = createstmt;
        }
    }

    class main
    {
        static int RowsOut = 0;
        static string constr, repreqtbl;
        static string[] eltlabels = new string[100];
        static string[] seglabels = new string[100];
        static int elementcount = 0;
        static int segmentcount = 0;
        static string Username;
        static string Secret;

        #region Omniture Functions

        // Encrypting passwords with SHA1 in .NET and Java 
        // http://authors.aspalliance.com/thycotic/articles/view.aspx?id=2 
        static private string getBase64Digest(string input)
        {
            SHA1 sha = new SHA1Managed();
            ASCIIEncoding ae = new ASCIIEncoding();
            byte[] data = ae.GetBytes(input);
            byte[] digest = sha.ComputeHash(data);
            return Convert.ToBase64String(digest);
        }

        // generate random nonce 
        static private string generateNonce()
        {
            Random random = new Random();
            int len = 24;
            string chars = "0123456789abcdef";
            string nonce = "";
            for (int i = 0; i < len; i++)
            {
                nonce += chars.Substring(Convert.ToInt32(Math.Floor(random.NextDouble() * chars.Length)), 1);
            }
            return nonce;
        }

        // Time stamp in UTC string 
        static private string generateTimestamp()
        {
            return DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        }

        // C#-Base64 Encoding 
        // http://www.vbforums.com/showthread.php?t=287324 
        static private string base64Encode(string data)
        {
            byte[] encData_byte = new byte[data.Length];
            encData_byte = System.Text.Encoding.UTF8.GetBytes(data);
            string encodedData = Convert.ToBase64String(encData_byte);
            return encodedData;
        }

        static HttpWebRequest BuildRequest(string method, string request)
        {
            string ENDPOINT = "https://api.omniture.com/admin/1.4/rest/";
            string timecreated = generateTimestamp();
            string nonce = generateNonce();
            string digest = getBase64Digest(nonce + timecreated + Secret);
            nonce = base64Encode(nonce);

            HttpWebRequest omniRequest = (HttpWebRequest)WebRequest.Create(ENDPOINT + method);
            omniRequest.Headers.Add("X-WSSE: UsernameToken Username=\"" + Username + "\", PasswordDigest=\"" + digest + "\", Nonce=\"" + nonce + "\", Created=\"" + timecreated + "\"");
            omniRequest.Method = "POST";
            omniRequest.ContentType = "application/x-www-form-urlencoded";
            //string reqrep = "{\r\n\"reportID\":\"" + ReportID.ToString() + "\"\r\n}";
            byte[] bytearray = Encoding.UTF8.GetBytes(request);
            Stream outstr = omniRequest.GetRequestStream();
            outstr.Write(bytearray, 0, bytearray.Length);
            outstr.Flush();
            outstr.Close();
            return omniRequest;
        }

        static string DoOmnitureRequest(string jsonreq)
        {
            HttpWebResponse statusResponse = null;
            string response = "";

            HttpWebRequest omniRequest = BuildRequest("?method=Report.Queue", jsonreq);
            try
            {
                statusResponse = (HttpWebResponse)omniRequest.GetResponse();
                using (Stream receiveStream = statusResponse.GetResponseStream())
                {
                    using (StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8))
                    {
                        response = readStream.ReadToEnd();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            JObject o = JObject.Parse(response);
            string ReportID = (string)o["reportID"];
            Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Report Queued - ReportID: " + ReportID);
            bool done = false;
            while (!done)
            {
                Thread.Sleep(15000);
                omniRequest = BuildRequest("?method=Report.Get", "{\r\n\"reportID\":\"" + ReportID.ToString() + "\"\r\n}");
                response = "";
                try
                {
                    statusResponse = (HttpWebResponse)omniRequest.GetResponse();                                                                // a bad request exception seems to corrupt the request. so we need to rebuild it
                    using (Stream receiveStream = statusResponse.GetResponseStream())
                    {
                        using (StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8))
                        {
                            response = readStream.ReadToEnd();
                            if (response.IndexOf("\"report_not_ready\"") == -1) done = true;                                                    // I don't think we would ever actually get report_not_ready, since it is always a 400 exception
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Message.IndexOf("(400) Bad Request") == -1) Console.WriteLine("Exception on Omniture Report.Get: " + ex.Message);    // stupid Adobe - (400) Bad Request may mean it isn't ready
                }
            }
            Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Report Complete - ReportID: " + ReportID);
            return response;
        }

        #endregion

        #region JSON Response Processing Functions

        static string RemoveJunk(string json)
        {
            StringBuilder sb = new StringBuilder("{\r\n", json.Length);
            int st = json.IndexOf("\"elements\"");
            int brackets = 0;
            for (int i = st; i < json.Length; i++)
            {
                if (json[i] != '\t') sb.Append(json[i]);
                if (json[i] == '[') brackets++;
                if (json[i] == ']' && --brackets <= 0) break;
            }
            sb.Append(",");
            st = json.IndexOf("\"metrics\"");
            brackets = 0;
            for (int i = st; i < json.Length; i++)
            {
                if (json[i] != '\t') sb.Append(json[i]);
                if (json[i] == '[') brackets++;
                if (json[i] == ']' && --brackets <= 0) break;
            }
            sb.Append(",");
            st = json.IndexOf("\"segments\"");
            if (st != -1)
            {
                brackets = 0;
                for (int i = st; i < json.Length; i++)
                {
                    if (json[i] != '\t') sb.Append(json[i]);
                    if (json[i] == '[') brackets++;
                    if (json[i] == ']' && --brackets <= 0) break;
                }
                sb.Append(",");
            }
            st = json.IndexOf("\"data\"");
            brackets = 0;
            for (int i = st; i < json.Length; i++)
            {
                if (json[i] != '\t') sb.Append(json[i]);
                if (json[i] == '[') brackets++;
                if (json[i] == ']' && --brackets <= 0) break;
            }
            sb.Append("\r\n}");
            return sb.ToString();
        }

        static void GetBreakdown(DataTable dtOutput, int level, DateTime date, DataRow dr)
        {
            bool nested = false;
            if (level != 0) eltlabels[level - 1] = (string)dr["name"];
            if (dr.Table.Columns.Contains("breakdown"))                                                                 // does the table row we are working on have a subtable?
            {
                nested = true;
                try
                {
                    if (dr["breakdown"] != System.DBNull.Value)                                                         // check if null - some entries don't have data (count=0) - so ignore
                    {
                        DataTable dt = (DataTable)dr["breakdown"];                                                      // try to create a breakdown subtable
                        foreach (DataRow ndr in dt.Rows) GetBreakdown(dtOutput, level + 1, date, ndr);                  // for each nested breakdown statistic recurse
                    }
                }
                catch (Exception e)                                                                                     // since we check for empty entries, this shouldn't happen
                {
                    Console.WriteLine("Exception: rows output " + RowsOut.ToString() + "  level: " + level.ToString() + "  labels[0]: " + eltlabels[0] + "  labels[1]: " + eltlabels[1] + "  labels[2]: " + eltlabels[2] + "\r\nException: " + e.Message);
                }
            }
            if (!nested)                                                                                                // at bottom level of nesting?
            {
                DataRow newrow = dtOutput.NewRow();                                                                     // add a new row to the output table and set the values
                newrow[0] = date;
                for (int i = 1; i <= level; i++) newrow[i] = eltlabels[i - 1];                                          // copy the element labels into the columns of the dataset
                if (segmentcount > 0) for (int i = 0; i < segmentcount; i++) newrow[(i + elementcount + 1)] = seglabels[i];         // copy any segment values
                string[] cnts = (string[])dr["counts"];                                                                 // get the array of count strings from the source table
                int indx = segmentcount + elementcount + 1;
                foreach (string cnt in cnts) newrow[indx++] = Convert.ToInt32(cnt);                                     // for each nested breakdown count in counts string array put in new row
                dtOutput.Rows.Add(newrow);                                                                              // add new row to the output data table
                RowsOut++;                                                                                              // increment output row count
            }
        }

        static void ProcessJsonResponse(DataTable dtOutput, string jsoninput)
        {
            string json = RemoveJunk(jsoninput);                                                                        // remove fluff we don't want to deal with
            DataSet ds = JsonConvert.DeserializeObject<DataSet>(json);                                                  // deserialize the whole mess into a DataSet with multiple DataTables - thank you, thank you Mr Newton!!!
            DataTable dtElements = ds.Tables["elements"];                                                               // pull out elements table
            elementcount = dtElements.Rows.Count;                                                                       // save the count of elements, so we know where to insert data in the Data Table.
            foreach (DataRow dr in dtElements.Rows) dtOutput.Columns.Add((string)dr["id"], System.Type.GetType("System.String"));   // add columns in the output schema for the elements
            DataTable dtMetrics = ds.Tables["metrics"];                                                                 // pull out metrics table
            DataTable dtSegments = ds.Tables["segments"];                                                               // maybe pull out segments table
            segmentcount = 0;                                                                                           // reset count of segments
            if (dtSegments != null)                                                                                     // are there segments in the returned json?
            {
                foreach (DataRow dr in dtSegments.Rows)                                                                 // for each selected segment
                {
                    seglabels[segmentcount++] = (string)dr["name"];                                                     // capture segment names (vs. IDs in the request)
                    dtOutput.Columns.Add("segment"+segmentcount.ToString(), System.Type.GetType("System.String"));      // add a column(s) to the output dataset to hold the segment(s) values saved above
                }
            }
            foreach (DataRow dr in dtMetrics.Rows) dtOutput.Columns.Add((string)dr["id"], System.Type.GetType("System.Int32"));     // add columns in the output schema for the data (after elements and any segments)
            DataTable dtData = ds.Tables["data"];                                                                       // pull out the data into a table
            foreach (DataRow dr in dtData.Rows)                                                                         // for each date/hour returned - process the row...
            {
                DateTime date = DateTime.Parse(dr["year"] + "/" + dr["month"] + "/" + dr["day"] + " " + dr["Hour"] + ":00:00");
                GetBreakdown(dtOutput, 0, date, dr);                                                                    // in here lies the magic in a single deceptively simple method call :-)
            }
        }

        #endregion

        #region SQL Server inpt and output functions

        static List<ReportRequest> GetReportRequests()
        {
            string reportSuiteID, dateFrom, dateTo, dateGranulatity, elementcsv, metricscsv, segmentscsv, destination;
            string[] elements, metrics, segments;
            string sqlcmd = "select reportSuiteID, dateFrom, dateTo, dateGranularity, elements, metrics, segments, DestinationTable from " + repreqtbl + " where Enabled=1 order by ExecutionOrder";
            List<ReportRequest> reports = new List<ReportRequest> { };

            SqlConnection cn = new SqlConnection(constr);
            cn.Open();
            SqlCommand cmd = new SqlCommand(sqlcmd, cn);
            cmd.CommandTimeout = 0;
            cmd.CommandType = CommandType.Text;
            SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleResult);
            while (dr.Read())
            {
                reportSuiteID = dr.GetString(0);
                dateFrom = dr.GetString(1);
                if (dateFrom.StartsWith("-"))
                {
                    double offset = Convert.ToDouble(dateFrom);
                    DateTime dt = DateTime.Today;
                    dt = dt.AddDays(offset);
                    dateFrom = dt.ToString("yyyy-MM-dd");
                }
                dateTo = dr.GetString(2);
                if (dr.GetString(1) == dateTo) dateTo = dateFrom;                           // if end is same as start, use the start value (which may have been calculated)
                else if (dateFrom.StartsWith("-"))
                {
                    double offset = Convert.ToDouble(dateTo);
                    DateTime dt = DateTime.Today;
                    dt = dt.AddDays(offset);
                    dateTo = dt.ToString("yyyy-MM-dd");
                }
                dateGranulatity = dr.GetString(3);
                elementcsv = dr.GetString(4);
                elements = elementcsv.Split(',');
                metricscsv = dr.GetString(5);
                metrics = metricscsv.Split(',');
                if (!dr.IsDBNull(6))
                {
                    segmentscsv = dr.GetString(6);
                    segments = segmentscsv.Split(',');
                }
                else
                {
                    segmentscsv = null;
                    segments = null;
                }
                destination = dr.GetString(7);
                StringBuilder createdest = new StringBuilder("CREATE TABLE " + destination + " ( SummaryDate datetime not null", 500);
                for (int i = 0; i < elements.Length; i++) createdest.Append(", " + elements[i] + " nvarchar(150) not null");
                if (segments != null) for (int i = 0; i < segments.Length; i++) createdest.Append(", segment" + (i + 1).ToString() + " nvarchar(150) null");
                for (int i=0; i < metrics.Length; i++) createdest.Append(", " + metrics[i] + " int not null");
                createdest.Append(" )");
                OmnitureReportQueueRequest qr = new OmnitureReportQueueRequest(reportSuiteID, dateFrom, dateTo, dateGranulatity, elements, metrics, segments);
                reports.Add(new ReportRequest(elements.Length, JsonConvert.SerializeObject(qr), destination, createdest.ToString()));
            }
            return reports;
        }

        static void WriteDataTable(DataTable dtOutput, string destination, string createdest)
        {
            string chk = "select count(*) from sys.tables where name='" + destination + "'";

            try
            {
                SqlConnection cn = new SqlConnection(constr);
                cn.Open();
                SqlCommand cmd = new SqlCommand(chk, cn);
                cmd.CommandTimeout = 0;
                cmd.CommandType = CommandType.Text;
                SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SingleResult);
                if (dr.Read() && dr.GetInt32(0) == 0)
                {
                    dr.Close();
                    cmd.CommandText = createdest;
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception on create of output Data Table for " + destination + "\r\nException: " + ex.Message + "\r\nSQL: " + createdest);
                    }
                }
                else dr.Close();
                SqlBulkCopy bc = new SqlBulkCopy(cn);
                bc.DestinationTableName = destination;
                bc.WriteToServer(dtOutput);
                cn.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception on DataTable Output: " + e.Message);
            }
        }

        #endregion

        static void Main(string[] args)
        {
            //get connection string and report request table from config
            constr = System.Configuration.ConfigurationManager.AppSettings.Get("ConnectionString");
            repreqtbl = System.Configuration.ConfigurationManager.AppSettings.Get("ReportTable");
            Username = System.Configuration.ConfigurationManager.AppSettings.Get("Username");
            Secret = System.Configuration.ConfigurationManager.AppSettings.Get("Secret");

            List<ReportRequest> ReportRequests = GetReportRequests();
            foreach(ReportRequest r in ReportRequests)
            {
                Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Processing Report for Table: " + r.destination);
                string json = DoOmnitureRequest(r.jsonrequest);
                DataTable dt = new DataTable(r.destination);
                dt.Columns.Add("SumaryDate", System.Type.GetType("System.DateTime"));
                ProcessJsonResponse(dt, json);
                WriteDataTable(dt, r.destination, r.createdest);
            }
            Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Processing Complete");
        }
    }
}


