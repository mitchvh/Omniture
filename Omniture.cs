using System;
using System.Net;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Net.Http;
using System.IO;
using System.Security.Cryptography;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data;


namespace Omniture
{
    #region Omniture JSON Request Type Definitions

    class searchdef
    {
        public string type;
        public string[] keywords;

        public searchdef(string _type, string[] _keywords)
        {
            type = _type;
            keywords = _keywords;
        }
    }

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
        public searchdef search = null;
        public string[] selected = null;

        public elementdef(string _id, string _classification, string searchtype, string[] searchkeywords, string[] _selected)
        {
            id = _id;
            classification = _classification;
            if (searchtype != null) search = new searchdef(searchtype, searchkeywords);
            selected = _selected;
        }
    }

    class segmentdef
    {
        public string id;
        public string element;
        public string classification;
        public searchdef search = null;
        public string[] selected = null;

        public segmentdef(string _id, string _element, string _classification, string searchtype, string[] searchkeywords, string[] _selected)
        {
            id = _id;
            element = _element;
            classification = _classification;
            if (searchtype != null) search = new searchdef(searchtype, searchkeywords);
            selected = _selected;
        }
    }


    // class used to simply building the json report request - the NewtonSoft serialized will serialize all public properties into a well formed json request
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


    // class to encapsulate the logic of filling in the class structure used to create the Report.Queue request
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
                for (int i = 0; i < len; i++) reportDescription.elements[i] = new elementdef(elements[i], null, null, null, null);
            }
            if ((len = metrics.Length) > 0)
            {
                reportDescription.metrics = new metricdef[len];
                for (int i = 0; i < len; i++) reportDescription.metrics[i] = new metricdef(metrics[i]);
            }
            if (segments != null && (len = segments.Length) > 0)
            {
                reportDescription.segments = new segmentdef[len];
                for (int i = 0; i < len; i++) reportDescription.segments[i] = new segmentdef(segments[i], null, null, null, null, null);
            }
            reportDescription.locale = "en-us";
        }
    }

    #endregion

    class Omniture
    {
        static int RowsOut = 0;
        static string[] eltlabels = new string[100];
        static string[] seglabels = new string[100];
        static int elementcount = 0;
        static int segmentcount = 0;

        #region Omniture Security/Utility Functions (borrowed)

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

        #endregion

        #region Omniture Request Functions

        public static string BuildRequestJsonString(ReportRequest r)
        {
            // create the Omniture request structure 
            OmnitureReportQueueRequest qr = new OmnitureReportQueueRequest(r.suiteID, r.dateFrom, r.dateTo, r.dateGran, r.elements, r.metrics, r.segments);

            // serialize the Omniture request structure into json and add it to the list of reports to process
            return JsonConvert.SerializeObject(qr);
        }

        // Generate the Omniture web service request with all of the security requirements - at some point this should be converted to use OAUTH2 security, since they appear to support it now
        static HttpWebRequest BuildWebRequest(string Username, string Secret, string method, string request)
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


        // Queue a report request and wait for it to complete - when complete, return the response (json report string)
        public static string DoOmnitureRequest(string UserName, string Secret, ReportRequest r)
        {
            HttpWebResponse statusResponse = null;
            string response = "";

            string jsonreq = BuildRequestJsonString(r);
            HttpWebRequest omniRequest = BuildWebRequest(UserName, Secret, "?method=Report.Queue", jsonreq);
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
                Thread.Sleep(15000);                                                                                                            // check for report completion every 15 seconds
                omniRequest = BuildWebRequest(UserName, Secret, "?method=Report.Get", "{\r\n\"reportID\":\"" + ReportID.ToString() + "\"\r\n}");  // build the check request (need to do this every loop, since the 400 status error blows up some of the request/connection properties
                response = "";
                try
                {
                    statusResponse = (HttpWebResponse)omniRequest.GetResponse();                                                                // a bad request exception seems to corrupt the request, so we need to rebuild it since reuse fails miserably
                    using (Stream receiveStream = statusResponse.GetResponseStream())
                    {
                        using (StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8))
                        {
                            response = readStream.ReadToEnd();                                                                                  // read the whole response into a string - how much memory do you have? Don't try a whole year of hourly data!
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

        #region Omniture JSON Response Processing Functions


        // Removes some of the garbage characters that aren't required or get in the way of deserializing the returned json string into a Data Table
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


        // Process any nested tables
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


        // Process the json string returned from the Omniture web service and deserialize it into a Data Table
        public static int ProcessJsonResponse(ReportRequest r, string jsoninput)
        {
            string json = RemoveJunk(jsoninput);                                                                        // remove fluff we don't want to deal with
            DataSet ds = JsonConvert.DeserializeObject<DataSet>(json);                                                  // deserialize the whole mess into a DataSet with multiple DataTables - thank you, thank you Mr Newton!!!
            DataTable dtElements = ds.Tables["elements"];                                                               // pull out elements table
            elementcount = dtElements.Rows.Count;                                                                       // save the count of elements, so we know where to insert data in the Data Table.
            foreach (DataRow dr in dtElements.Rows) r.dt.Columns.Add((string)dr["id"], System.Type.GetType("System.String"));   // add columns in the output schema for the elements
            DataTable dtMetrics = ds.Tables["metrics"];                                                                 // pull out metrics table
            DataTable dtSegments = ds.Tables["segments"];                                                               // maybe pull out segments table
            segmentcount = 0;                                                                                           // reset count of segments
            if (dtSegments != null)                                                                                     // are there segments in the returned json?
            {
                foreach (DataRow dr in dtSegments.Rows)                                                                 // for each selected segment
                {
                    seglabels[segmentcount++] = (string)dr["name"];                                                     // capture segment names (vs. IDs in the request)
                    r.dt.Columns.Add("segment" + segmentcount.ToString(), System.Type.GetType("System.String"));        // add a column(s) to the output dataset to hold the segment(s) values saved above
                }
            }
            foreach (DataRow dr in dtMetrics.Rows) r.dt.Columns.Add((string)dr["id"], System.Type.GetType("System.Int32"));     // add columns in the output schema for the data (after elements and any segments)
            DataTable dtData = ds.Tables["data"];                                                                       // pull out the data into a table
            foreach (DataRow dr in dtData.Rows)                                                                         // for each date/hour returned - process the row...
            {
                DateTime date;
                if (dtData.Columns.Contains("Hour"))                                                                    // did we get an hour back?
                    date = DateTime.Parse(dr["year"] + "/" + dr["month"] + "/" + dr["day"] + " " + dr["Hour"] + ":00:00");  // include the hour in the date
                else
                    date = DateTime.Parse(dr["year"] + "/" + dr["month"] + "/" + dr["day"] + " 00:00:00");              // get the date - not sure if this will work for monthly or yearly granularity
                GetBreakdown(r.dt, 0, date, dr);                                                                        // in here lies the magic in a single deceptively simple method call :-)
            }
            return RowsOut;
        }

        #endregion

    }

}
