using System;
using System.Net;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using System.Configuration;

namespace Omniture
{
    class main
    {

        // Main Application Entry Point
        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Start of Processing");

            //get connection string and report request table from config
            string constr = System.Configuration.ConfigurationManager.AppSettings.Get("ConnectionString");
            string repreqtbl = System.Configuration.ConfigurationManager.AppSettings.Get("ReportTable");
            string Username = System.Configuration.ConfigurationManager.AppSettings.Get("Username");
            string Secret = System.Configuration.ConfigurationManager.AppSettings.Get("Secret");

            try
            {
                SqlServer.Initialize(constr);

                // Build list of report requests
                List<ReportRequest> ReportRequests = SqlServer.GetReportRequests(repreqtbl);

                // Process each requested reports
                for (int i=0; i < ReportRequests.Count; i++)
                {
                    ReportRequest r = ReportRequests[i];
                    Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Processing Report for Table: " + r.desttable);
                    while (true)
                    {
                        if (!SqlServer.GetNextRequestDate(ref r)) break;                                                            // get date(s) for next request (could include recovery of previous dates) - false if we are done
                        string json = Omniture.DoOmnitureRequest(Username, Secret, r);                                              // queue the report and wait for it to complete, returned string is the json report string
                        Omniture.ProcessJsonResponse(r, json);                                                                      // process the json report string
                        SqlServer.WriteDataTable(r);                                                                                // save the results in SQL Server
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Unhandled exception: " + e.Message);
            }
            finally
            {
                SqlServer.Shutdown();                                                                                               // swallow any exception on final connection close
            }
            Console.WriteLine(DateTime.Now.ToString("hhhh:mm:ss") + " - Processing Complete");
        }
    }
}


