using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IdentityModel;
using Omniture.Adobe;

namespace Omniture
{
    class Program
    {
        static void Main(string[] args)
        {
            Omniture.Adobe.AdobeAnalyticsServicePortTypeClient client = new Omniture.Adobe.AdobeAnalyticsServicePortTypeClient();
            client.ClientCredentials.SecurityTokenHandlerCollectionManager.SecurityTokenHandlerCollections..Add(new UsernameToken());


            client.ClientCredentials =
            // User: "azure:TD Bank";
            // secret: "ceeb65f0b8d07864690190d0b0235612"

            OmnitureWebServicePortTypeClient client = OmnitureWebServicePortTypeClient.getClient("[api username]", "[api secret]", "https://api.omniture.com/admin/1.3/");

            /* Create a reportDescription object to set all properties on */
            reportDescription rd = new reportDescription();
            rd.reportSuiteID = "tdtdct";
            rd.dateFrom = "2015-12-01";
            rd.dateTo = "2015-12-31";
            rd.metrics = new reportDescriptionMetric[1];
            rd.metrics[0] = new reportDescriptionMetric();
            rd.metrics[0].id = "visits";
            rd.elements = new reportDescriptionElement[1];
            rd.elements[0] = new reportDescriptionElement();
            rd.elements[0].id = "pages";
            //rd.elements[0].classification = "brand";
            rd.locale = reportDescriptionLocale.en_US;

            Console.WriteLine("Queuing report...");

            reportQueueResponse response = client.ReportQueue(rd);

            /* Store the report response in reportID variable */
            int reportID = response.reportID;

            while (true)
            {
                Thread.Sleep(5000);

                /* Get the report status (using Report.GetStatus) */
                reportResponse resp = client.ReportGet(reportID);

                if (resp.report != null)
                {
                    // loop through the returned data and process every row
                    for (int i = 0; i < resp.report.data.Length; i++)
                    {
                        Console.WriteLine("name " + resp.report.data[i].name + " count " + resp.report.data[i].counts[0]);
                    }
                    break;
                }
            }
        }
    }
}
}
