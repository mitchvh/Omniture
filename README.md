# Omniture
A C# application that allows the calling of the Adobe Omniture 1.4 REST Reporting API, parses the results and bulk loads it into a Microsoft SQL Server database.

You need to modify the app config with your Database Connection String, Omniture User Name and Secret. 

The reports (elements/metrics) to transfer are read from a table in the SQL Server (by default this table is called ReportDefinition and a creation script is included). The columns of the table are relatively self explanatory (they map pretty well to the API Explorer Report.Queue values) with a few qualifications;

1. If the destination table does not exist, the application will create it - I recommend you let it do this, especially if using segments.
2. The dateFrom column is when to start pulling the data from. The application will check that there is data for each day from this day until the dateTo column. If you make this column relative (i.e. -1) it basically disables the fill in feature.
3. The dateTo columns can be a negative number (i.e. -1) to transfer the data from the previous day(s). If you specify actual dates, use the format YYYY-MM-DD.
4. The elements, metrics and segments are comma separated values without spaces. Spaces may work, but it is untested. 
5. If you specify a segment(s), the code assumes there is a (or more) segment column(s) in the target SQL Server table and populates it using the segment description that comes back from Omniture. You can therefore have multiple requests populating the same destination table. The application will create the segment columns in the target table schema, if the table does not exist, but the naming isn't particularly clever (i.e. segment1, segment2).
6. The SegmentNames column needs to be filled in for requests where you use segments and put the results in the same table. This allows the fill in logic to determine if there is missing data for each segment request.

Note that this code was written, with limited time, as sample code for a customer, so the testing and error handling still needs some work. 

Feel free to fork away...
