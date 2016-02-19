# Omniture
A C# application that allows the calling of the Adobe Omniture 1.4 REST Reporting API, parses the results and bulk loads it into a Microsoft SQL Server database.

You need to modify the app config with your Database Connection String, Omniture User Name and Secret. 

The reports (elements/metrics) to transfer are read from a table in the SQL Server. Included is a SQL script to create the table. The columns of the table are self explanatory (they map pretty well to the API Explorer Report.Queue values) with a few qualifications;

1. If the destination table does not exist, the application will create it - I recommend you let it do this, especially if using segments.
2. The dateFrom and dateTo columns can be a negative number (i.e. -1) to transfer the data from the previous day(s). If you specify actual dates, use the format YYYY-MM-DD.
3. The elements, metrics and segments are comma separated values without spaces. Spaces may work, but it is untested. 
4. If you specify a segment(s), the code assumes there is a (or more) segment column(s) in the target SQL Server table and populates it using the segment description that comes back from Omniture. You can therefore have multiple requests populating the same destination table. The application will create the segment columns in the target table schema, if the table does not exist, but the naming isn't particularly clever (i.e. segment1, segment2).

Note that this code was written, with limited time, as sample code for a customer, so the error handling still needs some work. 

Feel free to fork away...
