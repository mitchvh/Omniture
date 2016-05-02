DROP TABLE [dbo].[ReportDefinition]
GO
CREATE TABLE [dbo].[ReportDefinition](
	[ExecutionOrder] [int] NOT NULL,
	[Enabled] [bit] NOT NULL,
	[reportSuiteID] [nvarchar](50) NOT NULL,
	[dateFrom] [nvarchar](20) NOT NULL,
	[dateTo] [nvarchar](20) NOT NULL,
	[dateGranularity] [nvarchar](20) NOT NULL,
	[elements] [nvarchar](2000) NOT NULL,
	[metrics] [nvarchar](2000) NOT NULL,
	[segments] [nvarchar](2000) NULL,
	[DestinationTable] [nvarchar](50) NOT NULL
)
GO


