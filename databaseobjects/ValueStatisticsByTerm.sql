USE [State_Report_Data]
GO

/****** Object:  Table [dbo].[ValueStatisticsByTerm]    Script Date: 1/31/2017 1:00:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ValueStatisticsByTerm](
	[DB] [varchar](255) NULL,
	[RecordType] [varchar](255) NULL,
	[Term] [varchar](255) NULL,
	[Submission Type] [varchar](255) NULL,
	[Element Number] [varchar](255) NULL,
	[Element Description] [varchar](255) NULL,
	[Value] [varchar](255) NULL,
	[Value Description] [varchar](255) NULL,
	[Percentage] [varchar](255) NULL,
	[Count] [varchar](255) NULL
) ON [PRIMARY]

GO


