USE [BI_MIP]
GO
/****** Object:  Table [MIP2].[PRAMATA_NUMBER]    Script Date: 1/8/2019 2:51:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [MIP2].[PRAMATA_NUMBER](
	[pramata_number] [int] NULL,
	[is_deleted] [nvarchar](255) NULL,
	[start_date_timestamp] [datetime] NULL,
	[end_date_timestamp] [datetime] NULL,
	[LoadedOn] [datetime] NULL,
	[LoadedBy] [nvarchar](255) NULL
) ON [PRIMARY]
GO
