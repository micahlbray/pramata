USE [BI_MIP]
GO
/****** Object:  Table [MIP2].[PRAMATA_NUMBER_SIGNATORY]    Script Date: 1/8/2019 2:51:22 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [MIP2].[PRAMATA_NUMBER_SIGNATORY](
	[pramata_number] [int] NULL,
	[Signatory_Number] [int] NULL,
	[Customer_Signatory_Name] [nvarchar](255) NULL,
	[Customer_Signatory_Title] [nvarchar](255) NULL,
	[LoadedOn] [datetime] NULL,
	[LoadedBy] [nvarchar](255) NULL
) ON [PRIMARY]
GO
