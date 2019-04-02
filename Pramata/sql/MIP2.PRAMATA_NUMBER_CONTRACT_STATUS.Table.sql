USE [BI_MIP]
GO
/****** Object:  Table [MIP2].[PRAMATA_NUMBER_CONTRACT_STATUS]    Script Date: 1/8/2019 2:51:20 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [MIP2].[PRAMATA_NUMBER_CONTRACT_STATUS](
	[pramata_number] [int] NULL,
	[Contract_Status_Number] [int] NULL,
	[Additional_Information] [varchar](500) NULL,
	[Referenced_Document] [varchar](255) NULL,
	[Status] [varchar](255) NULL,
	[LoadedOn] [datetime] NULL,
	[LoadedBy] [varchar](100) NULL
) ON [PRIMARY]
GO
