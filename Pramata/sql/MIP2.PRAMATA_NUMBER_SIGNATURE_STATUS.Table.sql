USE [BI_MIP]
GO
/****** Object:  Table [MIP2].[PRAMATA_NUMBER_SIGNATURE_STATUS]    Script Date: 1/8/2019 2:51:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [MIP2].[PRAMATA_NUMBER_SIGNATURE_STATUS](
	[pramata_number] [int] NULL,
	[Signature_Status_Number] [int] NULL,
	[Signature_Status_Status] [varchar](255) NULL,
	[Signature_Status_Which_Party] [varchar](500) NULL,
	[LoadedOn] [datetime] NULL,
	[LoadedBy] [varchar](100) NULL
) ON [PRIMARY]
GO
