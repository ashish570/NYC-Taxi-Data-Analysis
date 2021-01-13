'''sql
/****** Object:  Table [dbo].[tip]    ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[tip](
	[quarter] [int] NULL,
	[tip_amount] [float] NULL,
	[year] [int] NOT NULL
) ON [PRIMARY]
GO


/****** Object:  Table [dbo].[tripSpeed]  ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[tripSpeed](
	[calendarHour] [int] NULL,
	[calendarDay] [int] NOT NULL,
	[calendarMonth] [int] NOT NULL,
	[calendarYear] [int] NOT NULL,
	[maxSpeed] [float] NULL
) ON [PRIMARY]
GO

'''
