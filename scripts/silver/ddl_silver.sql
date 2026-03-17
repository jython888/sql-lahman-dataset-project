


IF OBJECT_ID('silver.lahman_batting', 'U') IS NOT NULL
	DROP TABLE silver.lahman_batting;

CREATE TABLE silver.lahman_batting (
	playerID NVARCHAR(50),
	yearID INT,
	stint INT,
	teamID NVARCHAR(50),
	lgID NVARCHAR(50),
	G INT,
	PA INT,
	AB INT,
	R INT,
	H INT,
	[1B] INT,
	[2B] INT,
	[3B] INT,
	HR INT,
	RBI INT,
	[AVG] DECIMAL(4,3),
	OBP DECIMAL(4,3),
	SLG DECIMAL(4,3),
	OPS DECIMAL(4,3),
	[TB] INT,
	SB INT,
	CS INT,
	BB INT,
	SO INT,
	IBB INT,
	HBP INT,
	SH INT,
	SF INT
);
