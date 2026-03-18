/*
=============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema from the 'bronze' schema.
  Actions Performed:
    - Truncates silver tables.
    - Inserts transformed and cleansed data from bronze into silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values. 

Usage Example:
    EXEC silver.load_silver;
=============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
	PRINT '==============================='
	PRINT 'Loading Silver Layer'
	PRINT '==============================='

-- Loading silver.lahman_batting
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_batting';
	TRUNCATE TABLE silver.lahman_batting;
	PRINT '>> Inserting Data Into: silver.lahman_batting;
INSERT INTO silver.lahman_batting (
	playerID,
	yearID,
	stint,
	teamID,
	lgID,
	G,
	PA,
	AB,
	R,
	H,
	[1B],
	[2B],
	[3B],
	HR,
	RBI,
	[AVG],
	OBP,
	SLG,
	OPS,
	[TB],
	SB,
	CS,
	BB,
	SO,
	IBB,
	HBP,
	SH,
	SF)
SELECT
	playerID,
	yearID,
	stint,
	teamID,
	lgLD AS lgID,
	G,
	CAST((ISNULL(AB, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0) + ISNULL(SH, 0) + ISNULL(SF, 0)) AS INT) AS PA,
	CAST(ISNULL(AB, 0) AS INT) AS AB,
	CAST(ISNULL(R, 0) AS INT) AS R,
	CAST(ISNULL(H, 0) AS INT) AS H,
	CAST(ISNULL(H,0) - (ISNULL([2B], 0) + ISNULL([3B], 0) + ISNULL(HR, 0)) AS INT) AS [1B],
	CAST(ISNULL([2B], 0) AS INT) AS [2B],
	CAST(ISNULL([3B], 0) AS INT) AS [3B],
	CAST(ISNULL(HR, 0) AS INT) AS HR,
	CAST(ISNULL(RBI, 0) AS INT) AS RBI,
	CAST
		(ROUND(ISNULL((ISNULL(H,0) * 1.0) / NULLIF(ISNULL(AB, 0), 0), 0), 3) AS DECIMAL(4,3)) AS AVG,
	CAST
		(ROUND(ISNULL(
			(ISNULL(H, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0)) * 1.0 / NULLIF(ISNULL(AB, 0) 
				+ ISNULL(BB, 0) + ISNULL(HBP, 0) + ISNULL(SF, 0), 0), 0), 3) AS DECIMAL(4,3)) AS OBP,
	CAST
		(ROUND(ISNULL(
			(((ISNULL(H, 0) - (ISNULL([2B], 0) + ISNULL([3B], 0) + ISNULL(HR, 0)))
			+ (2 * ISNULL([2B], 0)) + (3 * ISNULL([3B], 0)) + (4 * ISNULL(HR, 0)))
				* 1.0 / NULLIF(AB, 0)), 0), 3) AS DECIMAL(4,3)) AS SLG,
	CAST
		(ROUND(
			ISNULL((ISNULL(H, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0)) * 1.0 / 
               NULLIF(ISNULL(AB, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0) + ISNULL(SF, 0), 0), 0)
        + 
			ISNULL((((ISNULL(H, 0) - (ISNULL([2B], 0) + ISNULL([3B], 0) + ISNULL(HR, 0))) 
               + (2 * ISNULL([2B], 0)) + (3 * ISNULL([3B], 0)) + (4 * ISNULL(HR, 0))) 
               * 1.0 / NULLIF(ISNULL(AB, 0), 0)), 0), 3) AS DECIMAL(4,3)) AS OPS,
	CAST
		(((ISNULL(H,0) - (ISNULL([2B], 0) + ISNULL([3B],0) + ISNULL(HR, 0))) 
		+ (2 * ISNULL([2B], 0)) 
		+ (3 * ISNULL([3B], 0)) 
		+ (4 * ISNULL(HR, 0))) AS INT) AS TB,
	CAST(ISNULL(SB, 0) AS INT) AS SB,
	CAST(ISNULL(CS, 0) AS INT) AS CS,
	CAST(ISNULL(BB, 0) AS INT) AS BB,
	CAST(ISNULL(SO, 0) AS INT) AS SO,
	CAST(ISNULL(IBB, 0) AS INT) AS IBB,
	CAST(ISNULL(HBP, 0) AS INT) AS HBP,
	CAST(ISNULL(SH, 0) AS INT) AS SH,
	CAST(ISNULL(SF, 0) AS INT) AS SF
FROM bronze.lahman_batting
SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

-- Loading silver.lahman_pitching
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_pitching';
	TRUNCATE TABLE silver.lahman_pitching;
	PRINT '>> Inserting Data Into: silver.lahman_pitching';
INSERT INTO silver.lahman_pitching (
	playerID,
	yearID,
	stint,
	teamID,
	lgID,
	W,
	L,
	G,
	GS,
	CG,
	SHO,
	SV,
	IP,
	IP_Math,
	H,
	ER,
	HR,
	BB,
	SO,
	BAOpp,
	ERA,
	WHIP,
	FIP,
	[K%],
	[BB%],
	[K/BB],
	[H/9],
	[HR/9],
	IBB,
	WP,
	HBP,
	BK,
	BFP,
	GF,
	R,
	SH,
	SF,
	GIDP)
SELECT
	playerID,
	yearID,
	stint,
	teamID,
	lgID,
	CAST(ISNULL(W, 0) AS INT) AS W,
	CAST(ISNULL(L, 0) AS INT) AS L,
	CAST(ISNULL(G, 0) AS INT) AS G,
	CAST(ISNULL(GS, 0) AS INT) AS GS,
	CAST(ISNULL(CG, 0) AS INT) AS CG,
	CAST(ISNULL(SHO, 0) AS INT) AS SHO,
	CAST(ISNULL(SV, 0) AS INT) AS SV,
	CAST(CONCAT((ISNULL(IPouts, 0) / 3), '.', (ISNULL(IPouts, 0) % 3)) AS DECIMAL(6,1)) AS IP,
	CAST(ISNULL(IPouts, 0) / 3.0 AS DECIMAL(6,2)) AS IP_Math,
	CAST(ISNULL(H, 0) AS INT) AS H,
	CAST(ISNULL(ER, 0) AS INT) ER,
	CAST(ISNULL(HR, 0) AS INT) HR,
	CAST(ISNULL(BB, 0) AS INT) BB,
	CAST(ISNULL(SO, 0) AS INT) SO,
	CAST(ISNULL(BAOpp, 0) AS DECIMAL(4,3)) AS BAOpp,
	CAST(ISNULL(ERA, 0) AS DECIMAL(5,2)) AS ERA,
	CAST((ISNULL(BB, 0) + ISNULL(H, 0)) * 3.0 / NULLIF(ISNULL(IPouts, 0), 0) AS DECIMAL(5,2)) AS WHIP,
	CAST(((13.0 * ISNULL(HR, 0)) + (3.0 * ISNULL(BB, 0)) - (2.0 * ISNULL(SO, 0))) / NULLIF(ISNULL(IPouts, 0) / 3.0,0) + 3.2 AS DECIMAL(5,2)) AS FIP,
	ISNULL(CAST((ISNULL(SO, 0) * 100.0) / NULLIF(ISNULL(BFP, 0), 0) AS DECIMAL(5,2)), 0) AS [K%],
	ISNULL(CAST((ISNULL(BB, 0) * 100.0) / NULLIF(ISNULL(BFP, 0), 0) AS DECIMAL(5,2)), 0) AS [BB%],
	CAST((ISNULL(SO, 0) * 1.0) / NULLIF(ISNULL(BB, 0), 0) AS DECIMAL(5,2)) AS [K/BB],
	CAST((27.0 * ISNULL(H, 0)) / NULLIF(ISNULL(IPouts, 0), 0) AS DECIMAL(5,2)) AS [H/9],
	CAST((27.0 * ISNULL(HR, 0)) / NULLIF(ISNULL(IPouts, 0), 0) AS DECIMAL(5,2)) AS [HR/9],
	CAST(ISNULL(IBB, 0) AS INT) AS IBB,
	CAST(ISNULL(WP, 0) AS INT) AS WP,
	CAST(ISNULL(HBP, 0) AS INT) AS HBP,
	CAST(ISNULL(BK, 0) AS INT) AS BK,
	CAST(ISNULL(BFP, 0) AS INT) AS BFP,
	CAST(ISNULL(GF, 0) AS INT) AS GF,
	CAST(ISNULL(R, 0) AS INT) AS R,
	CAST(ISNULL(SH, 0) AS INT) AS SH,
	CAST(ISNULL(SF, 0) AS INT) AS SF,
	CAST(ISNULL(GIDP, 0) AS INT) AS GIDP
FROM bronze.lahman_pitching
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

-- Loading silver.lahman_fielding
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_fielding';
	TRUNCATE TABLE silver.lahman_fielding;
	PRINT '>> Inserting Data Into: silver.lahman_fielding';
WITH PositionAverages AS (
    -- Step 1: Calculate the baseline (League Average) for each year and position
    SELECT 
        yearID, 
        POS,
        -- Total Plays / Total Outs = Average Plays per Out
        (SUM(CAST(ISNULL(PO, 0) AS FLOAT)) + SUM(CAST(ISNULL(A, 0) AS FLOAT))) 
        / NULLIF(SUM(CAST(ISNULL(InnOuts, 0) AS FLOAT)), 0) AS AvgPlaysPerOut
    FROM bronze.lahman_fielding
    GROUP BY yearID, POS
)
INSERT INTO silver.lahman_fielding (
	playerID,
	yearID,
	stint,
	teamID,
	lgID,
	POS,
	G,
	GS,
	InningsPlayed,
	PO,
	A,
	E,
	CH,
	DP,
	PB,
	WP,
	SB,
	CS,
	ZR,
	FPCT,
	[RF/9],
	DRS_Lite)
SELECT
f.playerID,
f.yearID,
f.stint,
f.teamID,
f.lgID,
f.POS,
CAST(ISNULL(f.G, 0) AS INT),
CAST(ISNULL(f.GS, 0) AS INT),
CAST(ISNULL(f.InnOuts, 0) / 3.0 AS DECIMAL(7,2)),
CAST(ISNULL(f.PO, 0) AS INT),
CAST(ISNULL(f.A, 0) AS INT),
CAST(ISNULL(f.E, 0) AS INT),
CAST(ISNULL(f.PO, 0) + ISNULL(f.A, 0) + ISNULL(f.E, 0) AS INT),
CAST(ISNULL(f.DP, 0) AS INT),
CAST(ISNULL(f.PB, 0) AS INT),
CAST(ISNULL(f.WP, 0) AS INT),
CAST(ISNULL(f.SB, 0) AS INT),
CAST(ISNULL(f.CS, 0) AS INT),
CAST(ISNULL(f.ZR, 0) AS DECIMAL(6,3)),
CAST((ISNULL(f.PO, 0) + ISNULL(f.A, 0)) * 1.0 / NULLIF(ISNULL(f.PO, 0) + ISNULL(f.A, 0) + ISNULL(f.E,0), 0) AS DECIMAL(5,3)) AS FPCT,
CAST(((ISNULL(f.PO, 0) + ISNULL(f.A,0)) * 27.0) / NULLIF(ISNULL(f.InnOuts, 0), 0) AS DECIMAL(8,2)) AS [RF/9],
CAST(
        (
            (ISNULL(f.PO, 0) + ISNULL(f.A, 0)) 
            - (ISNULL(f.InnOuts, 0) * pa.AvgPlaysPerOut)
        ) * 0.75 
        AS DECIMAL(10,2)
    ) AS DRS_Lite
FROM bronze.lahman_fielding AS f
LEFT JOIN PositionAverages AS pa
ON f.yearID = pa.yearID
AND f.POS = pa.POS;
SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

-- Loading silver.lahman_people
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_people';
	TRUNCATE TABLE silver.lahman_people;
	PRINT '>> Inserting Data Into: silver.lahman_people';
INSERT INTO silver.lahman_people (
	playerID,
	birthDate,
	birthCountry,
	birthState,
	birthCity,
	deathDate,
	deathCountry,
	deathState,
	deathCity,
	nameFirst,
	nameLast,
	nameGiven,
	fullName,
	height,
	Height_FT_IN,
	weight,
	bats,
	throws,
	debut,
	finalGame,
	CareerLength,
	bbrefID,
	retroID)
SELECT
	TRIM(playerID) AS playerID,
	TRY_CAST(
		CAST(birthYear AS NVARCHAR(50)) + '-' +
		CAST(ISNULL(birthMonth, 1) AS NVARCHAR(50)) + '-' +
		CAST(ISNULL(birthDay, 1) AS NVARCHAR(50)) AS DATE) AS birthDate,
	TRIM(birthCountry) AS birthCountry,
	TRIM(birthState) AS birthState,
	TRIM(birthCity) AS birthCity,
	TRY_CAST(
		CAST(deathYear AS NVARCHAR(50)) + '-' +
		CAST(ISNULL(deathMonth, 1) AS NVARCHAR(50)) + '-' +
		CAST(ISNULL(deathDay, 1) AS NVARCHAR(50)) AS DATE) AS deathDate,
	TRIM(deathCountry) AS deathCountry,
	TRIM(deathState) AS deathState,
	TRIM(deathCity) AS deathCity,
	TRIM(nameFirst) AS nameFirst,
	TRIM(nameLast) AS nameLast,
	TRIM(nameGiven) AS nameGiven,
	CONCAT(TRIM(nameFirst), ' ', TRIM(nameLast)) AS fullName,
	CAST(ISNULL(height, 0) AS INT) AS height,
	CAST(height / 12 AS NVARCHAR(50)) + '''' + CAST(height % 12 AS NVARCHAR(50)) + '"' AS Height_FT_IN,
	CAST(ISNULL(weight, 0) AS INT) AS weight,
	UPPER(TRIM(ISNULL(bats, 'U'))) AS bats,
	UPPER(TRIM(ISNULL(throws, 'U'))) AS throws,
	TRY_CAST(debut AS DATE) AS debut,
	TRY_CAST(finalGame AS DATE) AS finalGame,
	DATEDIFF(year, debut, finalGame) AS CareerLength,
	TRIM(bbrefID) AS bbrefID,
	TRIM(retroID) AS retroID
FROM bronze.lahman_people
SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

-- Loading silver.lahman_teams
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_teams';
	TRUNCATE TABLE silver.lahman_teams;
	PRINT '>> Inserting Data Into: silver.lahman_teams';
INSERT INTO silver.lahman_teams (
	yearID,
	lgID,
	teamID,
	franchID,
	divID,
	[Rank],
	G,
	GHome,
	W,
	L,
	WinPct,
	DivWin,
	WCWin,
	LgWin,
	WSWin,
	R,
	RA,
	RunDiff,
	AB,
	H,
	[2B],
	[3B],
	HR,
	BB,
	SO,
	SB,
	CS,
	HBP,
	SF,
	[AVG],
	OBP,
	SLG,
	OPS,
	[TB],
	ER,
	ERA,
	WHIP,
	FIP,
	CG,
	SHO,
	SV,
	IPouts,
	HA,
	HRA,
	BBA,
	SOA,
	E,
	DP,
	FPct,
	[name],
	park,
	attendance,
	BPF,
	PPF,
	teamIDBR,
	teamIDlahman45,
	teamIDretro)
SELECT
	yearID AS yearID,
	TRIM(lgID) AS lgID,
	TRIM(teamID) AS teamID,
	TRIM(franchID) AS franchID,
	TRIM(divID) AS divID,
	ISNULL([Rank], 0) AS [Rank],
	ISNULL(G, 0) AS G,
	ISNULL(GHome, 0) AS GHome,
	ISNULL(W, 0) AS W,
	ISNULL(L, 0) AS L,
	CAST((ISNULL(W, 0) * 1.0) / (NULLIF(ISNULL(W, 0) + ISNULL(L, 0), 0)) AS DECIMAL(5,3)) AS WinPct,
	TRIM(DivWin) AS DivWin,
	TRIM(WCWin) AS WCWin,
	TRIM(LgWin) AS LgWin,
	TRIM(WSWin) AS WSWin,
	ISNULL(R, 0) AS R,
	ISNULL(RA, 0) AS RA,
	ISNULL(R, 0) - ISNULL(RA, 0) AS RunDiff,
	ISNULL(AB, 0) AS AB,
	ISNULL(H, 0) AS H,
	ISNULL([2B], 0) AS [2B],
	ISNULL([3B], 0) AS [3B],
	ISNULL(HR, 0) AS HR,
	ISNULL(BB, 0) AS BB,
	ISNULL(SO, 0) AS SO,
	ISNULL(SB, 0) AS SB,
	ISNULL(CS, 0) AS CS,
	ISNULL(HBP, 0) AS HBP,
	ISNULL(SF, 0) AS SF,
	CAST(ROUND(ISNULL((ISNULL(H,0) * 1.0) / NULLIF(ISNULL(AB, 0), 0), 0), 3) AS DECIMAL(4,3)) AS AVG,
	CAST(ROUND(ISNULL((ISNULL(H, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0)) * 1.0 / NULLIF(ISNULL(AB, 0) 
				+ ISNULL(BB, 0) + ISNULL(HBP, 0) + ISNULL(SF, 0), 0), 0), 3) AS DECIMAL(4,3)) AS OBP,
	CAST(ROUND(ISNULL((((ISNULL(H, 0) - (ISNULL([2B], 0) + ISNULL([3B], 0) + ISNULL(HR, 0)))
			+ (2 * ISNULL([2B], 0)) + (3 * ISNULL([3B], 0)) + (4 * ISNULL(HR, 0))) * 1.0 / NULLIF(AB, 0)), 0), 3) AS DECIMAL(4,3)) AS SLG,
	CAST(ROUND(ISNULL((ISNULL(H, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0)) * 1.0 / 
               NULLIF(ISNULL(AB, 0) + ISNULL(BB, 0) + ISNULL(HBP, 0) + ISNULL(SF, 0), 0), 0)
        + 
			ISNULL((((ISNULL(H, 0) - (ISNULL([2B], 0) + ISNULL([3B], 0) + ISNULL(HR, 0))) 
               + (2 * ISNULL([2B], 0)) + (3 * ISNULL([3B], 0)) + (4 * ISNULL(HR, 0))) 
               * 1.0 / NULLIF(ISNULL(AB, 0), 0)), 0), 3) AS DECIMAL(5,3)) AS OPS,
	CAST
		(((ISNULL(H,0) - (ISNULL([2B], 0) + ISNULL([3B],0) + ISNULL(HR, 0))) 
		+ (2 * ISNULL([2B], 0)) 
		+ (3 * ISNULL([3B], 0)) 
		+ (4 * ISNULL(HR, 0))) AS INT) AS TB,
	ISNULL(ER, 0) AS ER,
	CAST(ISNULL(ERA, 0) AS DECIMAL(5,2)) AS ERA,
	CAST((ISNULL(BBA, 0) + ISNULL(HA, 0)) * 3.0 / NULLIF(ISNULL(IPouts, 0), 0) AS DECIMAL(5,2)) AS WHIP,
	CAST(((13.0 * ISNULL(HR, 0)) + (3.0 * ISNULL(BBA, 0)) - (2.0 * ISNULL(SO, 0))) / NULLIF(ISNULL(IPouts, 0) / 3.0,0) + 3.2 AS DECIMAL(5,2)) AS FIP,
	ISNULL(CG, 0) AS CG,
	ISNULL(SHO, 0) AS SHO,
	ISNULL(SV, 0) AS SV,
	ISNULL(IPouts, 0) AS IPouts,
	ISNULL(HA, 0) AS HA,
	ISNULL(HRA, 0) AS HRA,
	ISNULL(BBA, 0) AS BBA,
	ISNULL(SOA, 0) AS SOA,
	ISNULL(E, 0) AS E,
	ISNULL(DP, 0) AS DP,
	CAST(ISNULL(FP, 0) AS DECIMAL(6,4)) AS FPct,
	TRIM(name) AS name,
	TRIM(park) AS park,
	ISNULL(attendance, 0) AS attendance,
	ISNULL(BPF, 0) AS BPF,
	ISNULL(PPF, 0) AS PPF,
	TRIM(teamIDBR) AS teamIDBR,
	TRIM(teamIDlahman45) AS teamIDlahman45,
	TRIM(teamIDretro) AS teamIDretro
FROM bronze.lahman_teams
SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

-- Loading silver.lahman_managers
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_managers';
	TRUNCATE TABLE silver.lahman_managers;
	PRINT '>> Inserting Data Into: silver.lahman_managers';
INSERT INTO silver.lahman_managers (
	playerID,
	yearID,
	teamID,
	lgID,
	stint,
	G,
	W,
	L,
	WinPct,
	[Rank],
	plyrMgr)

SELECT
	TRIM(playerID) AS playerID,
	yearID,
	TRIM(teamID) AS teamID,
	TRIM(lgID) AS lgID,
	ISNULL(inseason, 0) AS stint,
	ISNULL(G, 0) AS G,
	ISNULL(W, 0) AS W,
	ISNULL(L, 0) AS L,
	CAST(ISNULL(W, 0) * 1.0 / NULLIF(ISNULL(W, 0) + ISNULL(L, 0), 0) AS DECIMAL(5,3)) AS WinPct,
	ISNULL([Rank], 0) AS [Rank],
	CASE WHEN TRIM(UPPER(plyrMgr)) = 'Y' THEN 1 ELSE 0 END AS plyrMgr
FROM bronze.lahman_managers
SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

-- Loading silver.lahman_salaries
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.lahman_salaries';
	TRUNCATE TABLE silver.lahman_salaries;
	PRINT '>> Inserting Data Into: silver.lahman_salaries';
INSERT INTO silver.lahman_salaries (
	yearID,
	teamID,
	lgId,
	playerID,
	salary,
	salary_m)

SELECT
	yearID,
	TRIM(teamID) AS teamID,
	TRIM(lgID) AS lgID,
	TRIM(playerID) AS playerID,
	CAST(ISNULL(salary,0) AS BIGINT) AS salary,
	CAST(ISNULL(salary, 0) / 1000000.0 AS DECIMAL(10,2)) AS salary_m
FROM bronze.lahman_salaries
SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ---------';

SET @batch_end_time = GETDATE();
	PRINT '=============================='
	PRINT 'Loading Silver Layer is Complete';
	PRINT '	 - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '==============================' 

END TRY
BEGIN CATCH
	PRINT '=============================='
	PRINT 'ERROR OCCURED DURING LOADING BRONZER LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '=============================='
END CATCH 
END



