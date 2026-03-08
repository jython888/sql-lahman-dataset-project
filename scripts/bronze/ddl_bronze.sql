/*
===============================================================================
DDL SCRIPT: Create Bronze Tables
===============================================================================
Script Purpose:
	This script creates tables in the 'bronze' schema, dropping existing tables
	if they already exist.
	Run this script to re-define the DDL structure of the 'bronze' tables. 
===============================================================================
*/

IF OBJECT_ID ('bronze.lahman_batting' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_batting;
GO
  
CREATE TABLE bronze.lahman_batting (
	playerID NVARCHAR(100),
	yearID NVARCHAR(100),
	stint INT,
	teamID NVARCHAR(100),
	lgID NVARCHAR(100),
	G INT,
	AB INT,
	R INT,
	H INT,
	[2B] INT,
	[3B] INT,
	HR INT,
	RBI INT,
	SB INT,
	CS INT,
	BB INT,
	SO INT,
	IBB INT,
	HBP INT,
	SH INT,
	SF INT
); 
GO
  
IF OBJECT_ID ('bronze.lahman_pitching' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_pitching;
GO
  
CREATE TABLE bronze.lahman_pitching (
	playerID NVARCHAR(100),
	yearID INT,
	stint INT,
	teamID NVARCHAR(100),
	lgID NVARCHAR(100),
	W INT,
	L INT,
	G INT,
	GS INT,
	CG INT,
	SHO INT,
	SV INT,
	H INT,
	ER INT,
	HR INT,
	BB INT,
	SO INT,
	BAOpp FLOAT,
	ERA FLOAT,
	IBB INT,
	WP INT,
	HBP INT,
	BK INT,
	BFP INT,
	GF INT,
	R INT,
	SH INT,
	SF INT
); 
GO
  
IF OBJECT_ID ('bronze.lahman_fielding' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_fielding;
GO
  
CREATE TABLE bronze.lahman_fielding (
	playerID, NVARCHAR(100),
	yearID INT,
	stint INT,
	teamID NVARCHAR(100),
	lgID NVARCHAR(100),
	POS NVARCHAR(100),
	G INT,
	GS INT,
	PO INT,
	A INT,
	E INT
); 
GO
  
IF OBJECT_ID ('bronze.lahman_parks' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_parks;
GO
CREATE TABLE bronze.lahman_parks (
	ID INT,
	parkkey NVARCHAR(100),
	parkname NVARCHAR(100),
	city NVARCHAR(100),
	state NVARCHAR(100),
	country NVARCHAR(100)
); 
GO
  
IF OBJECT_ID ('bronze.lahman_people' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_people;
GO
  
CREATE TABLE bronze.lahman_people (
	playerID NVARCHAR(100),
	birthYear INT,
	birthMonth INT,
	birthDay INT,
	birthCity NVARCHAR(100),
	birthCountry NVARCHAR(100),
	birthState NVARCHAR(100),
	deathYear INT,
	deathMonth INT,
	deathDay INT,
	deathCountry NVARCHAR(100),
	deathState NVARCHAR(100),
	deathCity NVARCHAR(100),
	nameFirst NVARCHAR(100,
	nameLast NVARCHAR(100),
	nameGiven NVARCHAR(100),
	weight INT,
	height INT,
	bats NVARCHAR(100),
	throws NVARCHAR(100),
	debut DATE,
	bbrefID NVARCHAR(100),
	finalGame DATE,
	retroID NVARCHAR(100)
); 
GO
  
IF OBJECT_ID ('bronze.lahman_salaries' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_salaries;
GO
  
CREATE TABLE bronze.lahman_salaries (
	yearID INT,
	teamID NVARCHAR(100),
	lgID NVARCHAR(100),
	playerID NVARCHAR(100),
	salary INT
); 
GO
  
IF OBJECT_ID ('bronze.lahman_teams' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_teams;
GO
  
CREATE TABLE bronze.lahman_teams (
	yearID INT,
	lgID NVARCHAR(100),
	teamID NVARCHAR(100),
	franchID NVARCHAR(100),
	G INT,
	W INT,
	L INT,
	R INT,
	AB INT,
	H INT,
	[2B] INT,
	[3B] INT,
	HR INT,
	BB INT,
	SO INT,
	SB INT,
	CS INT,
	HBP INT,
	SF INT,
	RA INT,
	ER INT,
	ERA FLOAT,
	CG INT,
	SHO INT,
	SV INT,
	HA INT,
	HRA INT,
	BBA INT,
	SOA INT,
	E INT,
	DP INT,
	FP FLOAT,
	name NVARCHAR(100),
	park NVARCHAR(100),
	attendance INT
); 
GO
  
IF OBJECT_ID ('bronze.lahman_managers' , 'U') IS NOT NULL
	DROP TABLE bronze.lahman_managers;
GO
  
CREATE TABLE bronze.lahman_managers (
	playerID NVARCHAR(100),
	yearID INT,
	teamID NVARCHAR(100),
	lgID NVARCHAR(100),
	G INT,
	W INT,
	L INT,
	rank INT
);
GO
