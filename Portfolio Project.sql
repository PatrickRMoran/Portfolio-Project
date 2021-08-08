--World Heath Organization Covid 19 Data from Kaggle. This dataset contains confirmed, death and 
--recovered data from most countries including daily reported numbers from January 22nd to September 28th.

-- Skills used: Aggregate Function, Aliases, Converting Datatypes, Creating Views, CTEs, Joins, Subqueries and Temp Tables  

--First I will test the databases

USE Covid19WHO
SELECT *
FROM covid_19_data$
	WHERE [Province/State] like 'Washingt_n'
	ORDER BY 5

SELECT *
FROM time_series_covid_19_confirmed$
	WHERE [Country/Region] like 'United%'

SELECT  *
FROM time_series_covid_19_deaths_US$

-- Confirmed cases in the United States from January 22nd ton September 29th

SELECT   
    SUM([1/22/20]) as Jan22, 
	SUM([3/25/20]) as Mar25, 
	SUM([5/26/20]) as May26, 
	SUM([9/21/20]) as Sep21
FROM time_series_covid_19_confirmed_US$

--The largest population of recovered cases

SELECT [Country/Region], 
	MAX([9/28/20]) as Sept_28
FROM time_series_covid_19_recovered$
	GROUP BY [Country/Region]
	ORDER BY MAX([9/28/20]) DESC

--Select Data we will focus on for this project

SELECT [Country/Region],  
	[Province/State], 
	Confirmed, 
	Deaths, 
	Recovered
FROM covid_19_data$ 
	WHERE Confirmed >= 1
	AND Deaths >= 1
	AND Recovered >= 1
	ORDER BY 3

-- The countries with the most deaths

SELECT [Country/Region] , 
	SUM(Deaths) as Deaths
FROM covid_19_data$
	GROUP BY [Country/Region]
	ORDER BY SUM(deaths) DESC

--Now we find the administrative subdivision with the most deaths.

SELECT [Country/Region], 
	[Province/State], 
	MAX(Deaths) as Deaths
FROM covid_19_data$
	GROUP BY [Country/Region], [Province/State]
	ORDER BY MAX(deaths) DESC

--Finding the top Countries and subdivitions with the highest infection numbers and the date.

SELECT TOP 100
	[Country/Region],  
	[Province/State], 
	MAX(confirmed) as Comfirmed, 
	MAX(CONVERT(DATE,[Last Update])) as Date
FROM covid_19_data$
	GROUP BY [Country/Region], [Province/State]
	ORDER BY 3 DESC

-- Finding the death rate as a percentage from the United States


SELECT [Province/State] , 
	Confirmed, 
	Deaths, 
	ROUND((deaths/confirmed) *100,2) as Death_Rate_From_Infected,
	[ObservationDate]
FROM covid_19_data$
	WHERE Deaths >= 1
	AND Confirmed >= 1
	AND [Country/Region] = 'US'
	AND [Last Update] is not null
	AND [Last Update] = (
		SELECT MAX([Last Update])
		FROM covid_19_data$
		) 
	AND Confirmed >= 10000
	ORDER BY Death_Rate_From_Infected DESC	
	
	
-- Using CTE to investigate cases in China and Austrailia

WITH CTE_ChAu as (

SELECT *
FROM time_series_covid_19_confirmed$
	WHERE [Country/Region] Like 'China'
	OR [Country/Region] Like 'Australia'
)

SELECT TOP 10 
	[Province/State],
	MAX([9/28/20]) AS ConfirmedCases_Sept28
FROM CTE_ChAu
	GROUP BY [Province/State]
	ORDER By MAX([9/28/20]) DESC

-- Temp table compairing Utah and Califonia

DROP TABLE IF EXISTS #UTCA
CREATE TABLE #UTCA (
StateUSA varchar(250),
DeathSept19 float null ,
DeathJuly28 float null,
DeathMay26 float null,
ConfirmedSept19 float null,
ConfirmedJuly28 float null,
ConfirmeedMay26 float null,
)

INSERT INTO #UTCA
SELECT ConUS.Province_State as StateUSA,
	SUM(DeaUS.[9/19/20]) AS Death_Sept19th,
	SUM(ConUS.[9/19/20]) AS Confirmed_Sept19th,
	SUM(DeaUS.[7/28/20]) AS Death_July28th,
	SUM(ConUS.[7/28/20]) AS Confirmed_July28th,
	SUM(DeaUS.[5/26/20]) AS Death_May26th,
	SUM(ConUS.[5/26/20]) AS Confirmed_May26th
FROM time_series_covid_19_confirmed_US$ as ConUS	
JOIN time_series_covid_19_deaths_US$ as DeaUS
	ON ConUS.[Province_State] = DeaUS.[Province_State] 
	WHERE ConUS.[5/26/20] >= 1
	AND DeaUS.[5/26/20] >= 1
	GROUP BY ConUS.Province_State, 
	DeaUS.[9/19/20],
	ConUS.[9/19/20],
	DeaUS.[7/28/20],
	ConUS.[7/28/20],
	DeaUS.[5/26/20],
	ConUS.[5/26/20]
	

SELECT StateUSA,
	ConfirmeedMay26,
	DeathMay26,
	ConfirmedJuly28,
	DeathJuly28,
	ConfirmedSept19,
	DeathSept19
FROM #UTCA
	WHERE StateUSA Like 'California'
	ORDER BY ConfirmedSept19 DESC,
	DeathSept19 DESC
	
SELECT StateUSA,
	ConfirmeedMay26,
	DeathMay26,
	ConfirmedJuly28,
	DeathJuly28,
	ConfirmedSept19,
	DeathSept19
FROM #UTCA
	WHERE StateUSA Like 'Utah'
	ORDER BY ConfirmedSept19 DESC,
	DeathSept19 DESC


--Using a join to find historical death percentage from confirmed cases

SELECT ConUS.[Province_State],
	SUM(ConUS.[5/26/20]) As ConfirmedCases_May26th,
	SUM(DeaUS.[5/26/20]) As Deaths_May26th,
	ROUND((SUM(DeaUS.[5/26/20])/SUM(ConUS.[5/26/20]))*100,2) as Death_Rate_From_Confirmed_May26th,
	SUM(ConUS.[7/28/20]) As ConfirmedCases_Jul28th,
	SUM(DeaUS.[7/28/20]) As Deaths_Jul28th,
	ROUND(SUM(DeaUS.[7/28/20])/(SUM(ConUS.[7/28/20]))*100,2) as Death_Rate_From_Confirmed_Jul28th,
	SUM(ConUS.[9/20/20]) As ConfirmedCases_Sept20th,
	SUM(DeaUS.[9/20/20]) As Deaths_Sept20th,
	ROUND(SUM(DeaUS.[9/20/20])/(SUM(ConUS.[9/20/20]))*100,2) as Death_Rate_From_Confirmed_Sept20th 
FROM time_series_covid_19_confirmed_US$ as ConUS	
JOIN time_series_covid_19_deaths_US$ as DeaUS
	ON ConUS.[Province_State] = DeaUS.[Province_State] 
	WHERE ConUS.[5/26/20] >= 1
	AND DeaUS.[5/26/20] >= 1
	--HAVING ROUND(SUM(DeaUS.[7/28/20])/(SUM(ConUS.[7/28/20]))*100,2) >= 100
	--AND (SUM(DeaUS.[5/26/20]))/SUM(ConUS.[5/26/20]) >= 100
	--AND (SUM(DeaUS.[9/20/20]))/SUM(ConUS.[9/20/20]) >= 100
	GROUP BY ConUS.[Province_State]
	ORDER BY SUM(DeaUS.[9/20/20])/SUM(ConUS.[9/20/20])DESC

-- Creating a view for later visualization

CREATE VIEW HistoricalDeathRate as (

SELECT ConUS.[Province_State],
	SUM(ConUS.[5/26/20]) As ConfirmedCases_May26th,
	SUM(DeaUS.[5/26/20]) As Deaths_May26th,
	ROUND((SUM(DeaUS.[5/26/20])/SUM(ConUS.[5/26/20]))*100,2) as Death_Rate_From_Confirmed_May26th,
	SUM(ConUS.[7/28/20]) As ConfirmedCases_Jul28th,
	SUM(DeaUS.[7/28/20]) As Deaths_Jul28th,
	ROUND(SUM(DeaUS.[7/28/20])/(SUM(ConUS.[7/28/20]))*100,2) as Death_Rate_From_Confirmed_Jul28th,
	SUM(ConUS.[9/20/20]) As ConfirmedCases_Sept20th,
	SUM(DeaUS.[9/20/20]) As Deaths_Sept20th,
	ROUND(SUM(DeaUS.[9/20/20])/(SUM(ConUS.[9/20/20]))*100,2) as Death_Rate_From_Confirmed_Sept20th 
FROM time_series_covid_19_confirmed_US$ as ConUS	
JOIN time_series_covid_19_deaths_US$ as DeaUS
	ON ConUS.[Province_State] = DeaUS.[Province_State] 
	WHERE ConUS.[5/26/20] >= 1
	AND DeaUS.[5/26/20] >= 1
	GROUP BY ConUS.[Province_State]
	)

SELECT TOP 10
*
FROM HistoricalDeathRate
	WHERE Death_Rate_From_Confirmed_Jul28th <=100
	AND Death_Rate_From_Confirmed_May26th <= 100
	AND Death_Rate_From_Confirmed_Sept20th <= 100
	ORDER BY Death_Rate_From_Confirmed_Sept20th DESC
