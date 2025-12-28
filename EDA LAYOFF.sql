SELECT * 
FROM layoff_PROJECT.layoff_staging2;

--- EDA---
SELECT MAX(total_laid_off)
FROM Layoff_PROJECT.layoff_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM layoff_PROJECT.layoff_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- WHICH COMAPNY HAD 1 (100%)  LAID OFF---
SELECT *
FROM layoff_PROJECT.layoff_staging2
WHERE  percentage_laid_off = 1;

SELECT COUNT(percentage_laid_off = 1)
FROM  layoff_PROJECT.layoff_staging2;


SELECT *
FROM layoff_PROJECT.layoff_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, total_laid_off
FROM layoff_project.layoff_staging
ORDER BY 2 DESC
LIMIT 7;

-- comapany with most laid off--
SELECT company, SUM(total_laid_off)
FROM layoff_project.layoff_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- laid off location--
SELECT location, SUM(total_laid_off)
FROM layoff_project.layoff_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;


-- this it total in the past 3 years or in the dataset--

SELECT country, SUM(total_laid_off)
FROM layoff_project.layoff_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoff_project.layoff_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM layoff_project.layoff_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoff_project.layoff_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at 

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoff_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoff_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoff_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;