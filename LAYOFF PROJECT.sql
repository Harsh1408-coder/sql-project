create database layoff_project;
use layoff_project;

create table layoff_project.layoff_staging
like layoff_project.layoffs;

insert layoff_staging
select * from layoff_project.layoffs;

--- removes duplicates
select *from layoff_project.layoff_staging;

SELECT company, industry, total_laid_off,`date`,
row_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layoff_project.layoff_staging;

SELECT *FROM (
SELECT company, industry, total_laid_off,`date`,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layoff_project.layoff_staging)
duplicates
WHERE row_num > 1;

select* from layoff_project.layoff_staging
where company ="oda";

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoff_project.layoff_staging
) duplicates
WHERE 
	row_num > 1;
    
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoff_project.layoff_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoff_project.layoff_staging
)
DELETE FROM layoff_project.layoff_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

ALTER TABLE layoff_project.layoff_staging ADD row_num INT;

    SELECT *
FROM layoff_project.layoff_staging
;

--- STAGING 2 TABLE MADE BECAUSE CLEANED VERSION OF FIRST STAGING ---

CREATE TABLE `layoff_project`.`layoff_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `layoff_project`.`layoff_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		LAYOFF_PROJECT.layoff_staging;
        
    DELETE FROM layoff_project.layoff_staging2
WHERE row_num >= 2;

---- standardizedata ---

SELECT * 
FROM layoff_project.layoff_staging2;

-- check all null values
SELECT DISTINCT industry
FROM layoff_project.layoff_staging2
ORDER BY industry;

SELECT *
FROM layoff_project.layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM layoff_project.layoff_staging2
WHERE company LIKE 'Bally%';

-- nothing wrong here
SELECT *
FROM layoff_project.layoff_staging2
WHERE company LIKE 'airbnb%';


UPDATE layoff_project.layoff_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoff_project.layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoff_project.layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;


-- we also need to look at 

SELECT *
FROM layoff_project.layoff_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM layoff_project.layoff_staging2
ORDER BY country;

UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoff_project.layoff_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM layoff_project.layoff_staging2;

-- we can use str to date to update this field
UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoff_project.layoff_staging2;

--- removes null values or blanks---

select *
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoff_staging2
set industry = null
where industry = '';

select *
from layoff_staging2
where industry is  null
or industry = '';

select *
from layoff_staging2
where company = 'Airbnb';

select t1.industry,t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;
    
select *
from layoff_staging2
where company = 'Airbnb';    
    
    
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values..

-- REMOVE COL AND ROWS--
SELECT *
FROM layoff_project.layoff_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoff_project.layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

--- delete useless data

DELETE FROM layoff_project.layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoff_project.layoff_staging2;

ALTER TABLE layoff_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoff_project.layoff_staging2;



