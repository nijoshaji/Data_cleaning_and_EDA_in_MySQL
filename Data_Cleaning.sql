-- Create a staging table for data cleaning and modifications.
create table layoffs_staging like layoffs_raw;

-- Copy all values from raw table to staging table
insert into layoffs_staging
select * from layoffs_raw;

-- Part 1. Remove duplicate rows from the dataset
-- Add row number to each row. Store it in a CTE to filter the duplicates. 
-- rows with row_num >1 are duplicate
with ranked_table as 
(
select *, row_number() over 
(
partition by company, location, industry, total_laid_off, percentage_laid_off,
 `date`, stage, country, funds_raised_millions
 ) as row_num
from layoffs_staging
)
select * from ranked_table
where row_num >1;

-- CTE row cannot be updated/deleted. So we store the CTE data to a new table and update it.
-- Create a new staging table with row_num column
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert all values of staging date plus row number to staging 2. 
insert into layoffs_staging2
select *, row_number() over 
(
partition by company, location, industry, total_laid_off, percentage_laid_off,
 `date`, stage, country, funds_raised_millions
 ) as row_num
from layoffs_staging;

-- Remove all row with row_num >1 from staging 2 data. Turn off safe updates
set SQL_SAFE_UPDATES = 0;
delete
from layoffs_staging2
where row_num>1;

-- Check to see if duplicates removed
select * from layoffs_staging2 where row_num >1;

-- Part 2. Standardizing data
-- It includes removing extra spaces, dots or characters. 
-- Or standardizing category/group names

-- trim extra space from columns
update layoffs_staging2
set company = trim(company);

-- Remove trailing dots from country
update layoffs_staging2 
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Standardising industry name. Crypto has 3 diff names. consolidate it to one.

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

-- Part 3. Convert date to correct format for time-series analysis
-- Change the dates to date format first

update layoffs_staging2 
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- Change the data type for the date column
alter table layoffs_staging2
modify column `date` date;

-- Part 4. Dealing with nulls/blanks
-- Try to fill missing data with similar data in data set.

-- check for null/blank values. Found null/blank values in industry column
select distinct industry
from layoffs_staging2 order by 1;

-- Convert the blank industry to null values. To make it easier to substitute values
update layoffs_staging2
set industry = null
where industry = '';

-- self join to see if there are similar values

select t1.company, t1.industry, t2.company, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2 
on t1.company = t2.company
where t1.industry is null 
and  t2.industry is not null;


-- Update the null values with substitute values
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and  t2.industry is not null;

-- if the values cannot be substituted. 
-- Either delete the rows(if not to be used in analysis) or leave it null

-- Part 5. Removing nulls/blanks
-- the rows with no layoff data (total laid of and percentage layoff) 
-- can be removed as it does not have data to analyse
select * 
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- Part 6. Removing unwanted columns from the dataset
-- drop the unwanted rows from the table

alter table layoffs_staging2
drop column row_num;

-- Check the cleaned data
select * from layoffs_staging2;
