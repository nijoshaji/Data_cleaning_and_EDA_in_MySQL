-- Explaratory Data Analysis
-- cleaned data is layoffs_staging2

select * from layoffs_staging2;

-- Find the max and min layoff
select max(total_laid_off) as highest_layoff, min(total_laid_off) as lowest_layoff
from layoffs_staging2;

-- retreive all data for highest and lowest layoff
Select * 
from layoffs_staging2
where total_laid_off = (select max(total_laid_off) from layoffs_staging2);
Select * 
from layoffs_staging2
where total_laid_off = (select min(total_laid_off) from layoffs_staging2);

-- Find the company that laid off complete staff. Ordered by number of staff laid off
select * 
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

-- Find the company that laid off complete staff. Ordered by total funding they received
select * 
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- total number of staff laid of by each company.
select company, sum(total_laid_off) as no_laid_off
from layoffs_staging2
group by company order by 2 desc;

-- find the date range of the data set
select max(`date`), min(`date`)
from layoffs_staging2;

-- Which industry has been impacted the most.
select industry, sum(total_laid_off) as no_laid_off
from layoffs_staging2
group by industry order by 2 desc;

-- impact assessment by country
select country, sum(total_laid_off) as no_laid_off
from layoffs_staging2
group by country order by 2 desc;

-- impact assessment by year
select year(`date`), sum(total_laid_off) as no_laid_off
from layoffs_staging2
group by year(`date`) order by 1 desc;

-- progression of layoffs. monthly rolling sum
with monthly_totals as (
select substr(`date`,1,7) as month_date, sum(total_laid_off) as total
from layoffs_staging2
where substr(`date`,1,7)  is not null
-- can add a filter for company,country,industry
group by month_date order by 1 asc
)
select month_date, total, sum(total) over(order by month_date) as monthly_rolling_sum
from monthly_totals;

-- year wise layoff for each company
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`) 
order by sum(total_laid_off) desc;

-- top 5 companies for each year
with company_year_totals as (
select company as company, year(`date`) as c_year, sum(total_laid_off) as total_offs
from layoffs_staging2
-- can add filter for any country/industry
group by company, year(`date`) 
) , company_ranks as 
(
select * ,
dense_rank() over(partition by c_year order by total_offs desc) as rankings
from company_year_totals 
where c_year is not null
)
select *
from company_ranks
where rankings <=5;

-- top 5 countries with layoff each year
with country_year_totals as (
select country as country, year(`date`) as c_year, sum(total_laid_off) as total_offs
from layoffs_staging2
-- can add filter for any industry
group by country, year(`date`) 
) , country_ranks as 
(
select * ,
dense_rank() over(partition by c_year order by total_offs desc) as rankings
from country_year_totals 
where c_year is not null
)
select *
from country_ranks
where rankings <=5;
