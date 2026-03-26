use world_layoffs;

select *
from layoffs;

-- step 1: remove duplicates
-- step 2 standardise data; spelling issues
-- step 3 Null values/Blank values
-- step 4 remove unnecessary columns/rows

-- create staging file
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

-- inserting into staging table
insert into layoffs_staging
select *
from layoffs;

-- step 1 remove duplicates
select *, 
ROW_NUMBER () OVER (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
FROM layoffs_staging;

with remove_duplicates AS
(select *, 
ROW_NUMBER () OVER (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
FROM layoffs_staging)
select * 
from remove_duplicates
where row_num > 1;

select *
from remove_duplicates
where company = 'Casper';

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

insert into layoffs_staging2
(select *, 
ROW_NUMBER () OVER (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
FROM layoffs_staging);

delete
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2;


-- standardize data
select distinct company, TRIM(company)
from layoffs_staging2;

update layoffs_staging2
set company = TRIM(company);

select * 
from layoffs_staging2;

select Distinct (industry)
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct(country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = TRIM(TRAILING '.' from country)
where country like 'United States%'
;

select `date`
from layoffs_staging2;

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
modify column `date` DATE;

SELECT *
from layoffs_staging2;


-- populate Null values/ blanks
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '') and (t2.industry is not null and t2.industry != '');

select distinct company, industry
from layoffs_staging2
where industry is null or industry = '';

	-- removing null rows from data
delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

select *
from layoffs_staging2;

-- data cleaning process
-- 1. Remove duplicate
-- 2. Standardise data ; spelling, data type formats
-- 3. Dealing with null values
-- 4. Remove unnecessary columns

-- removing columns
Alter table layoffs_staging2
DROP COLUMN row_num;


-- Exploratory Data Analysis
-- Basic exploratory
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off Desc;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company;

select min(`date`), max(`date`)
from layoffs_staging2;

select industry, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by industry
order by total_laid_off desc;


select country, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by country
order by total_laid_off desc;

select YEAR(`date`) as year_of_lay_off, sum(total_laid_off)
from layoffs_staging2
group by year_of_lay_off
order by 1 desc;


with company_year as
(
select company, YEAR(`date`) as year_of_layoff, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by year_of_layoff, company
), Company_layoff_ranking as
(
select *, 
dense_rank() over(partition by year_of_layoff order by total_laid_off desc) as ranking
from company_year
where year_of_layoff is not null
)


