CREATE OR REPLACE TABLE `pgibidwh.Financials.dm_day_month_year`
-- Direct partition by the DAY column
AS
    SELECT 
        FORMAT_DATE('%Y-%m-%d',DATE '2020-01-01' + INTERVAL x DAY) AS dates,
        EXTRACT(YEAR FROM DATE '2020-01-01' + INTERVAL x DAY) AS year,
        FORMAT_TIMESTAMP('%B', DATE '2020-01-01' + INTERVAL x DAY) AS month_name,  -- Full month name
        EXTRACT(MONTH FROM DATE '2020-01-01' + INTERVAL x DAY) AS month_mun,        -- Month number
        EXTRACT(DAY FROM LAST_DAY(DATE '2020-01-01' + INTERVAL x DAY)) AS days_ofmonth,  -- Number of days in the month
        EXTRACT(DAY FROM DATE '2020-01-01' + INTERVAL x DAY) AS day,                  -- Day of the month
        EXTRACT(WEEK FROM DATE '2020-01-01' + INTERVAL x DAY) AS week,                -- Week number
        EXTRACT(DAYOFWEEK FROM DATE '2020-01-01' + INTERVAL x DAY) AS dayweek_number,  -- Day of the week (1=Sun, 2=Mon, ..., 7=Sat)
        FORMAT_TIMESTAMP('%A', DATE '2020-01-01' + INTERVAL x DAY) AS weekday_name,    -- Weekday name (Sunday, Monday, ...)
        FORMAT_DATE('%Y-%m', DATE '2020-01-01' + INTERVAL x DAY) AS yearmonth,         -- Year-Month format (YYYY-MM)
        
        -- Week start dates
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(DATE '2020-01-01' + INTERVAL x DAY, INTERVAL EXTRACT(DAYOFWEEK FROM DATE '2020-01-01' + INTERVAL x DAY) - 1 DAY)) AS Week_sun,  -- Sunday of the week
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(DATE '2020-01-01' + INTERVAL x DAY, INTERVAL EXTRACT(DAYOFWEEK FROM DATE '2020-01-01' + INTERVAL x DAY) - 2 DAY)) AS Week_mon,  -- Monday of the week
        
        -- Month calculations
        FORMAT_DATE('%Y-%m-%d', DATE_ADD(DATE '2020-01-01' + INTERVAL x DAY, INTERVAL 1 MONTH)) AS nextmonth,  -- Next month same day
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(DATE '2020-01-01' + INTERVAL x DAY, INTERVAL 1 MONTH)) AS lastmonth,   -- Previous month same day
        
        -- Week number in the year
        EXTRACT(WEEK FROM DATE '2020-01-01' + INTERVAL x DAY) AS weeknum_year,
        
        -- Quarter calculation
        CASE 
            WHEN EXTRACT(MONTH FROM DATE '2020-01-01' + INTERVAL x DAY) IN (1, 2, 3) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM DATE '2020-01-01' + INTERVAL x DAY) IN (4, 5, 6) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM DATE '2020-01-01' + INTERVAL x DAY) IN (7, 8, 9) THEN 'Q3'
            ELSE 'Q4'
        END AS quarter,
        
        EXTRACT(QUARTER FROM DATE '2020-01-01' + INTERVAL x DAY) AS quarter_num   -- Numeric quarter (1-4)
        
    FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE '2024-12-31', DATE '2020-01-01', DAY))) AS x;
