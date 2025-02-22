-- 重命名rent_estimates视图
DROP MATERIALIZED VIEW IF EXISTS apartment_list_rent_estimates_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_rent_estimates_1_3 CASCADE;
CREATE MATERIALIZED VIEW apartment_list_rent_estimates_view AS
SELECT 
    location_name,
    location_type,
    location_fips_code,
    population,
    state,
    county,
    metro,
    year_month,
    rent_estimate_overall,
    rent_estimate_1br,
    rent_estimate_2br,
    last_update_time
FROM apartment_list_rent_estimates
WITH DATA;

-- 重命名vacancy_index视图
DROP MATERIALIZED VIEW IF EXISTS apartment_list_vacancy_index_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_vacancy_index_1_3 CASCADE;
CREATE MATERIALIZED VIEW apartment_list_vacancy_index_view AS
SELECT 
    location_name,
    location_type,
    location_fips_code,
    population,
    state,
    county,
    metro,
    year_month,
    vacancy_index,
    last_update_time
FROM apartment_list_vacancy_index
WITH DATA;

-- 重命名time_on_market视图
DROP MATERIALIZED VIEW IF EXISTS apartment_list_time_on_market_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_time_on_market_1_3 CASCADE;
CREATE MATERIALIZED VIEW apartment_list_time_on_market_view AS
SELECT DISTINCT ON (location_fips_code, year_month)
    location_name,
    'City' as location_type,
    location_fips_code,
    population,
    state,
    county,
    metro,
    year_month,
    time_on_market,
    last_update_time
FROM apartment_list_time_on_market
ORDER BY location_fips_code, year_month, last_update_time DESC
WITH DATA;

-- 创建索引
CREATE INDEX idx_rent_estimates_view_location 
ON apartment_list_rent_estimates_view(location_fips_code);

CREATE INDEX idx_rent_estimates_view_year_month 
ON apartment_list_rent_estimates_view(year_month);

CREATE INDEX idx_vacancy_index_view_location 
ON apartment_list_vacancy_index_view(location_fips_code);

CREATE INDEX idx_vacancy_index_view_year_month 
ON apartment_list_vacancy_index_view(year_month);

CREATE INDEX idx_time_on_market_view_location 
ON apartment_list_time_on_market_view(location_fips_code);

CREATE INDEX idx_time_on_market_view_year_month 
ON apartment_list_time_on_market_view(year_month);

-- 授权
GRANT SELECT ON apartment_list_rent_estimates_view TO authenticated, anon;
GRANT SELECT ON apartment_list_vacancy_index_view TO authenticated, anon;
GRANT SELECT ON apartment_list_time_on_market_view TO authenticated, anon; 