-- 删除物化视图
DROP MATERIALIZED VIEW IF EXISTS apartment_list_rent_estimates_view;
DROP MATERIALIZED VIEW IF EXISTS apartment_list_vacancy_index_view;
DROP MATERIALIZED VIEW IF EXISTS apartment_list_time_on_market_view;

-- 清空所有表
TRUNCATE TABLE apartment_list_rent_estimates;
TRUNCATE TABLE apartment_list_vacancy_index;
TRUNCATE TABLE apartment_list_time_on_market;

-- 重新创建物化视图
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

-- 验证表是否为空
SELECT 
    'rent_estimates' as table_name, 
    COUNT(*) as record_count 
FROM apartment_list_rent_estimates
UNION ALL
SELECT 
    'vacancy_index' as table_name, 
    COUNT(*) as record_count 
FROM apartment_list_vacancy_index
UNION ALL
SELECT 
    'time_on_market' as table_name, 
    COUNT(*) as record_count 
FROM apartment_list_time_on_market; 