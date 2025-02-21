-- Drop existing view
DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_time_on_market_1_3;

-- Create materialized view for time on market data
CREATE MATERIALIZED VIEW db_view_apartment_list_time_on_market_1_3 AS
SELECT DISTINCT ON (location_fips_code, year_month)
    location_name,
    'City' as location_type,  -- 统一使用大写的City
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

-- Create indexes for better query performance
CREATE INDEX idx_mv_time_on_market_location 
ON db_view_apartment_list_time_on_market_1_3(location_fips_code);

CREATE INDEX idx_mv_time_on_market_year_month 
ON db_view_apartment_list_time_on_market_1_3(year_month);

-- Grant access to authenticated and anon users
GRANT SELECT ON db_view_apartment_list_time_on_market_1_3 TO authenticated, anon; 