-- Create materialized view for rent estimates
DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_rent_estimates_1_3;

CREATE MATERIALIZED VIEW db_view_apartment_list_rent_estimates_1_3 AS
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

-- Create indexes for better query performance
CREATE INDEX idx_mv_rent_estimates_location 
ON db_view_apartment_list_rent_estimates_1_3(location_fips_code);

CREATE INDEX idx_mv_rent_estimates_year_month 
ON db_view_apartment_list_rent_estimates_1_3(year_month);

-- Grant access to authenticated and anon users
GRANT SELECT ON db_view_apartment_list_rent_estimates_1_3 TO authenticated, anon; 