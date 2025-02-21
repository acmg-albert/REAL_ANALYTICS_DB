-- Create materialized view for vacancy index
DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_vacancy_index_1_3;

CREATE MATERIALIZED VIEW db_view_apartment_list_vacancy_index_1_3 AS
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

-- Create indexes for better query performance
CREATE INDEX idx_mv_vacancy_index_location 
ON db_view_apartment_list_vacancy_index_1_3(location_fips_code);

CREATE INDEX idx_mv_vacancy_index_year_month 
ON db_view_apartment_list_vacancy_index_1_3(year_month);

-- Grant access to authenticated and anon users
GRANT SELECT ON db_view_apartment_list_vacancy_index_1_3 TO authenticated, anon; 