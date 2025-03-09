-- Drop existing view if exists
DROP MATERIALIZED VIEW IF EXISTS zillow_median_sale_price_all_home_view;

-- Create materialized view
CREATE MATERIALIZED VIEW zillow_median_sale_price_all_home_view AS
SELECT DISTINCT ON (region_id, date)
    region_name,
    region_type,
    region_id,
    size_rank,
    state_name,
    date,
    median_sale_price_all_home,
    last_update_time
FROM zillow_median_sale_price_all_home
ORDER BY region_id, date, last_update_time DESC
WITH DATA;

-- Create indexes for better query performance
CREATE INDEX idx_zillow_median_sale_price_view_region 
ON zillow_median_sale_price_all_home_view(region_id);

CREATE INDEX idx_zillow_median_sale_price_view_date 
ON zillow_median_sale_price_all_home_view(date);

CREATE INDEX idx_zillow_median_sale_price_view_region_type 
ON zillow_median_sale_price_all_home_view(region_type);

CREATE INDEX idx_zillow_median_sale_price_view_state 
ON zillow_median_sale_price_all_home_view(state_name);

-- Grant access to authenticated and anon users
GRANT SELECT ON zillow_median_sale_price_all_home_view TO authenticated, anon; 