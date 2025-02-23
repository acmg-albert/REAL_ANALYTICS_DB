-- Drop existing view if exists
DROP MATERIALIZED VIEW IF EXISTS zillow_new_homeowner_affordability_down_20pct_view;

-- Create materialized view
CREATE MATERIALIZED VIEW zillow_new_homeowner_affordability_down_20pct_view AS
SELECT DISTINCT ON (region_id, date)
    region_name,
    region_type,
    region_id,
    size_rank,
    state_name,
    date,
    new_home_affordability_down_20pct,
    last_update_time
FROM zillow_new_homeowner_affordability_down_20pct
ORDER BY region_id, date, last_update_time DESC
WITH DATA;

-- Create indexes for better query performance
CREATE INDEX idx_zillow_affordability_view_region 
ON zillow_new_homeowner_affordability_down_20pct_view(region_id);

CREATE INDEX idx_zillow_affordability_view_date 
ON zillow_new_homeowner_affordability_down_20pct_view(date);

CREATE INDEX idx_zillow_affordability_view_region_type 
ON zillow_new_homeowner_affordability_down_20pct_view(region_type);

CREATE INDEX idx_zillow_affordability_view_state 
ON zillow_new_homeowner_affordability_down_20pct_view(state_name);

-- Grant access to authenticated and anon users
GRANT SELECT ON zillow_new_homeowner_affordability_down_20pct_view TO authenticated, anon; 