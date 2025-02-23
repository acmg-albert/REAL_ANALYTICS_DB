-- Create the main table for Zillow affordability data
CREATE TABLE IF NOT EXISTS zillow_new_homeowner_affordability_down_20pct (
    region_id TEXT NOT NULL,
    size_rank INTEGER NOT NULL,
    region_name TEXT NOT NULL,
    region_type TEXT NOT NULL,
    state_name TEXT,
    date DATE NOT NULL,
    new_home_affordability_down_20pct DOUBLE PRECISION,
    last_update_time TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    PRIMARY KEY (region_id, date)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_zillow_affordability_region 
ON zillow_new_homeowner_affordability_down_20pct(region_id);

CREATE INDEX IF NOT EXISTS idx_zillow_affordability_date 
ON zillow_new_homeowner_affordability_down_20pct(date);

CREATE INDEX IF NOT EXISTS idx_zillow_affordability_region_type 
ON zillow_new_homeowner_affordability_down_20pct(region_type);

CREATE INDEX IF NOT EXISTS idx_zillow_affordability_state 
ON zillow_new_homeowner_affordability_down_20pct(state_name);

-- Enable RLS
ALTER TABLE zillow_new_homeowner_affordability_down_20pct ENABLE ROW LEVEL SECURITY;

-- Create read policy (allow all users to read)
CREATE POLICY "Enable read access for all users" 
ON zillow_new_homeowner_affordability_down_20pct
FOR SELECT 
USING (true);

-- Create write policy (only allow service_role to write)
CREATE POLICY "Enable write access for service role" 
ON zillow_new_homeowner_affordability_down_20pct
FOR ALL
USING (auth.jwt() ->> 'role' = 'service_role')
WITH CHECK (auth.jwt() ->> 'role' = 'service_role');

-- Grant access to authenticated and anon users
GRANT SELECT ON zillow_new_homeowner_affordability_down_20pct TO authenticated, anon; 