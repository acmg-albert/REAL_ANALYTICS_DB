-- Create the main table for Zillow median sale price data
CREATE TABLE IF NOT EXISTS zillow_median_sale_price_all_home (
    region_id TEXT NOT NULL,
    size_rank INTEGER NOT NULL,
    region_name TEXT NOT NULL,
    region_type TEXT NOT NULL,
    state_name TEXT,
    date DATE NOT NULL,
    median_sale_price_all_home DOUBLE PRECISION,
    last_update_time TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    PRIMARY KEY (region_id, date)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_zillow_median_sale_price_region 
ON zillow_median_sale_price_all_home(region_id);

CREATE INDEX IF NOT EXISTS idx_zillow_median_sale_price_date 
ON zillow_median_sale_price_all_home(date);

CREATE INDEX IF NOT EXISTS idx_zillow_median_sale_price_region_type 
ON zillow_median_sale_price_all_home(region_type);

CREATE INDEX IF NOT EXISTS idx_zillow_median_sale_price_state 
ON zillow_median_sale_price_all_home(state_name);

-- Enable RLS
ALTER TABLE zillow_median_sale_price_all_home ENABLE ROW LEVEL SECURITY;

-- Create read policy (allow all users to read)
CREATE POLICY "Enable read access for all users" 
ON zillow_median_sale_price_all_home
FOR SELECT 
USING (true);

-- Create write policy (only allow service_role to write)
CREATE POLICY "Enable write access for service role" 
ON zillow_median_sale_price_all_home
FOR ALL
USING (auth.jwt() ->> 'role' = 'service_role')
WITH CHECK (auth.jwt() ->> 'role' = 'service_role');

-- Grant access to authenticated and anon users
GRANT SELECT ON zillow_median_sale_price_all_home TO authenticated, anon; 