-- Create the vacancy index table
CREATE TABLE IF NOT EXISTS apartment_list_vacancy_index (
    location_name TEXT NOT NULL,
    location_type TEXT NOT NULL,
    location_fips_code TEXT NOT NULL,
    population INTEGER,
    state TEXT,
    county TEXT,
    metro TEXT,
    year_month TEXT NOT NULL,
    vacancy_index DOUBLE PRECISION,
    last_update_time TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    PRIMARY KEY (location_fips_code, year_month)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_vacancy_index_location 
ON apartment_list_vacancy_index(location_fips_code);

CREATE INDEX IF NOT EXISTS idx_vacancy_index_year_month 
ON apartment_list_vacancy_index(year_month);

CREATE INDEX IF NOT EXISTS idx_vacancy_index_location_type 
ON apartment_list_vacancy_index(location_type);

CREATE INDEX IF NOT EXISTS idx_vacancy_index_state 
ON apartment_list_vacancy_index(state);

-- Enable RLS
ALTER TABLE apartment_list_vacancy_index ENABLE ROW LEVEL SECURITY;

-- Create read policy (allow all users to read)
CREATE POLICY "Enable read access for all users" 
ON apartment_list_vacancy_index
FOR SELECT 
USING (true);

-- Create write policy (only allow service_role to write)
CREATE POLICY "Enable write access for service role" 
ON apartment_list_vacancy_index
FOR ALL
USING (auth.jwt() ->> 'role' = 'service_role')
WITH CHECK (auth.jwt() ->> 'role' = 'service_role');

-- Grant access to authenticated and anon users
GRANT SELECT ON apartment_list_vacancy_index TO authenticated, anon; 