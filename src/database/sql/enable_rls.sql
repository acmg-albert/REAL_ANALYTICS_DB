-- Enable Row Level Security
ALTER TABLE apartment_list_rent_estimates ENABLE ROW LEVEL SECURITY;

-- Create policies for read access
CREATE POLICY "Enable read access for all users" ON apartment_list_rent_estimates
    FOR SELECT
    TO authenticated, anon
    USING (true);

-- Create policies for write access (only service_role can write)
CREATE POLICY "Enable write access for service_role only" ON apartment_list_rent_estimates
    FOR INSERT
    TO service_role
    USING (true);

CREATE POLICY "Enable update access for service_role only" ON apartment_list_rent_estimates
    FOR UPDATE
    TO service_role
    USING (true);

-- Revoke all permissions from public
REVOKE ALL ON apartment_list_rent_estimates FROM public;

-- Grant specific permissions
GRANT SELECT ON apartment_list_rent_estimates TO authenticated, anon;
GRANT INSERT, UPDATE ON apartment_list_rent_estimates TO service_role; 