-- 启用RLS
ALTER TABLE apartment_list_rent_estimates ENABLE ROW LEVEL SECURITY;

-- 删除现有策略
DROP POLICY IF EXISTS "Enable read access for all users" ON apartment_list_rent_estimates;
DROP POLICY IF EXISTS "Enable write access for service role" ON apartment_list_rent_estimates;

-- 创建读取策略（允许所有用户读取）
CREATE POLICY "Enable read access for all users" 
ON apartment_list_rent_estimates
FOR SELECT 
USING (true);

-- 创建写入策略（仅允许service_role写入）
CREATE POLICY "Enable write access for service role" 
ON apartment_list_rent_estimates
FOR ALL
USING (auth.jwt() ->> 'role' = 'service_role')
WITH CHECK (auth.jwt() ->> 'role' = 'service_role'); 