-- 删除time_on_market表中的测试数据
DELETE FROM apartment_list_time_on_market
WHERE location_fips_code IN ('12345', '67890')
OR location_name LIKE '%Test%';

-- 删除rent_estimates表中的测试数据
DELETE FROM apartment_list_rent_estimates
WHERE location_fips_code IN ('12345', '67890')
OR location_name LIKE '%Test%';

-- 刷新物化视图
REFRESH MATERIALIZED VIEW apartment_list_time_on_market_view;
REFRESH MATERIALIZED VIEW apartment_list_rent_estimates_view;

-- 验证清理结果
SELECT 'time_on_market' as table_name, COUNT(*) as test_records_count
FROM apartment_list_time_on_market
WHERE location_fips_code IN ('12345', '67890')
OR location_name LIKE '%Test%'
UNION ALL
SELECT 'rent_estimates' as table_name, COUNT(*) as test_records_count
FROM apartment_list_rent_estimates
WHERE location_fips_code IN ('12345', '67890')
OR location_name LIKE '%Test%'; 