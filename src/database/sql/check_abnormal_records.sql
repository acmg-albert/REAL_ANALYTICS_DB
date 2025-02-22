-- 检查rent_estimates表中的所有可疑记录
SELECT 
    location_name,
    location_type,
    location_fips_code,
    state,
    county,
    metro,
    year_month,
    rent_estimate_overall,
    rent_estimate_1br,
    rent_estimate_2br,
    last_update_time
FROM apartment_list_rent_estimates
WHERE 
    -- 检查测试数据
    location_fips_code IN ('12345', '67890')
    OR location_name LIKE '%Test%'
    OR location_name LIKE '%test%'
    -- 检查可能的异常值
    OR rent_estimate_overall < 100  -- 异常低的租金
    OR rent_estimate_overall > 10000  -- 异常高的租金
    -- 检查可能的错误数据
    OR location_type NOT IN ('City', 'State', 'County', 'Metro')
    OR location_fips_code ~ '[^0-9]'  -- FIPS码应该只包含数字
    OR length(location_fips_code) NOT IN (2, 5)  -- FIPS码应该是2位(州)或5位(城市/县)
ORDER BY last_update_time DESC; 