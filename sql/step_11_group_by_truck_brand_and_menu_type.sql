-- Step 11: Group by Truck Brand and Menu Type
-- Shows menu item counts by truck brand and menu type

-- Group by truck brand name and menu type
SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;