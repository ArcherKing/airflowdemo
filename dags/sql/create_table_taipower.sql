-- create real_time_power_data table
CREATE TABLE IF NOT EXISTS real_time_power_data (
    update_time varchar(30), -- 更新時間
    unit_type varchar(50), -- 機組類型
    unit_name varchar(50), -- 機組名稱
    installed_capacity_mw varchar(30), -- 裝置容量(MW)
    net_generation_mw varchar(30), -- 淨發電量(MW)
    generation_to_capacity_ratio varchar(30), -- 淨發電量/裝置容量比(%)
    comments varchar(255) -- 備註
);