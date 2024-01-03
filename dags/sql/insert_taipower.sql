INSERT INTO real_time_power_data (
    update_time,
    unit_type,
    unit_name,
    installed_capacity_mw,
    net_generation_mw,
    generation_to_capacity_ratio,
    comments
) VALUES {{params.taipower_sql_values}};