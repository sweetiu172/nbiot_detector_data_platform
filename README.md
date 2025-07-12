# nbiot_detector_data_platform


## Trino queries
```sql
CREATE SCHEMA hive.iot_lakehouse
WITH (location = 's3a://iot-time-series/');
```

```sql
CREATE TABLE hive.iot_lakehouse.traffic (
    "MI_dir_L5_weight" DOUBLE ,
    "MI_dir_L5_mean" DOUBLE ,
    "MI_dir_L5_variance" DOUBLE ,
    "MI_dir_L3_weight" DOUBLE ,
    "MI_dir_L3_mean" DOUBLE ,
    "MI_dir_L3_variance" DOUBLE ,
    "MI_dir_L1_weight" DOUBLE ,
    "MI_dir_L1_mean" DOUBLE ,
    "MI_dir_L1_variance" DOUBLE ,
    "MI_dir_L0.1_weight" DOUBLE ,
    "MI_dir_L0.1_mean" DOUBLE ,
    "MI_dir_L0.1_variance" DOUBLE ,
    "MI_dir_L0.01_weight" DOUBLE ,
    "MI_dir_L0.01_mean" DOUBLE ,
    "MI_dir_L0.01_variance" DOUBLE ,
    "H_L5_weight" DOUBLE ,
    "H_L5_mean" DOUBLE ,
    "H_L5_variance" DOUBLE ,
    "H_L3_weight" DOUBLE ,
    "H_L3_mean" DOUBLE ,
    "H_L3_variance" DOUBLE ,
    "H_L1_weight" DOUBLE ,
    "H_L1_mean" DOUBLE ,
    "H_L1_variance" DOUBLE ,
    "H_L0.1_weight" DOUBLE ,
    "H_L0.1_mean" DOUBLE ,
    "H_L0.1_variance" DOUBLE ,
    "H_L0.01_weight" DOUBLE ,
    "H_L0.01_mean" DOUBLE ,
    "H_L0.01_variance" DOUBLE ,
    "HH_L5_weight" DOUBLE ,
    "HH_L5_mean" DOUBLE ,
    "HH_L5_std" DOUBLE ,
    "HH_L5_magnitude" DOUBLE ,
    "HH_L5_radius" DOUBLE ,
    "HH_L5_covariance" DOUBLE ,
    "HH_L5_pcc" DOUBLE ,
    "HH_L3_weight" DOUBLE ,
    "HH_L3_mean" DOUBLE ,
    "HH_L3_std" DOUBLE ,
    "HH_L3_magnitude" DOUBLE ,
    "HH_L3_radius" DOUBLE ,
    "HH_L3_covariance" DOUBLE ,
    "HH_L3_pcc" DOUBLE ,
    "HH_L1_weight" DOUBLE ,
    "HH_L1_mean" DOUBLE ,
    "HH_L1_std" DOUBLE ,
    "HH_L1_magnitude" DOUBLE ,
    "HH_L1_radius" DOUBLE ,
    "HH_L1_covariance" DOUBLE ,
    "HH_L1_pcc" DOUBLE ,
    "HH_L0.1_weight" DOUBLE ,
    "HH_L0.1_mean" DOUBLE ,
    "HH_L0.1_std" DOUBLE ,
    "HH_L0.1_magnitude" DOUBLE ,
    "HH_L0.1_radius" DOUBLE ,
    "HH_L0.1_covariance" DOUBLE ,
    "HH_L0.1_pcc" DOUBLE ,
    "HH_L0.01_weight" DOUBLE ,
    "HH_L0.01_mean" DOUBLE ,
    "HH_L0.01_std" DOUBLE ,
    "HH_L0.01_magnitude" DOUBLE ,
    "HH_L0.01_radius" DOUBLE ,
    "HH_L0.01_covariance" DOUBLE ,
    "HH_L0.01_pcc" DOUBLE ,
    "HH_jit_L5_weight" DOUBLE ,
    "HH_jit_L5_mean" DOUBLE ,
    "HH_jit_L5_variance" DOUBLE ,
    "HH_jit_L3_weight" DOUBLE ,
    "HH_jit_L3_mean" DOUBLE ,
    "HH_jit_L3_variance" DOUBLE ,
    "HH_jit_L1_weight" DOUBLE ,
    "HH_jit_L1_mean" DOUBLE ,
    "HH_jit_L1_variance" DOUBLE ,
    "HH_jit_L0.1_weight" DOUBLE ,
    "HH_jit_L0.1_mean" DOUBLE ,
    "HH_jit_L0.1_variance" DOUBLE ,
    "HH_jit_L0.01_weight" DOUBLE ,
    "HH_jit_L0.01_mean" DOUBLE ,
    "HH_jit_L0.01_variance" DOUBLE ,
    "HpHp_L5_weight" DOUBLE ,
    "HpHp_L5_mean" DOUBLE ,
    "HpHp_L5_std" DOUBLE ,
    "HpHp_L5_magnitude" DOUBLE ,
    "HpHp_L5_radius" DOUBLE ,
    "HpHp_L5_covariance" DOUBLE ,
    "HpHp_L5_pcc" DOUBLE ,
    "HpHp_L3_weight" DOUBLE ,
    "HpHp_L3_mean" DOUBLE ,
    "HpHp_L3_std" DOUBLE ,
    "HpHp_L3_magnitude" DOUBLE ,
    "HpHp_L3_radius" DOUBLE ,
    "HpHp_L3_covariance" DOUBLE ,
    "HpHp_L3_pcc" DOUBLE ,
    "HpHp_L1_weight" DOUBLE ,
    "HpHp_L1_mean" DOUBLE ,
    "HpHp_L1_std" DOUBLE ,
    "HpHp_L1_magnitude" DOUBLE ,
    "HpHp_L1_radius" DOUBLE ,
    "HpHp_L1_covariance" DOUBLE ,
    "HpHp_L1_pcc" DOUBLE ,
    "HpHp_L0.1_weight" DOUBLE ,
    "HpHp_L0.1_mean" DOUBLE ,
    "HpHp_L0.1_std" DOUBLE ,
    "HpHp_L0.1_magnitude" DOUBLE ,
    "HpHp_L0.1_radius" DOUBLE ,
    "HpHp_L0.1_covariance" DOUBLE ,
    "HpHp_L0.1_pcc" DOUBLE ,
    "HpHp_L0.01_weight" DOUBLE ,
    "HpHp_L0.01_mean" DOUBLE ,
    "HpHp_L0.01_std" DOUBLE ,
    "HpHp_L0.01_magnitude" DOUBLE ,
    "HpHp_L0.01_radius" DOUBLE ,
    "HpHp_L0.01_covariance" DOUBLE ,
    "HpHp_L0.01_pcc" DOUBLE,
    attack_type VARCHAR,
    device_name VARCHAR,
    dt VARCHAR
)
WITH (
   format = 'PARQUET',
   external_location = 's3a://iot-time-series/processed/',
   partitioned_by = ARRAY['device_name', 'dt']
);
```

```sql
CALL hive.system.sync_partition_metadata(schema_name => 'iot_lakehouse', table_name => 'traffic', mode => 'FULL');
```

```sql
SELECT
    device_name,
    dt,
    attack_type,
    count(*) as event_count
FROM
    hive.iot_lakehouse.traffic
GROUP BY
    1, 2, 3
ORDER BY
    event_count DESC
LIMIT 10;
```

```sql
SELECT
    device_name,
    COUNT(*) as total_record_count
FROM
    hive.iot_lakehouse.traffic
GROUP BY
    device_name
ORDER BY
    total_record_count DESC;
```