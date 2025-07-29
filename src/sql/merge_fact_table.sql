MERGE INTO "{catalog}"."{database}"."{dst_table}" target
USING (
    SELECT 
        LOWER(TO_HEX(MD5(CAST(concat(event_id,session_id,cast(drivernumber as varchar), cast(timeutc as varchar)) AS VARBINARY)))) as pk_id,
        cast(event_id as varchar) event_id,
        cast(event_year as varchar) event_year,
        cast(event_code as varchar) event_code,
        cast(event_num as varchar) event_num,
        cast(session_id as varchar) session_id,
        cast(timeutc as timestamp) ts,
        cast(drivernumber as int) drivernumber,
        cast(rpm as int) rpm,
        cast(speed as int) speed,
        cast(gear as int) gear,
        cast(throttle as int) throttle,
        cast(brake as boolean) as breaks,
        cast(drs as int) as drs
    FROM "{catalog}"."{database}"."{src_table}"
) src
ON (target.pk_id = src.pk_id)
WHEN MATCHED THEN
    UPDATE SET
        rpm = src.rpm,
        speed = src.speed,
        gear = src.gear,
        throttle = src.throttle,
        breaks = src.breaks,
        drs = src.drs
WHEN NOT MATCHED THEN INSERT (
    pk_id, event_id, event_year, event_code, event_num, session_id, ts,
    drivernumber, rpm, speed, gear, throttle, breaks, drs
)
VALUES (
    src.pk_id, src.event_id, src.event_year, src.event_code, src.event_num,
    src.session_id, src.ts, src.drivernumber, src.rpm, src.speed,
    src.gear, src.throttle, src.breaks, src.drs
);
