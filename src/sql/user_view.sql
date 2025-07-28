CREATE VIEW cs_telemtry_view AS(
     SELECT 
          event_id, event_year, event_code, event_num, session_id, ts, rpm, speed, gear, throttle, breaks, drs
     FROM fact_telemetry
     WHERE drivernumber = 55
);

CREATE VIEW aa_telemtry_view AS(
     SELECT 
          event_id, event_year, event_code, event_num, session_id, ts, rpm, speed, gear, throttle, breaks, drs
     FROM fact_telemetry
     WHERE drivernumber = 23
);