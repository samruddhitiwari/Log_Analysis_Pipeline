-- IPs with most requests
SELECT ip, COUNT(*) AS request_count FROM log_table GROUP BY ip ORDER BY request_count DESC;

-- Peak traffic times (per minute)
SELECT DATE_TRUNC('minute', timestamp) AS minute, COUNT(*) FROM log_table GROUP BY minute ORDER BY minute;

-- Failed logins (status code 401)
SELECT ip, COUNT(*) AS failed_logins FROM log_table WHERE status_code = '401' GROUP BY ip ORDER BY failed_logins DESC;
