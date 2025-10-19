-- I want to know what the tip percentage for the various boroughs is. There 

SELECT 
	tz."Borough", 
    ROUND(CAST(SUM(ytt."tip_amount") / SUM("fare_amount") as numeric), 4) as prop_tipped_amount, 
    count(*) as nrows
FROM 
	"DE25".yellow_taxi_trips ytt
LEFT JOIN 
	"DE25".taxi_zones tz
		ON ytt."PULocationID" = tz."LocationID"
GROUP BY tz."Borough"
ORDER BY 2 DESC;


-- another interesting thing could be what percent of trips have tips

WITH JOINED_DATA AS (
    SELECT 
        ytt.*,
        CASE WHEN ytt."tip_amount" > 0 THEN 1 ELSE 0 END AS flag_tip,
        tz."Borough", 
        tz."Zone", 
        tz."service_zone"
    FROM 
        "DE25".yellow_taxi_trips ytt
    LEFT JOIN 
        "DE25".taxi_zones tz
            ON ytt."PULocationID" = tz."LocationID"
)

SELECT 
	"Borough" as pickup_borough, 
    ROUND(AVG("flag_tip")::numeric, 4) as prop_trips_w_tips, 
    count(*) as nrows
FROM 
	JOINED_DATA
GROUP BY "Borough"
ORDER BY 2 DESC;



-- should probably get a sql formatter for postgres
-- postgres definitely has differences to sqlite / BQ / Databricks sql. its a bit weird tbh