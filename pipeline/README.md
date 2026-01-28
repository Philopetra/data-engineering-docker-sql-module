Change your directory to the pipeline directory using 

cd pipeline/


From your terminal run the following commands in the following order

docker-compose up # This fires up the docker-compose.yaml file

docker build -t taxi_ingest:v001 . # This builds an image based on the entry script on the docker file.


Then you ingest your data from the url into a pgDatabase using these commands. 

# These command are solely based on conditions in the ingest_data.py file.

For Yellow Taxi (CSV)

docker run -it --rm \
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --service=yellow \
    --year=2021 \
    --month=3 \
    --target-table=yellow_taxi_trips_2021_03 \
    --chunksize=100000

For Green Taxi (Parquet)

docker run -it --rm \
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --service=green \
    --year=2025 \
    --month=11 \
    --target-table=green_taxi_trips_2025_11

For Taxi Zones (Lookup CSV)

docker run -it --rm \
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --service=zones \
    --target-table=taxi_zone_lookup


Question 1. What's the version of pip in the python:3.13 image? (1 point)

Bash Solution command 
pip -V


Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?

SQL COMMAND

SELECT COUNT(*)
FROM green_taxi_trips_2025_11
WHERE trip_distance <= 1.0;


Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles.

SELECT 
    CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
    MAX(trip_distance) AS longest_distance
FROM green_taxi_trips_2025_11
WHERE trip_distance < 100.0
GROUP BY pickup_day
ORDER BY longest_distance DESC
LIMIT 1;


Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?


SELECT 
    tz."Zone",
    SUM(gt.total_amount) AS total_revenue
FROM 
    green_taxi_trips_2025_11 gt
JOIN 
    taxi_zone_lookup tz ON gt."PULocationID" = tz."LocationID"
WHERE 
    CAST(gt.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 
    tz."Zone"
ORDER BY 
    total_revenue DESC
LIMIT 1;


Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

SELECT 
    tz_do."Zone" AS dropoff_zone,
    MAX(gt.tip_amount) AS largest_tip
FROM 
    green_taxi_trips_2025_11 gt
JOIN 
    taxi_zone_lookup tz_pu ON gt."PULocationID" = tz_pu."LocationID"
JOIN 
    taxi_zone_lookup tz_do ON gt."DOLocationID" = tz_do."LocationID"
WHERE 
    tz_pu."Zone" = 'East Harlem North'
GROUP BY 
    dropoff_zone
ORDER BY 
    largest_tip DESC
LIMIT 1;