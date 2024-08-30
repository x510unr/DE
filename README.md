Taking parquet files as input and uploading the data in postgres and dockersing the whole process.


1. Running the postgres and pgcli -  docker-compose up -d 
2. Build image using docker file -  docker build -t ingest:v0001 .
3. Spinning the container from the image
docker run -it -v /home/gray/practice/docker_p/logs:/home/pipe/logs \
 	ingest:v0001 --url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet \
	--db ny_taxi \
	--table yellow_taxi_trips
