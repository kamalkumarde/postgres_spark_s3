cd /Users/kamalkumar/projects/pipeline
docker-compose -f docker-compose.yml down
docker rmi pipeline-airflow-webserver pipeline-airflow-scheduler
docker builder prune -f
docker ps -a
docker images
cd /Users/kamalkumar/projects/pipeline
docker-compose -f docker-compose.yml build --no-cache
docker-compose -f docker-compose.yml up -d

