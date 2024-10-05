echo "Clearing data"
rm -rf ../postgresql-rp/data/*
rm -rf ../postgresql-rp/data-slave/*
docker-compose down

docker-compose up -d  postgres_master

echo "Starting postgres_master node..."
sleep 60  # Waits for master note start complete

echo "Prepare replica config..."
docker exec -it postgres_master sh /etc/postgresql/init-script/init.sh
echo "Restart master node"
docker-compose restart postgres_master
sleep 30

echo "Starting slave node..."
docker-compose up -d  postgres_slave
sleep 30  # Waits for note start complete

echo "Done"
