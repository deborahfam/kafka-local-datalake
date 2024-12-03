load containers:

docker load -i zookeeper_image.tar
docker load -i kafka_image.tar

after having the container, execute

```
docker compose up
python producer.py
python consumer.py
```