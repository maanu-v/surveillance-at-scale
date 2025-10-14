uv run src/kafka/producer/hdfs_producer.py --max-frames 50 

uv run src/kafka/producer/hdfs_producer.py --camera 1 --max-frames 10
uv run src/kafka/producer/hdfs_producer.py --camera 2 --max-frames 10
# ... for cameras 3-7


uv run src/kafka/consumer/consumer.py --camera 1 --max-messages 5