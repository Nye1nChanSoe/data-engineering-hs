import asyncio
from aiokafka import AIOKafkaConsumer


KAFKA_TOPIC = "hsutest"


async def consume():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="my-async-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    # Start the consumer
    await consumer.start()
    try:
        print("Listening for messages...")
        async for msg in consumer:
            print(
                f"Topic: {msg.topic}, Partition: {msg.partition}, Offset: {msg.offset}"
            )
            print(
                f"Key: {msg.key}, Value: {msg.value.decode('utf-8') if msg.value else None}"
            )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
