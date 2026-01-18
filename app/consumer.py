import asyncio
import aio_pika


async def connect_to_rabbitmq():
    """Подключение к RabbitMQ с retry логикой"""
    connection = None
    while connection is None:
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
        except Exception:
            print("RabbitMQ не готов, жду 5 секунды...")
            await asyncio.sleep(5)
    return connection


async def main():
    connection = await connect_to_rabbitmq()

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("task_queue")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print("Received:", message.body.decode())


if __name__ == "__main__":
    asyncio.run(main())