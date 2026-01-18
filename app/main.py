# app/main.py
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from prometheus_fastapi_instrumentator import Instrumentator
import asyncio
import aio_pika

from .db import get_db, engine, Base
from .models import Item
from .schemas import ItemCreate, ItemRead

app = FastAPI()

# Метрики Prometheus
Instrumentator().instrument(app).expose(app)


async def init_db():
    """Инициализация БД с retry логикой"""
    while True:
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            break
        except Exception:
            print("PostgreSQL не готов, жду 2 секунды...")
            await asyncio.sleep(2)


@app.on_event("startup")
async def startup():
    await init_db()


async def send_to_rabbitmq(message: str):
    """Отправка сообщения в RabbitMQ с retry логикой"""
    connection = None
    while connection is None:
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
        except Exception:
            print("RabbitMQ не готов, жду 2 секунды...")
            await asyncio.sleep(2)

    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key="task_queue"
        )


@app.post("/items/", response_model=ItemRead)
async def create_item(item: ItemCreate, db: AsyncSession = Depends(get_db)):
    new_item = Item(name=item.name)
    db.add(new_item)
    await db.commit()
    await db.refresh(new_item)

    await send_to_rabbitmq(f"Item created: {new_item.id}")

    return new_item
