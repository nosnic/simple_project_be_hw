import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy.exc import SQLAlchemyError
from aio_pika.exceptions import AMQPConnectionError

from app.main import app, startup, init_db, send_to_rabbitmq
from app.models import Item
from app.schemas import ItemCreate, ItemRead
from app.db import engine, Base, get_db


# Фикстуры
@pytest.fixture
def mock_db_session():
    """Мок асинхронной сессии БД"""
    session = AsyncMock(spec=AsyncSession)
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_item():
    """Мок объекта Item"""
    item = MagicMock(spec=Item)
    item.id = 1
    item.name = "Test Item"
    return item


# Тесты для эндпоинтов API
class TestFastAPIEndpoints:
    """Тесты для FastAPI эндпоинтов"""

    @pytest.mark.asyncio
    async def test_create_item_success(self, mock_db_session):
        """Тест успешного создания элемента"""
        test_item_data = {"name": "Test Item"}

        mock_new_item = MagicMock()
        mock_new_item.id = 1
        mock_new_item.name = "Test Item"

        with patch('app.main.Item', return_value=mock_new_item):
            with patch('app.main.send_to_rabbitmq', new_callable=AsyncMock) as mock_send:
                client = TestClient(app)
                app.dependency_overrides[get_db] = lambda: mock_db_session

                try:
                    response = client.post("/items/", json=test_item_data)

                    assert response.status_code == 200
                    response_data = response.json()
                    assert response_data["name"] == test_item_data["name"]

                    mock_db_session.add.assert_called_once_with(mock_new_item)
                    mock_db_session.commit.assert_called_once()
                    mock_db_session.refresh.assert_called_once_with(mock_new_item)

                    mock_send.assert_called_once_with("Item created: 1")

                finally:
                    app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_create_item_db_error(self, mock_db_session):
        """Тест создания элемента с ошибкой БД"""
        test_item_data = {"name": "Test Item"}

        mock_new_item = MagicMock()
        mock_new_item.id = 1
        mock_new_item.name = "Test Item"

        failing_session = AsyncMock(spec=AsyncSession)
        failing_session.add = MagicMock()
        failing_session.commit = AsyncMock(side_effect=SQLAlchemyError("DB Error"))
        failing_session.refresh = AsyncMock()
        failing_session.rollback = AsyncMock()

        with patch('app.main.Item', return_value=mock_new_item):
            client = TestClient(app)
            app.dependency_overrides[get_db] = lambda: failing_session

            try:
                with pytest.raises(SQLAlchemyError, match="DB Error"):
                    client.post("/items/", json=test_item_data)

                failing_session.commit.assert_called_once()

            finally:
                app.dependency_overrides.clear()

    def test_prometheus_metrics_endpoint(self):
        """Тест эндпоинта метрик Prometheus"""
        client = TestClient(app)
        response = client.get("/metrics")

        assert response.status_code == 200
        assert response.text is not None


# Тесты для функций инициализации
class TestInitFunctions:
    """Тесты для функций инициализации"""

    @pytest.mark.asyncio
    async def test_init_db_success(self):
        """Тест успешной инициализации БД"""
        mock_conn = AsyncMock()
        mock_conn.run_sync = AsyncMock()

        with patch('app.main.engine') as mock_engine:
            mock_begin = AsyncMock()
            mock_engine.begin.return_value = mock_begin
            mock_begin.__aenter__.return_value = mock_conn
            mock_begin.__aexit__ = AsyncMock()

            await init_db()

            mock_conn.run_sync.assert_called_once_with(Base.metadata.create_all)

    @pytest.mark.asyncio
    async def test_init_db_with_retries(self):
        """Тест инициализации БД с повторными попытками"""
        with patch('app.main.engine') as mock_engine:
            with patch('app.main.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                call_count = [0]

                class MockBegin:
                    """Мок для контекстного менеджера engine.begin()"""

                    async def __aenter__(self):
                        call_count[0] += 1
                        mock_conn = AsyncMock()

                        # Первые две попытки выбрасывают исключение
                        if call_count[0] <= 2:
                            # Исключение должно быть выброшено при run_sync
                            async def failing_run_sync(func):
                                raise Exception("DB not ready")

                            mock_conn.run_sync = failing_run_sync
                        else:
                            # Третья попытка успешна
                            mock_conn.run_sync = AsyncMock()

                        return mock_conn

                    async def __aexit__(self, exc_type, exc_val, exc_tb):
                        # Не подавляем исключение, пусть оно пробрасывается
                        return False

                # Каждый вызов begin() возвращает новый экземпляр MockBegin
                mock_engine.begin.side_effect = lambda: MockBegin()

                await init_db()

                assert mock_engine.begin.call_count == 3
                assert mock_sleep.call_count == 2
                mock_sleep.assert_has_calls([call(2), call(2)])

    @pytest.mark.asyncio
    async def test_startup_calls_init_db(self):
        """Тест что startup вызывает init_db"""
        with patch('app.main.init_db', new_callable=AsyncMock) as mock_init:
            await startup()
            mock_init.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_to_rabbitmq_success(self):
        """Тест успешной отправки в RabbitMQ"""
        with patch('aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect:
            mock_connection = AsyncMock()
            mock_channel = AsyncMock()
            mock_exchange = AsyncMock()

            mock_connect.return_value = mock_connection
            mock_connection.__aenter__.return_value = mock_connection
            mock_connection.__aexit__ = AsyncMock()
            mock_connection.channel.return_value = mock_channel
            mock_channel.default_exchange = mock_exchange
            mock_exchange.publish = AsyncMock()

            await send_to_rabbitmq("Test message")

            mock_connect.assert_called_once_with("amqp://guest:guest@rabbitmq/")
            mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_to_rabbitmq_with_retries(self):
        """Тест отправки в RabbitMQ с повторными попытками"""
        with patch('aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect:
            with patch('app.main.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                mock_connection = AsyncMock()
                mock_channel = AsyncMock()
                mock_exchange = AsyncMock()

                mock_connect.side_effect = [
                    Exception("Connection failed"),
                    Exception("Connection failed"),
                    mock_connection
                ]

                mock_connection.__aenter__.return_value = mock_connection
                mock_connection.__aexit__ = AsyncMock()
                mock_connection.channel.return_value = mock_channel
                mock_channel.default_exchange = mock_exchange
                mock_exchange.publish = AsyncMock()

                await send_to_rabbitmq("Test message")

                assert mock_connect.call_count == 3
                assert mock_sleep.call_count == 2
                mock_sleep.assert_has_calls([call(2), call(2)])


# Тесты для моделей и схем
class TestModelsAndSchemas:
    """Тесты для моделей и Pydantic схем"""

    def test_item_model(self):
        """Тест модели Item"""
        item = Item(name="Test Item")

        assert item.name == "Test Item"
        assert hasattr(item, 'id')
        assert item.__tablename__ == 'items'

    def test_item_create_schema(self):
        """Тест схемы ItemCreate"""
        item_data = {"name": "Test Item"}
        schema = ItemCreate(**item_data)

        assert schema.name == "Test Item"

    def test_item_read_schema(self):
        """Тест схемы ItemRead"""
        item_data = {"id": 1, "name": "Test Item"}
        schema = ItemRead(**item_data)

        assert schema.id == 1
        assert schema.name == "Test Item"


# Тесты для базы данных
class TestDatabase:
    """Тесты для модуля базы данных"""

    @pytest.mark.asyncio
    async def test_get_db(self):
        """Тест получения сессии БД"""
        with patch('app.db.AsyncSessionLocal') as mock_session_local:
            mock_session = AsyncMock(spec=AsyncSession)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock()

            mock_session_local.return_value = mock_session

            db_gen = get_db()

            async for session in db_gen:
                assert session == mock_session
                break

            mock_session.__aexit__.assert_not_called()

    def test_engine_creation(self):
        """Тест создания движка БД"""
        assert engine is not None
        assert isinstance(engine, AsyncEngine)


# Тесты для consumer
class TestRabbitMQConsumer:
    """Тесты для RabbitMQ потребителя"""

    @pytest.mark.asyncio
    async def test_connect_to_rabbitmq_success(self):
        """Тест успешного подключения к RabbitMQ"""
        with patch('app.consumer.aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect:
            mock_connection = AsyncMock()
            mock_connect.return_value = mock_connection

            from app.consumer import connect_to_rabbitmq

            result = await connect_to_rabbitmq()

            assert result == mock_connection
            mock_connect.assert_called_once_with("amqp://guest:guest@rabbitmq/")

    @pytest.mark.asyncio
    async def test_connect_to_rabbitmq_with_retries(self):
        """Тест подключения к RabbitMQ с повторными попытками"""
        with patch('app.consumer.aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect:
            with patch('app.consumer.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                mock_connection = AsyncMock()

                mock_connect.side_effect = [
                    Exception("Connection failed"),
                    Exception("Connection failed"),
                    mock_connection
                ]

                from app.consumer import connect_to_rabbitmq

                result = await connect_to_rabbitmq()

                assert result == mock_connection
                assert mock_connect.call_count == 3
                assert mock_sleep.call_count == 2
                mock_sleep.assert_has_calls([call(5), call(5)])

    @pytest.mark.asyncio
    async def test_consumer_main_success(self):
        """Тест успешного запуска потребителя"""
        with patch('app.consumer.connect_to_rabbitmq', new_callable=AsyncMock) as mock_connect_func:
            mock_connection = AsyncMock()
            mock_channel = AsyncMock()
            mock_queue = AsyncMock()

            mock_connect_func.return_value = mock_connection
            mock_connection.__aenter__.return_value = mock_connection
            mock_connection.__aexit__ = AsyncMock()
            mock_connection.channel.return_value = mock_channel
            mock_channel.declare_queue.return_value = mock_queue

            mock_queue_iterator = AsyncMock()
            mock_queue.iterator.return_value = mock_queue_iterator
            mock_queue_iterator.__aenter__.return_value = AsyncMock()
            mock_queue_iterator.__aexit__ = AsyncMock()

            from app.consumer import main

            task = asyncio.create_task(main())
            await asyncio.sleep(0.01)
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

            mock_connect_func.assert_called_once()
            mock_channel.declare_queue.assert_called_once_with("task_queue")


# Простые тесты для проверки основного функционала
def test_basic_item_creation():
    """Простой тест создания элемента через схему"""
    item_data = ItemCreate(name="Test Item")
    assert item_data.name == "Test Item"

    item_response = ItemRead(id=1, name="Test Item")
    assert item_response.id == 1
    assert item_response.name == "Test Item"


def test_item_model_creation():
    """Простой тест создания модели"""
    item = Item(name="Test Item")
    assert item.name == "Test Item"


# Тест для проверки корректности импортов
def test_imports():
    """Тест корректности импортов"""
    from app.main import app, startup, init_db, send_to_rabbitmq
    from app.models import Item
    from app.schemas import ItemCreate, ItemRead
    from app.db import engine, Base, get_db
    from app.consumer import main as consumer_main, connect_to_rabbitmq

    assert app is not None
    assert Item is not None
    assert ItemCreate is not None
    assert ItemRead is not None
    assert engine is not None
    assert Base is not None
    assert get_db is not None
    assert init_db is not None
    assert send_to_rabbitmq is not None
    assert connect_to_rabbitmq is not None


if __name__ == "__main__":
    pytest.main(["-v", "--cov=app", "--cov-report=term-missing", "--cov-fail-under=90"])