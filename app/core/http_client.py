import httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .. import database
from ..bot_handler import create_bot_app
from ..services.telegram_sync_service import get_telegram_sync_service

# 这个变量将持有全局共享的客户端实例。
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理器。
    启动时初始化数据库、HTTP 客户端、Bot 与删除同步服务；
    关闭时按相反顺序释放资源。
    """
    print("🚀 Запуск приложения...")

    database.init_db()
    print("✔️ База данных инициализирована.")

    global http_client
    limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
    http_client = httpx.AsyncClient(timeout=300.0, limits=limits)
    print("✔️ Общий HTTP-клиент создан.")

    bot_app = None
    bot_initialized = False
    try:
        bot_app = create_bot_app()
        app.state.bot_app = bot_app
        await bot_app.initialize()
        bot_initialized = True
        await bot_app.start()
        await bot_app.updater.start_polling(drop_pending_updates=True)
        print("✔️ Бот запущен в фоновом режиме.")
    except Exception as exc:
        print(f"❌ 启动机器人失败，Web 服务将继续运行: {exc}")
        app.state.bot_app = None
        if bot_app and bot_initialized:
            try:
                await bot_app.shutdown()
            except Exception as shutdown_exc:
                print(f"❌ 清理机器人资源失败: {shutdown_exc}")

    telegram_sync_service = get_telegram_sync_service()
    app.state.telegram_sync_service = telegram_sync_service
    try:
        await telegram_sync_service.start()
    except Exception as exc:
        print(f"❌ Ошибка запуска службы синхронизации удалений Telegram: {exc}")

    yield

    print("🔌 Завершение работы приложения...")

    if getattr(app.state, "telegram_sync_service", None):
        await app.state.telegram_sync_service.stop()
        print("✔️ Служба синхронизации удалений Telegram остановлена.")

    if http_client:
        await http_client.aclose()
        print("✔️ Общий HTTP-клиент закрыт.")

    if hasattr(app.state, "bot_app") and app.state.bot_app:
        print("Остановка бота...")
        await app.state.bot_app.updater.stop()
        await app.state.bot_app.stop()
        await app.state.bot_app.shutdown()
        print("✔️ Бот остановлен.")


def get_http_client() -> httpx.AsyncClient:
    """提供共享的 `httpx.AsyncClient` 实例。"""
    if http_client is None:
        raise RuntimeError("HTTP client is not initialized. Is the app lifespan configured correctly?")
    return http_client
