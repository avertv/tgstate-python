import asyncio
import os
from functools import lru_cache
from typing import BinaryIO
import telegram
from telegram import InputFile, Update
from telegram.ext import CallbackContext
from telegram.request import HTTPXRequest
from telegram.error import RetryAfter, TimedOut
from ..core.config import Settings, get_settings
from .. import database

# Для работы через стандартный Telegram Bot API скачивание ограничено 20MB, 
# поэтому устанавливаем размер чанка 19.5MB.
CHUNK_SIZE_BYTES = int(19.5 * 1024 * 1024)


class ChunkReader:
    """Ограничивает файловый хэндл одним чанком без копирования в память."""

    def __init__(self, file: BinaryIO, size: int, filename: str):
        self.file = file
        self.size = size
        self.start_pos = file.tell()
        self.remaining = size
        self.name = filename

    def reset(self):
        """Сброс позиционирования файла для повторной попытки отправки."""
        self.file.seek(self.start_pos)
        self.remaining = self.size

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        read_size = self.remaining if size < 0 else min(size, self.remaining)
        data = self.file.read(read_size)
        self.remaining -= len(data)
        return data


class TelegramService:
    """
    Сервис для взаимодействия с Telegram Bot API.
    """
    def __init__(self, settings: Settings):
        # Таймаут ожидания загрузки крупных файлов (5 минут)
        request = HTTPXRequest(
            connection_pool_size=8,
            connect_timeout=300.0,
            read_timeout=300.0,
            write_timeout=300.0,
            media_write_timeout=300.0,
        )
        self.bot = telegram.Bot(token=settings.BOT_TOKEN, request=request)
        self.channel_name = settings.CHANNEL_NAME

    async def _upload_as_chunks(self, file_path: str, original_filename: str) -> str | None:
        """
        Разбивает крупный файл на части и связывает их цепью ответов (reply).
        """
        chunk_file_ids = []
        first_message_id = None
        total_size = os.path.getsize(file_path)

        try:
            with open(file_path, 'rb') as file:
                chunk_number = 1
                remaining = total_size
                while remaining:
                    chunk_size = min(CHUNK_SIZE_BYTES, remaining)
                    chunk_name = f"{original_filename}.part{chunk_number}"
                    print(f"Загрузка части: {chunk_name}")
                    
                    # Интервал безопасности перед отправкой очередного чанка
                    await asyncio.sleep(2.0)

                    chunk_reader = ChunkReader(file, chunk_size, chunk_name)
                    success_upload = False
                    retries = 0

                    # Цикл обработки временных ошибок таймаута и лимитов
                    while not success_upload and retries < 5:
                        # Всегда сбрасываем указатель чтения перед попыткой
                        chunk_reader.reset()
                        document = InputFile(
                            chunk_reader,
                            filename=chunk_name,
                            read_file_handle=False,
                        )

                        try:
                            message = await self.bot.send_document(
                                chat_id=self.channel_name,
                                document=document,
                                filename=chunk_name,
                                reply_to_message_id=first_message_id,
                            )
                            success_upload = True
                        except RetryAfter as e:
                            wait_time = int(e.retry_after) + 2
                            print(f"Превышен лимит Telegram (Flood control). Ожидание {wait_time} сек...")
                            await asyncio.sleep(wait_time)
                            retries += 1
                        except TimedOut:
                            print("Таймаут соединения Telegram API. Повтор через 5 секунд...")
                            await asyncio.sleep(5)
                            retries += 1
                        except Exception as e:
                            print(f"Ошибка при попытке отправки чанка: {e}")
                            await asyncio.sleep(3)
                            retries += 1

                    if not success_upload:
                        print(f"Не удалось загрузить часть {chunk_name} после нескольких попыток.")
                        return None

                    if not first_message_id:
                        first_message_id = message.message_id

                    chunk_file_ids.append(f"{message.message_id}:{message.document.file_id}")
                    remaining -= chunk_size
                    chunk_number += 1
        except IOError as e:
            print(f"Ошибка ввода-вывода при чтении файла: {e}")
            return None
        except Exception as e:
            print(f"Ошибка отправки части файла: {e}")
            return None

        manifest_content = f"tgstate-blob\n{original_filename}\n" + "\n".join(chunk_file_ids)
        manifest_name = f"{original_filename}.manifest"

        print("Все чанки загружены. Отправка манифест-файла...")
        try:
            await asyncio.sleep(1.5)
            message = await self.bot.send_document(
                chat_id=self.channel_name,
                document=manifest_content.encode('utf-8'),
                filename=manifest_name,
                reply_to_message_id=first_message_id
            )
            if message.document:
                print("Манифест-файл успешно загружен.")
                composite_id = f"{message.message_id}:{message.document.file_id}"
                await asyncio.to_thread(
                    database.add_file_metadata,
                    filename=original_filename,
                    file_id=composite_id,
                    filesize=total_size,
                )
                return composite_id
        except Exception as e:
            print(f"Ошибка загрузки манифеста: {e}")

        return None

    async def upload_file(self, file_path: str, file_name: str) -> str | None:
        if not self.channel_name:
            print("Ошибка: переменная CHANNEL_NAME не задана в окружении.")
            return None

        try:
            file_size = os.path.getsize(file_path)
        except OSError as e:
            print(f"Не удалось определить размер файла: {e}")
            return None

        if file_size >= CHUNK_SIZE_BYTES:
            print(f"Размер файла ({file_size / 1024 / 1024:.2f} MB) превышает или равен {CHUNK_SIZE_BYTES / 1024 / 1024:.2f}MB. Запуск чанковой загрузки...")
            return await self._upload_as_chunks(file_path, file_name)

        print(f"Размер файла ({file_size / 1024 / 1024:.2f} MB) меньше {CHUNK_SIZE_BYTES / 1024 / 1024:.2f}MB. Запуск прямой загрузки...")
        try:
            with open(file_path, 'rb') as document_file:
                message = await self.bot.send_document(
                    chat_id=self.channel_name,
                    document=document_file,
                    filename=file_name
                )
            if message.document:
                composite_id = f"{message.message_id}:{message.document.file_id}"
                await asyncio.to_thread(
                    database.add_file_metadata,
                    filename=file_name,
                    file_id=composite_id,
                    filesize=file_size,
                )
                return composite_id
        except Exception as e:
            print(f"Ошибка загрузки файла в Telegram: {e}")

        return None

    async def get_download_url(self, file_id: str) -> str | None:
        try:
            file = await self.bot.get_file(file_id)
            return file.file_path
        except Exception as e:
            print(f"Ошибка получения ссылки скачивания Telegram: {e}")
            return None

    async def delete_message(self, message_id: int) -> tuple[bool, str]:
        try:
            await self.bot.delete_message(
                chat_id=self.channel_name,
                message_id=message_id
            )
            return (True, "deleted")
        except telegram.error.BadRequest as e:
            if "not found" in str(e).lower():
                print(f"Сообщение {message_id} не найдено, считается удаленным.")
                return (True, "not_found")
            else:
                print(f"Ошибка удаления сообщения {message_id} (BadRequest): {e}")
                return (False, "error")
        except Exception as e:
            print(f"Ошибка при удалении сообщения {message_id}: {e}")
            return (False, "error")

    async def delete_file_with_chunks(self, file_id: str) -> dict:
        results = {
            "status": "pending",
            "main_file_id": file_id,
            "deleted_chunks": [],
            "failed_chunks": [],
            "main_message_deleted": False,
            "is_manifest": False,
            "reason": ""
        }

        try:
            main_message_id_str, main_actual_file_id = file_id.split(':', 1)
            main_message_id = int(main_message_id_str)
        except (ValueError, IndexError):
            results["status"] = "error"
            results["reason"] = "Неверный формат составного file_id."
            return results

        download_url = await self.get_download_url(main_actual_file_id)
        if not download_url:
            print(f"Предупреждение: Не удалось получить URL для файла {main_actual_file_id}.")
            results["reason"] = f"Не удалось получить ссылку скачивания для {main_actual_file_id}."
        else:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.get(download_url)
                    if response.status_code == 200 and response.content.startswith(b'tgstate-blob\n'):
                        results["is_manifest"] = True
                        print(f"Файл {file_id} является манифестом. Удаление частей...")

                        manifest_content = response.content.decode('utf-8')
                        lines = manifest_content.strip().split('\n')
                        chunk_composite_ids = lines[2:]

                        for chunk_id in chunk_composite_ids:
                            try:
                                chunk_message_id_str, _ = chunk_id.split(':', 1)
                                chunk_message_id = int(chunk_message_id_str)
                                success, _ = await self.delete_message(chunk_message_id)
                                if success:
                                    results["deleted_chunks"].append(chunk_id)
                                else:
                                    results["failed_chunks"].append(chunk_id)
                            except Exception as e:
                                print(f"Ошибка удаления части {chunk_id}: {e}")
                                results["failed_chunks"].append(chunk_id)
            except Exception as e:
                error_message = f"Ошибка разбора манифеста {file_id}: {e}"
                print(error_message)
                results["reason"] += " " + error_message

        main_message_deleted, delete_reason = await self.delete_message(main_message_id)
        results["main_message_deleted"] = main_message_deleted

        if main_message_deleted:
            if delete_reason == "deleted":
                print(f"Главное сообщение {main_message_id} успешно удалено.")
            elif delete_reason == "not_found":
                print(f"Главное сообщение {main_message_id} не найдено, операция успешна.")
        else:
            print(f"Не удалось удалить главное сообщение {main_message_id}.")

        if results["main_message_deleted"] and (not results["is_manifest"] or not results["failed_chunks"]):
            results["status"] = "success"
        else:
            results["status"] = "partial_failure"
            if not results["main_message_deleted"]:
                results["reason"] += " Не удалось удалить главное сообщение."
            if results["failed_chunks"]:
                results["reason"] += f" Не удалось удалить {len(results['failed_chunks'])} чанков."

        return results

    async def list_files_in_channel(self) -> list[dict]:
        files = []
        last_message_id = None
        MAX_ITERATIONS = 100

        print("Получение истории сообщений канала...")

        for i in range(MAX_ITERATIONS):
            try:
                messages = await self.bot.get_chat_history(
                    chat_id=self.channel_name,
                    limit=100,
                    offset_id=last_message_id if last_message_id else 0
                )
            except Exception as e:
                print(f"Ошибка получения истории чата: {e}")
                break

            if not messages:
                break

            for message in messages:
                if message.document:
                    doc = message.document
                    if doc.file_size < CHUNK_SIZE_BYTES and not doc.file_name.endswith('.manifest'):
                        files.append({
                            "name": doc.file_name,
                            "file_id": doc.file_id,
                            "size": doc.file_size
                        })
                    elif doc.file_name.endswith('.manifest'):
                        manifest_url = await self.get_download_url(doc.file_id)
                        if not manifest_url: continue

                        import httpx
                        async with httpx.AsyncClient() as client:
                            try:
                                resp = await client.get(manifest_url)
                                if resp.status_code == 200 and resp.content.startswith(b'tgstate-blob\n'):
                                    lines = resp.content.decode('utf-8').strip().split('\n')
                                    original_filename = lines[1]
                                    files.append({
                                        "name": original_filename,
                                        "file_id": doc.file_id,
                                        "size": None
                                    })
                            except httpx.RequestError:
                                continue

            last_message_id = messages[-1].message_id

        print(f"Получено файлов: {len(files)}.")
        return files

@lru_cache()
def get_telegram_service() -> TelegramService:
    return TelegramService(settings=get_settings())