import asyncio
import hmac
import mimetypes
import os
import tempfile
from typing import Any, List, Optional
from urllib.parse import quote

import httpx
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from .. import database
from ..core.config import Settings, get_active_password, get_settings
from ..core.http_client import get_http_client
from ..events import (
    publish_file_update,
    subscribe_file_updates,
    unsubscribe_file_updates,
)
from ..services.telegram_service import TelegramService, get_telegram_service
from ..utils.file_paths import build_file_path, extract_file_id_from_value

router = APIRouter()


class PasswordRequest(BaseModel):
    password: str
    current_password: Optional[str] = None


class BatchDeleteRequest(BaseModel):
    file_ids: List[str]


def _credentials_match(expected: str | None, submitted: str | None) -> bool:
    return bool(expected and submitted and hmac.compare_digest(expected, submitted))


def _ensure_request_authorized(
    request: Request,
    settings: Settings,
    submitted_key: str | None = None,
    submitted_password: str | None = None,
) -> None:
    """Единая логика авторизации для веб-интерфейса и API."""
    picgo_api_key = settings.PICGO_API_KEY
    active_password = get_active_password()
    session_password = request.cookies.get("password")

    if not active_password and not picgo_api_key:
        return

    if _credentials_match(active_password, session_password) or _credentials_match(
        active_password,
        submitted_password,
    ):
        return

    if _credentials_match(picgo_api_key, submitted_key):
        return

    error_detail = "Недействительный ключ API" if picgo_api_key else "Требуется авторизация"
    raise HTTPException(status_code=401, detail=error_detail)


def _serialize_file(file_info: dict[str, Any], settings: Settings) -> dict[str, Any]:
    path = build_file_path(file_info["file_id"], file_info["filename"], settings.FILE_ROUTE)
    url = f"{settings.BASE_URL.strip('/')}{path}"
    return {
        "filename": file_info["filename"],
        "file_id": file_info["file_id"],
        "filesize": file_info["filesize"],
        "upload_date": file_info["upload_date"],
        "path": path,
        "url": url,
    }


def _extract_delete_targets(payload: Any, settings: Settings) -> list[str]:
    """Извлекает file_id из различных форматов запросов PicList."""
    collected: list[str] = []

    def visit(value: Any) -> None:
        if value is None:
            return

        if isinstance(value, str):
            file_id = extract_file_id_from_value(value, settings.FILE_ROUTE)
            if file_id:
                collected.append(file_id)
            return

        if isinstance(value, list):
            for item in value:
                visit(item)
            return

        if isinstance(value, dict):
            for key in ("file_id", "fileId", "url", "imgUrl", "path", "src"):
                file_id = extract_file_id_from_value(value.get(key), settings.FILE_ROUTE)
                if file_id:
                    collected.append(file_id)

            for key in ("fullResult", "delete", "list", "items", "data"):
                if key in value:
                    visit(value[key])

    visit(payload)

    deduplicated: list[str] = []
    seen: set[str] = set()
    for file_id in collected:
        if file_id not in seen:
            deduplicated.append(file_id)
            seen.add(file_id)
    return deduplicated


async def _delete_file_and_sync(
    file_id: str,
    telegram_service: TelegramService,
) -> dict[str, Any]:
    """Удаляет главное сообщение в Telegram, очищает БД и рассылает событие удаления."""
    delete_result = await telegram_service.delete_file_with_chunks(file_id)
    delete_result["file_id"] = file_id
    error_text = " ".join(
        str(value)
        for value in (delete_result.get("reason"), delete_result.get("error"))
        if value
    ).lower()
    is_not_found_error = "not found" in error_text
    main_message_deleted = bool(delete_result.get("main_message_deleted"))

    if main_message_deleted or is_not_found_error:
        was_deleted_from_db = await asyncio.to_thread(database.delete_file_metadata, file_id)
        if is_not_found_error:
            delete_result["db_status"] = "deleted_after_not_found"
            delete_result["status"] = "success"
        elif was_deleted_from_db:
            delete_result["db_status"] = "deleted"
        else:
            delete_result["db_status"] = "not_found_in_db"

        await publish_file_update(
            {
                "action": "delete",
                "file_id": file_id,
            }
        )

        message = f"Файл {file_id} успешно удалён."
        if delete_result.get("status") == "partial_failure":
            message = (
                f"Файл {file_id} удалён из списка веб-интерфейса, "
                f"но {len(delete_result.get('failed_chunks', []))} чанков не удалось удалить."
            )
        elif is_not_found_error:
            message = f"Файл {file_id} не найден в Telegram, синхронизация удаления завершена."

        return {
            "status": "ok",
            "file_id": file_id,
            "message": message,
            "details": delete_result,
        }

    if delete_result.get("status") == "partial_failure":
        raise HTTPException(
            status_code=500,
            detail={
                "file_id": file_id,
                "message": f"Ошибка частичного удаления файла {file_id}.",
                "details": delete_result,
            },
        )

    raise HTTPException(
        status_code=400,
        detail={
            "file_id": file_id,
            "message": f"Ошибка при удалении файла {file_id}.",
            "details": delete_result,
        },
    )


@router.post("/api/upload")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    key: Optional[str] = Form(None),
    settings: Settings = Depends(get_settings),
    telegram_service: TelegramService = Depends(get_telegram_service),
    x_api_key: Optional[str] = Header(None),
):
    """Обрабатывает запросы на загрузку файлов из веб-интерфейса и PicList."""
    submitted_key = x_api_key or key
    _ensure_request_authorized(request, settings, submitted_key)

    temp_file_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            while chunk := await file.read(1024 * 1024):
                temp_file.write(chunk)

        upload_filename = file.filename or "upload"
        file_id = await telegram_service.upload_file(temp_file_path, upload_filename)
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

    if not file_id:
        raise HTTPException(status_code=500, detail="Ошибка загрузки файла.")

    file_info = await asyncio.to_thread(database.get_file_info, file_id)
    if file_info:
        serialized_file = _serialize_file(file_info, settings)
        await publish_file_update({
            "action": "add",
            **serialized_file,
        })
    else:
        serialized_file = {
            "path": build_file_path(file_id, upload_filename, settings.FILE_ROUTE),
            "url": f"{settings.BASE_URL.strip('/')}{build_file_path(file_id, upload_filename, settings.FILE_ROUTE)}",
            "file_id": file_id,
            "filename": upload_filename,
        }

    return {
        "path": serialized_file["path"],
        "url": serialized_file["url"],
        "file_id": file_id,
        "filename": upload_filename,
        "delete_api": f"{settings.BASE_URL.strip('/')}/api/delete",
        "fullResult": {
            "file_id": file_id,
            "filename": upload_filename,
            "path": serialized_file["path"],
            "url": serialized_file["url"],
            "delete_api": f"{settings.BASE_URL.strip('/')}/api/delete",
        },
    }


@router.get("/d/{file_id}/{filename}")
async def download_file(
    file_id: str,
    filename: str,
    telegram_service: TelegramService = Depends(get_telegram_service),
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Обрабатывает скачивание одиночных файлов и файлов с манифестом чанков."""
    try:
        _, real_file_id = file_id.split(':', 1)
    except ValueError:
        real_file_id = file_id

    download_url = await telegram_service.get_download_url(real_file_id)
    if not download_url:
        raise HTTPException(status_code=404, detail="Файл не найден или ссылка на скачивание истекла.")

    try:
        head_resp = await client.get(download_url, headers={"Range": "bytes=0-127"})
        head_resp.raise_for_status()
        first_bytes = head_resp.content
        if first_bytes.startswith(b'tgstate-blob\n'):
            if head_resp.status_code == 206:
                manifest_resp = await client.get(download_url)
                manifest_resp.raise_for_status()
                manifest_content = manifest_resp.content
            else:
                manifest_content = first_bytes
        else:
            manifest_content = None
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Не удалось получить файл из Telegram.") from exc

    if manifest_content is not None:
        try:
            lines = manifest_content.decode('utf-8').strip().split('\n')
        except (httpx.HTTPError, UnicodeDecodeError) as exc:
            raise HTTPException(status_code=503, detail="Не удалось прочитать манифест файла.") from exc

        if len(lines) < 2:
            raise HTTPException(status_code=502, detail="Неверный формат манифеста файла.")

        original_filename = lines[1]
        chunk_file_ids = lines[2:]
        filename_encoded = quote(str(original_filename), safe="")
        response_headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename_encoded}"
        }
        return StreamingResponse(
            stream_chunks(chunk_file_ids, telegram_service, client),
            headers=response_headers,
        )

    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp')
    is_image = filename.lower().endswith(image_extensions)
    content_type, _ = mimetypes.guess_type(filename)
    if content_type is None:
        content_type = "application/octet-stream"

    filename_encoded = quote(str(filename), safe="")
    disposition_type = "inline" if is_image else "attachment"
    response_headers = {
        "Content-Disposition": f"{disposition_type}; filename*=UTF-8''{filename_encoded}",
        "Content-Type": content_type,
    }

    if manifest_content is None and len(first_bytes) > 128:
        async def buffered_streamer():
            yield first_bytes

        return StreamingResponse(buffered_streamer(), headers=response_headers)

    async def single_file_streamer():
        async with client.stream("GET", download_url) as resp:
            resp.raise_for_status()
            async for chunk in resp.aiter_bytes():
                yield chunk

    return StreamingResponse(single_file_streamer(), headers=response_headers)


@router.get("/api/file-updates")
async def file_updates(request: Request):
    """Рассылает события добавления и удаления файлов на клиенты через SSE."""
    subscriber_queue = await subscribe_file_updates()

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    print("Клиент отключился, отправка событий остановлена.")
                    break

                try:
                    update_json = await asyncio.wait_for(subscriber_queue.get(), timeout=30)
                    yield {"data": update_json}
                except asyncio.TimeoutError:
                    continue
                except Exception as exc:
                    print(f"Ошибка при отправке события SSE: {exc}")
        finally:
            await unsubscribe_file_updates(subscriber_queue)

    return EventSourceResponse(event_generator())


@router.get("/api/files")
async def get_files_list(
    page: int = 1,
    page_size: int = 50,
    settings: Settings = Depends(get_settings),
):
    """Возвращает постраничный список файлов из базы данных."""
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    files, total = await asyncio.gather(
        asyncio.to_thread(database.get_files_page, page_size, (page - 1) * page_size),
        asyncio.to_thread(database.count_files),
    )
    return {
        "items": [_serialize_file(file_info, settings) for file_info in files],
        "page": page,
        "page_size": page_size,
        "total": total,
    }


@router.delete("/api/files/{file_id}")
async def delete_file(
    file_id: str,
    request: Request,
    key: Optional[str] = None,
    settings: Settings = Depends(get_settings),
    telegram_service: TelegramService = Depends(get_telegram_service),
    x_api_key: Optional[str] = Header(None),
):
    """Удаляет одиночный файл и синхронизирует состояние веб-интерфейса и Telegram."""
    _ensure_request_authorized(request, settings, x_api_key or key)
    return await _delete_file_and_sync(file_id, telegram_service)


@router.post("/api/delete")
@router.post("/api/piclist/delete")
async def delete_files_for_piclist(
    request: Request,
    payload: Any = Body(...),
    settings: Settings = Depends(get_settings),
    telegram_service: TelegramService = Depends(get_telegram_service),
    x_api_key: Optional[str] = Header(None),
):
    """Точка входа для удаления файлов через PicList или пользовательские запросы."""
    key = None
    if isinstance(payload, dict):
        key = payload.get("key")

    _ensure_request_authorized(request, settings, x_api_key or key)
    file_ids = _extract_delete_targets(payload, settings)
    if not file_ids:
        raise HTTPException(status_code=400, detail="В теле запроса не найден пригодный file_id.")

    deleted = []
    failed = []
    for file_id in file_ids:
        try:
            deleted.append(await _delete_file_and_sync(file_id, telegram_service))
        except HTTPException as exc:
            failed.append(exc.detail)

    return {
        "status": "completed",
        "deleted": deleted,
        "failed": failed,
    }


@router.post("/api/set-password")
async def set_password(
    payload: PasswordRequest,
    request: Request,
    settings: Settings = Depends(get_settings),
    x_api_key: Optional[str] = Header(None),
):
    """Устанавливает или обновляет пароль доступа к приложению."""
    _ensure_request_authorized(
        request,
        settings,
        x_api_key,
        payload.current_password,
    )
    password = payload.password.strip()
    if not password:
        raise HTTPException(status_code=400, detail="Пароль не может быть пустым.")

    try:
        with open(".password", "w", encoding="utf-8") as file:
            file.write(password)

        return JSONResponse(
            status_code=200,
            content={"status": "ok", "message": "Пароль успешно установлен."},
        )
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Не удалось записать файл пароля.") from exc


@router.post("/api/batch_delete")
async def batch_delete_files(
    request_data: BatchDeleteRequest,
    request: Request,
    key: Optional[str] = None,
    settings: Settings = Depends(get_settings),
    telegram_service: TelegramService = Depends(get_telegram_service),
    x_api_key: Optional[str] = Header(None),
):
    """Пакетное удаление файлов."""
    _ensure_request_authorized(request, settings, x_api_key or key)

    successful_deletions = []
    failed_deletions = []
    for file_id in request_data.file_ids:
        try:
            successful_deletions.append(await _delete_file_and_sync(file_id, telegram_service))
        except HTTPException as exc:
            failed_deletions.append(exc.detail)

    return {
        "status": "completed",
        "deleted": successful_deletions,
        "failed": failed_deletions,
    }


async def stream_chunks(
    chunk_composite_ids: list[str],
    telegram_service: TelegramService,
    client: httpx.AsyncClient,
):
    """Потоковая передача чанков с предварительным получением ссылки на следующий чанк."""
    actual_chunk_ids: list[tuple[str, str]] = []
    for chunk_id in chunk_composite_ids:
        try:
            _, actual_chunk_id = chunk_id.split(':', 1)
            actual_chunk_ids.append((chunk_id, actual_chunk_id))
        except (ValueError, IndexError):
            print(f"Предупреждение: неверный формат ID чанка '{chunk_id}', пропущено.")

    if not actual_chunk_ids:
        return

    url_task = asyncio.create_task(
        telegram_service.get_download_url(actual_chunk_ids[0][1])
    )
    for index, (chunk_id, actual_chunk_id) in enumerate(actual_chunk_ids):
        chunk_url = await url_task
        if index + 1 < len(actual_chunk_ids):
            url_task = asyncio.create_task(
                telegram_service.get_download_url(actual_chunk_ids[index + 1][1])
            )

        if not chunk_url:
            print(f"Предупреждение: не удалось получить ссылку скачивания для чанка {actual_chunk_id}, пропущено.")
            continue

        try:
            async with client.stream('GET', chunk_url) as chunk_resp:
                if chunk_resp.status_code != 200:
                    print(f"Ошибка: не удалось загрузить чанк {chunk_id}, код ответа: {chunk_resp.status_code}")
                    await asyncio.sleep(1)
                    chunk_url = await telegram_service.get_download_url(actual_chunk_id)
                    if not chunk_url:
                        print(f"Повторная попытка не удалась: не удалось получить новую ссылку для чанка {chunk_id}.")
                        break

                    async with client.stream('GET', chunk_url) as retry_resp:
                        retry_resp.raise_for_status()
                        async for chunk_data in retry_resp.aiter_bytes():
                            yield chunk_data
                else:
                    async for chunk_data in chunk_resp.aiter_bytes():
                        yield chunk_data
        except httpx.RequestError as exc:
            print(f"Сетевая ошибка при потоковой передаче чанка {chunk_id}: {exc}")
            break