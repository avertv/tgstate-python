import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

# Определение пути к базе данных
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_URL = os.path.join(BASE_DIR, "file_metadata.db")

# Использование блокировки потоков для обеспечения безопасности доступа к БД в многопоточной среде
_db_lock = threading.Lock()


def get_db_connection():
    """Получает соединение с базой данных SQLite."""
    conn = sqlite3.connect(DATABASE_URL, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Инициализирует базу данных и создаёт необходимые таблицы и индексы."""
    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    file_id TEXT NOT NULL UNIQUE,
                    filesize INTEGER NOT NULL,
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_files_upload_date_id
                ON files(upload_date DESC, id DESC);
                """
            )
            conn.commit()
        finally:
            conn.close()


def add_file_metadata(
    filename: str,
    file_id: str,
    filesize: int,
    upload_date: str | None = None,
) -> bool:
    """
    Добавляет метаданные файла в БД с автоматическим обновлением при совпадении имени.
    Возвращает True, если была создана новая запись или обновлена существующая.
    """
    if not upload_date:
        upload_date = datetime.now(timezone.utc).isoformat()

    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            # 1. Проверяем точное совпадение по file_id
            cursor.execute("SELECT id FROM files WHERE file_id = ?", (file_id,))
            if cursor.fetchone():
                return False

            # 2. Дедупликация: проверяем, существует ли файл с таким же именем
            cursor.execute("SELECT id, file_id FROM files WHERE filename = ?", (filename,))
            existing = cursor.fetchone()

            if existing:
                # Обновляем запись (заменяем устаревший file_id и формат даты)
                cursor.execute(
                    """
                    UPDATE files 
                    SET file_id = ?, filesize = ?, upload_date = ? 
                    WHERE id = ?
                    """,
                    (file_id, filesize, upload_date, existing["id"])
                )
                conn.commit()
                print(f"Метаданные файла обновлены (устранён дубликат): {filename}")
                return True

            # 3. Вставляем новую запись
            cursor.execute(
                """
                INSERT INTO files (filename, file_id, filesize, upload_date)
                VALUES (?, ?, ?, ?)
                """,
                (filename, file_id, filesize, upload_date)
            )
            conn.commit()
            print(f"Метаданные файла добавлены: {filename}")
            return True
        finally:
            conn.close()


def get_files_page(
    limit: int,
    offset: int = 0,
    *,
    images_only: bool = False,
) -> list[dict[str, Any]]:
    """Возвращает постраничный список файлов, отсортированный по дате загрузки в обратном порядке."""
    where_clause = ""
    parameters: list[Any] = []
    if images_only:
        where_clause = (
            "WHERE lower(filename) LIKE ? OR lower(filename) LIKE ? "
            "OR lower(filename) LIKE ? OR lower(filename) LIKE ? "
            "OR lower(filename) LIKE ? OR lower(filename) LIKE ?"
        )
        parameters.extend(("%.jpg", "%.jpeg", "%.png", "%.gif", "%.bmp", "%.webp"))

    parameters.extend((limit, offset))
    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT filename, file_id, filesize, upload_date
                FROM files
                {where_clause}
                ORDER BY upload_date DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                parameters,
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()


def count_files(*, images_only: bool = False) -> int:
    """Возвращает общее количество файлов в БД. Для страницы галереи может фильтровать только изображения."""
    where_clause = ""
    parameters: tuple[str, ...] = ()
    if images_only:
        where_clause = (
            "WHERE lower(filename) LIKE ? OR lower(filename) LIKE ? "
            "OR lower(filename) LIKE ? OR lower(filename) LIKE ? "
            "OR lower(filename) LIKE ? OR lower(filename) LIKE ?"
        )
        parameters = ("%.jpg", "%.jpeg", "%.png", "%.gif", "%.bmp", "%.webp")

    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM files {where_clause}", parameters)
            return int(cursor.fetchone()[0])
        finally:
            conn.close()


def get_all_files() -> list[dict[str, Any]]:
    """Получает метаданные всех файлов из базы данных."""
    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT filename, file_id, filesize, upload_date FROM files ORDER BY upload_date DESC"
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()


def get_file_info(file_id: str) -> dict[str, Any] | None:
    """Возвращает полные метаданные одного файла по его file_id."""
    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT filename, file_id, filesize, upload_date FROM files WHERE file_id = ?",
                (file_id,)
            )
            result = cursor.fetchone()
            return dict(result) if result else None
        finally:
            conn.close()


def get_file_by_id(file_id: str) -> dict[str, Any] | None:
    """Функция обратной совместимости, возвращает словарь с именем и размером файла."""
    result = get_file_info(file_id)
    if not result:
        return None
    return {
        "filename": result["filename"],
        "filesize": result["filesize"]
    }


def delete_file_metadata(file_id: str) -> bool:
    """
    Удаляет метаданные файла из базы данных по его file_id.
    Возвращает True, если строка была успешно удалена, иначе False.
    """
    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE file_id = ?", (file_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()


def delete_file_by_message_id(message_id: int) -> str | None:
    """
    Удаляет метаданные файла из базы данных по message_id и возвращает его file_id.
    Так как одно главное сообщение соответствует одному файлу, удаление происходит напрямую.
    """
    file_id_to_delete = None
    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT file_id FROM files WHERE file_id LIKE ?",
                (f"{message_id}:%",)
            )
            result = cursor.fetchone()
            if result:
                file_id_to_delete = result[0]
                cursor.execute("DELETE FROM files WHERE file_id = ?", (file_id_to_delete,))
                conn.commit()
                print(
                    f"Удалена из базы данных запись для сообщения ID {message_id} , файл: {file_id_to_delete}"
                )
            return file_id_to_delete
        finally:
            conn.close()