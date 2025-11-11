"""Database operations for migration and validation."""

import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from urllib.parse import urlparse

try:
    import aiosqlite

    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    import asyncpg

    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    url: str
    engine: str  # "sqlite", "postgres", "mysql"
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


class DatabaseManager:
    """Manages database connections and operations for migration."""

    def __init__(self, database_url: str):
        self.config = self._parse_url(database_url)
        self.connection = None

    def _parse_url(self, url: str) -> DatabaseConfig:
        """Parse database URL into configuration."""
        parsed = urlparse(url)

        engine = parsed.scheme
        if engine not in ["sqlite", "postgresql", "mysql"]:
            raise ValueError(f"Unsupported database engine: {engine}")

        return DatabaseConfig(
            url=url,
            engine=engine,
            host=parsed.hostname,
            port=parsed.port,
            database=parsed.path.lstrip("/") if engine != "sqlite" else parsed.path,
            username=parsed.username,
            password=parsed.password,
        )

    async def connect(self):
        """Establish database connection."""
        if self.config.engine == "sqlite":
            if not SQLITE_AVAILABLE:
                raise ImportError("aiosqlite is required for SQLite support")
            self.connection = await aiosqlite.connect(self.config.database)

        elif self.config.engine == "postgresql":
            if not POSTGRES_AVAILABLE:
                raise ImportError("asyncpg is required for PostgreSQL support")
            self.connection = await asyncpg.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
            )

        else:
            raise NotImplementedError(
                f"Engine {self.config.engine} not yet implemented"
            )

    async def disconnect(self):
        """Close database connection."""
        if self.connection:
            if self.config.engine == "sqlite":
                await self.connection.close()
            elif self.config.engine == "postgresql":
                await self.connection.close()
            self.connection = None

    async def ensure_schema(self):
        """Ensure database schema exists for migration."""
        if self.config.engine == "sqlite":
            await self._ensure_sqlite_schema()
        elif self.config.engine == "postgresql":
            await self._ensure_postgres_schema()

    async def _ensure_sqlite_schema(self):
        """Ensure SQLite schema exists."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS migration_status (
            table_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            version INTEGER,
            files_processed INTEGER DEFAULT 0,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS delta_files (
            table_id TEXT NOT NULL,
            path TEXT NOT NULL,
            size INTEGER,
            modification_time INTEGER,
            deletion_timestamp INTEGER,
            data_change BOOLEAN DEFAULT TRUE,
            stats TEXT,
            tags TEXT,
            partition_values TEXT,
            action_type TEXT NOT NULL,
            version INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (table_id, path, version)
        );
        
        CREATE INDEX IF NOT EXISTS idx_delta_files_table_id ON delta_files(table_id);
        CREATE INDEX IF NOT EXISTS idx_delta_files_path ON delta_files(path);
        CREATE INDEX IF NOT EXISTS idx_delta_files_version ON delta_files(version);
        """

        await self.connection.executescript(schema_sql)

    async def _ensure_postgres_schema(self):
        """Ensure PostgreSQL schema exists."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS migration_status (
            table_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            version INTEGER,
            files_processed INTEGER DEFAULT 0,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS delta_files (
            table_id TEXT NOT NULL,
            path TEXT NOT NULL,
            size BIGINT,
            modification_time BIGINT,
            deletion_timestamp BIGINT,
            data_change BOOLEAN DEFAULT TRUE,
            stats TEXT,
            tags TEXT,
            partition_values TEXT,
            action_type TEXT NOT NULL,
            version INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (table_id, path, version)
        );
        
        CREATE INDEX IF NOT EXISTS idx_delta_files_table_id ON delta_files(table_id);
        CREATE INDEX IF NOT EXISTS idx_delta_files_path ON delta_files(path);
        CREATE INDEX IF NOT EXISTS idx_delta_files_version ON delta_files(version);
        """

        await self.connection.execute(schema_sql)

    async def bulk_insert_files(
        self, table_id: str, files: List[Dict[str, Any]]
    ) -> None:
        """Bulk insert file actions into database."""
        if not files:
            return

        if self.config.engine == "sqlite":
            await self._bulk_insert_files_sqlite(table_id, files)
        elif self.config.engine == "postgresql":
            await self._bulk_insert_files_postgres(table_id, files)

    async def _bulk_insert_files_sqlite(
        self, table_id: str, files: List[Dict[str, Any]]
    ) -> None:
        """Bulk insert files using SQLite."""
        insert_sql = """
        INSERT OR REPLACE INTO delta_files (
            table_id, path, size, modification_time, deletion_timestamp,
            data_change, stats, tags, partition_values, action_type, version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data = []
        for file in files:
            data.append(
                (
                    table_id,
                    file["path"],
                    file.get("size"),
                    file.get("modificationTime"),
                    file.get("deletionTimestamp"),
                    file.get("dataChange", True),
                    file.get("stats", ""),
                    file.get("tags", "{}"),
                    file.get("partitionValues", "{}"),
                    file["action_type"],
                    file["version"],
                )
            )

        await self.connection.executemany(insert_sql, data)
        await self.connection.commit()

    async def _bulk_insert_files_postgres(
        self, table_id: str, files: List[Dict[str, Any]]
    ) -> None:
        """Bulk insert files using PostgreSQL COPY."""
        # Convert files to CSV format for COPY
        import io
        import csv

        output = io.StringIO()
        writer = csv.writer(output)

        for file in files:
            writer.writerow(
                [
                    table_id,
                    file["path"],
                    file.get("size", ""),
                    file.get("modificationTime", ""),
                    file.get("deletionTimestamp", ""),
                    file.get("dataChange", True),
                    file.get("stats", ""),
                    file.get("tags", "{}"),
                    file.get("partitionValues", "{}"),
                    file["action_type"],
                    file["version"],
                ]
            )

        output.seek(0)

        copy_sql = """
        COPY delta_files (
            table_id, path, size, modification_time, deletion_timestamp,
            data_change, stats, tags, partition_values, action_type, version
        ) FROM STDIN WITH (FORMAT CSV, NULL '')
        """

        await self.connection.execute(copy_sql, output.getvalue())

    async def update_migration_status(
        self,
        table_id: str,
        status: str,
        version: Optional[int] = None,
        files_processed: Optional[int] = None,
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
    ) -> None:
        """Update migration status for a table."""
        if self.config.engine == "sqlite":
            if started_at:
                sql = """
                INSERT OR REPLACE INTO migration_status 
                (table_id, status, version, files_processed, error_message, started_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                await self.connection.execute(
                    sql,
                    (
                        table_id,
                        status,
                        version,
                        files_processed,
                        error_message,
                        started_at,
                    ),
                )
            else:
                sql = """
                INSERT OR REPLACE INTO migration_status 
                (table_id, status, version, files_processed, error_message, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                await self.connection.execute(
                    sql, (table_id, status, version, files_processed, error_message)
                )
            await self.connection.commit()

        elif self.config.engine == "postgresql":
            if started_at:
                sql = """
                INSERT INTO migration_status 
                (table_id, status, version, files_processed, error_message, started_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                ON CONFLICT (table_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    version = EXCLUDED.version,
                    files_processed = EXCLUDED.files_processed,
                    error_message = EXCLUDED.error_message,
                    started_at = COALESCE(EXCLUDED.started_at, migration_status.started_at),
                    updated_at = CURRENT_TIMESTAMP
                """
                await self.connection.execute(
                    sql,
                    table_id,
                    status,
                    version,
                    files_processed,
                    error_message,
                    started_at,
                )
            else:
                sql = """
                INSERT INTO migration_status 
                (table_id, status, version, files_processed, error_message, updated_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (table_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    version = EXCLUDED.version,
                    files_processed = EXCLUDED.files_processed,
                    error_message = EXCLUDED.error_message,
                    updated_at = CURRENT_TIMESTAMP
                """
                await self.connection.execute(
                    sql, table_id, status, version, files_processed, error_message
                )

    async def get_migration_status(self, table_id: str) -> Optional[Dict[str, Any]]:
        """Get migration status for a table."""
        if self.config.engine == "sqlite":
            cursor = await self.connection.execute(
                "SELECT * FROM migration_status WHERE table_id = ?", (table_id,)
            )
            row = await cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))

        elif self.config.engine == "postgresql":
            row = await self.connection.fetchrow(
                "SELECT * FROM migration_status WHERE table_id = $1", table_id
            )
            if row:
                return dict(row)

        return None

    async def get_all_migration_status(self) -> List[Dict[str, Any]]:
        """Get migration status for all tables."""
        if self.config.engine == "sqlite":
            cursor = await self.connection.execute("SELECT * FROM migration_status")
            rows = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        elif self.config.engine == "postgresql":
            rows = await self.connection.fetch("SELECT * FROM migration_status")
            return [dict(row) for row in rows]

        return []

    async def get_file_snapshot(self, table_id: str) -> List[Dict[str, Any]]:
        """Get file snapshot for a table from SQL database."""
        if self.config.engine == "sqlite":
            cursor = await self.connection.execute(
                "SELECT * FROM delta_files WHERE table_id = ? ORDER BY version",
                (table_id,),
            )
            rows = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        elif self.config.engine == "postgresql":
            rows = await self.connection.fetch(
                "SELECT * FROM delta_files WHERE table_id = $1 ORDER BY version",
                table_id,
            )
            return [dict(row) for row in rows]

        return []

    async def table_exists(self, table_id: str) -> bool:
        """Check if a table exists in the migration status."""
        status = await self.get_migration_status(table_id)
        return status is not None

    async def get_file_count(self, table_id: str) -> int:
        """Get the number of files for a table."""
        if self.config.engine == "sqlite":
            cursor = await self.connection.execute(
                "SELECT COUNT(*) FROM delta_files WHERE table_id = ?", (table_id,)
            )
            result = await cursor.fetchone()
            return result[0] if result else 0

        elif self.config.engine == "postgresql":
            result = await self.connection.fetchval(
                "SELECT COUNT(*) FROM delta_files WHERE table_id = $1", table_id
            )
            return result if result else 0

        return 0
