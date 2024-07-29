import asyncio
import hashlib
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import aiofiles
import click
import nats
import nats.errors
import yaml
from watchdog.events import FileSystemEventHandler

# from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver as Observer

# Set up logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - file-watcher - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class WatchedDirectory:
    tag: str
    path: Path
    pattern: str


class FileWatcher:

    def __init__(
        self,
        config_path: Path,
        nats_url: str = "nats://nats:4222",
        db_path: Path = Path("watched_files.db"),
    ):
        self.config = self.load_config(config_path)
        self.db_path = db_path
        self.ensure_db_exists()
        self.conn = sqlite3.connect(str(self.db_path))
        self.create_table_if_necessary()
        self.event_queue = asyncio.Queue()
        self.shutdown_event = asyncio.Event()
        self.nats_url = nats_url
        self.nats_client: nats.NATS | None = None
        self.jetstream = None
        self.command_sub = None
        logger.info(f"FileWatcher initialized with config from {config_path}")

    @staticmethod
    def load_config(config_path: Path) -> Dict[str, WatchedDirectory]:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return {
            tag: WatchedDirectory(tag, Path(directory["path"]), directory["pattern"])
            for tag, directory in config["watched_directories"].items()
        }

    def ensure_db_exists(self):
        if not self.db_path.exists():
            self.db_path.touch()
            logger.info(f"Created new database file at {self.db_path}")

    def create_table_if_necessary(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watched_files (
                    path TEXT PRIMARY KEY,
                    hash TEXT,
                    tag TEXT
                )
            """
            )
        logger.info("Ensured 'watched_files' table exists in the database")

    async def connect_nats(self):
        await self.connect_client_with_retry()
        self.jetstream = self.nats_client.jetstream()
        self.command_sub = await self.nats_client.subscribe("command.watcher.>")
        logger.info("Subscribed to command.watcher.> subject")

    async def connect_client_with_retry(self, num_retries=5):
        for i in range(num_retries):
            try:
                logger.debug(f"Trying to connect to NATS at {self.nats_url}")
                self.nats_client = await nats.connect(self.nats_url)
                logger.info(f"Connected to NATS at {self.nats_url}")
                return
            except Exception as e:
                logger.error(f"Error connecting to NATS: {e}")
                await asyncio.sleep(2**i)
        raise OSError("Could not connect to NATS")

    async def scan_directories(self):
        logger.info("Starting initial directory scan")
        for watched_dir in self.config.values():
            logger.debug(f"Scanning directory: {watched_dir.path}")
            for file_path in watched_dir.path.glob(watched_dir.pattern):
                await self.process_file(file_path, watched_dir.tag, initial_scan=True)
        logger.info("Initial directory scan completed")

    def file_matches_pattern(self, file_path: Path, tag: str) -> bool:
        watched_dir = self.config[tag]
        return file_path.match(watched_dir.pattern)

    async def process_file(self, file_path: Path, tag: str, initial_scan=False):
        if not self.file_matches_pattern(file_path, tag):
            logger.debug(
                f"Skipping file {file_path} as it doesn't match the pattern "
                f"for tag {tag}"
            )
            return

        file_hash = await self.compute_hash(file_path)
        existing_hash = self.get_hash_from_db(file_path)

        if existing_hash is None:
            self.update_hash_in_db(file_path, file_hash, tag)
            await self.send_event("file.created", file_path, tag, file_hash)
        elif existing_hash != file_hash:
            self.update_hash_in_db(file_path, file_hash, tag)
            await self.send_event("file.updated", file_path, tag, file_hash)
        elif initial_scan:
            await self.send_event("file.unchanged", file_path, tag, file_hash)

    @staticmethod
    async def compute_hash(file_path: Path) -> str:
        async with aiofiles.open(file_path, "rb") as f:
            file_content = await f.read()
        return hashlib.md5(file_content).hexdigest()

    def get_hash_from_db(self, file_path: Path) -> str:
        with self.conn:
            cursor = self.conn.execute(
                "SELECT hash FROM watched_files WHERE path = ?", (str(file_path),)
            )
            result = cursor.fetchone()
        return result[0] if result else None

    def update_hash_in_db(self, file_path: Path, file_hash: str, tag: str):
        with self.conn:
            self.conn.execute(
                (
                    "INSERT OR REPLACE INTO watched_files (path, hash, tag) "
                    "VALUES (?, ?, ?)"
                ),
                (str(file_path), file_hash, tag),
            )
        logger.debug(f"Updated hash in database for file: {file_path}")

    def remove_from_db(self, file_path: Path):
        with self.conn:
            self.conn.execute(
                "DELETE FROM watched_files WHERE path = ?", (str(file_path),)
            )
        logger.debug(f"Removed file from database: {file_path}")

    async def send_event(
        self, event_type: str, file_path: Path, tag: str, file_hash: str
    ):
        payload = {
            "tag": tag,
            "relative_path": str(file_path.relative_to(self.config[tag].path)),
            "full_path": str(file_path),
            "file_name": file_path.name,
            "file_extension": file_path.suffix,
            "hash": file_hash,
        }

        logger.debug(f"Sending event: {event_type} for file: {file_path}")
        logger.debug(f"Payload: {payload}")
        await self.jetstream.publish(
            f"event.{event_type}.{tag}", json.dumps(payload).encode()
        )
        logger.debug(f"Sent event: {event_type} for file: {file_path}")

    async def process_events(self):
        logger.info("Starting event processing")
        while not self.shutdown_event.is_set():
            try:
                event_type, path, tag = await asyncio.wait_for(
                    self.event_queue.get(), timeout=1.0
                )
                logger.debug(f"Processing event: {event_type} for {path}")
                if event_type in ("created", "modified", "deleted"):
                    file_path = Path(path)
                    if self.file_matches_pattern(file_path, tag):
                        if event_type in ("created", "modified"):
                            await self.process_file(file_path, tag)
                        elif event_type == "deleted":
                            self.remove_from_db(file_path)
                            await self.send_event("file.deleted", file_path, tag, "")
                            logger.debug(f"File deleted: {path}")
                    else:
                        logger.debug(
                            f"Skipping {event_type} event for {path} "
                            "as it doesn't match the pattern"
                        )
                elif event_type == "moved":
                    src_path, dest_path = path
                    src_matches = self.file_matches_pattern(Path(src_path), tag)
                    dest_matches = self.file_matches_pattern(Path(dest_path), tag)
                    if src_matches or dest_matches:
                        self.remove_from_db(Path(src_path))
                        await self.process_file(Path(dest_path), tag)
                        await self.send_event("file.moved", Path(dest_path), tag, "")
                        logger.debug(f"File moved from {src_path} to {dest_path}")
                    else:
                        logger.debug(
                            f"Skipping move event for {src_path} to {dest_path} "
                            "as neither matches the pattern"
                        )
                else:
                    logger.error(f"Unknown event type: {event_type}")
                self.event_queue.task_done()
            except asyncio.TimeoutError:
                continue

    async def watch_directories(self):
        logger.info("Starting directory watching")
        observer = Observer()
        for watched_dir in self.config.values():
            event_handler = FileEventHandler(self, watched_dir.tag)
            observer.schedule(event_handler, str(watched_dir.path), recursive=True)
            logger.info(f"Watching directory: {watched_dir.path}")
        observer.start()

        try:
            while True:
                try:
                    command_msg = await self.command_sub.next_msg(timeout=1)
                    command = command_msg.subject.split(".")[2]
                    logger.info(f"Received command: {command}")
                    if command == "rescan":
                        await self.rescan()
                    elif command == "shutdown":
                        self.shutdown_event.set()
                        break
                except TimeoutError:
                    continue
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down...")
        finally:
            observer.stop()
            observer.join()
            logger.info("Directory watching stopped")

    async def rescan(self):
        logger.info("Initiating rescan")
        self.conn.close()
        self.db_path.unlink(missing_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.create_table_if_necessary()
        await self.scan_directories()
        logger.info("Rescan completed")

    async def run(self):
        logger.info("Starting FileWatcher")
        await self.connect_nats()
        logger.debug("Connected to NATS")
        await self.scan_directories()
        event_processor = asyncio.create_task(self.process_events())
        watch_task = asyncio.create_task(self.watch_directories())

        await asyncio.gather(watch_task, event_processor)

        # Clean up
        if self.nats_client:
            await self.nats_client.close()
        self.conn.close()
        logger.info("FileWatcher shut down successfully")


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, file_watcher: FileWatcher, tag: str):
        self.file_watcher = file_watcher
        self.tag = tag

    def on_created(self, event):
        src_path = Path(event.src_path)
        if src_path.is_file() and self.file_watcher.file_matches_pattern(
            src_path, self.tag
        ):
            logger.debug(f"File created: {event.src_path}")
            self.file_watcher.event_queue.put_nowait(
                ("created", event.src_path, self.tag)
            )
        else:
            logger.debug(f"Skipping creation event for {event.src_path}")

    def on_modified(self, event):
        src_path = Path(event.src_path)
        if src_path.is_file() and self.file_watcher.file_matches_pattern(
            src_path, self.tag
        ):
            logger.debug(f"File modified: {event.src_path}")
            self.file_watcher.event_queue.put_nowait(
                ("modified", event.src_path, self.tag)
            )
        else:
            logger.debug(f"Skipping modification event for {event.src_path}")

    def on_deleted(self, event):
        src_path = Path(event.src_path)
        if self.file_watcher.file_matches_pattern(src_path, self.tag):
            logger.debug(f"File deleted: {event.src_path}")
            self.file_watcher.event_queue.put_nowait(
                ("deleted", event.src_path, self.tag)
            )
        else:
            logger.debug(f"Skipping deletion event for {event.src_path}")

    def on_moved(self, event):
        src_path = Path(event.src_path)
        dest_path = Path(event.dest_path)
        if dest_path.is_file() and (
            self.file_watcher.file_matches_pattern(src_path, self.tag)
            or self.file_watcher.file_matches_pattern(dest_path, self.tag)
        ):
            logger.debug(f"File moved from {event.src_path} to {event.dest_path}")
            self.file_watcher.event_queue.put_nowait(
                ("moved", (event.src_path, event.dest_path), self.tag)
            )
        else:
            logger.debug(
                f"Skipping move event from {event.src_path} to {event.dest_path}"
            )


@click.command()
@click.option(
    "--config",
    type=click.Path(exists=True),
    required=True,
    help="Path to the configuration file",
)
@click.option(
    "--db-path",
    type=click.Path(),
    required=False,
    default="watched_files.db",
    help="Path to the SQLite database file",
)
def main(config, db_path):
    logger.info(f"Starting FileWatcher with config: {config}")
    absolute_db_path = Path(db_path).resolve()
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")
    file_watcher = FileWatcher(
        Path(config), nats_url=nats_url, db_path=absolute_db_path
    )
    asyncio.run(file_watcher.run())


if __name__ == "__main__":
    main()
