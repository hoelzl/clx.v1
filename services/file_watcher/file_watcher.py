import asyncio
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import aiofiles
import click
import nats  # noqa
import nats.errors
import yaml
from nats.js.api import RetentionPolicy
from nats.js.errors import BadRequestError
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


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
        self.nats_url = nats_url
        self.nats_client: nats.NATS | None = None
        self.jetstream = None
        self.command_sub = None

    @staticmethod
    def load_config(config_path: Path) -> Dict[str, WatchedDirectory]:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return {
            tag: WatchedDirectory(tag, Path(directory["path"]), directory["pattern"])
            for tag, directory in config["watched_directories"].items()
        }

    def ensure_db_exists(self):
        if not self.db_path.exists():
            self.db_path.touch()

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

    async def connect_nats(self):
        self.nats_client = await nats.connect(self.nats_url)
        self.jetstream = self.nats_client.jetstream()

        try:
            await self.jetstream.add_stream(name="EVENTS", subjects=["event.>"])
        except BadRequestError as ex:
            print(f"Stream EVENTS already exists: {ex}", flush=True)
        try:
            await self.jetstream.add_stream(
                name="COMMANDS",
                subjects=["command.>"],
                retention=RetentionPolicy.WORK_QUEUE,
            )
        except BadRequestError as ex:
            print(f"Stream COMMANDS already exists: {ex}", flush=True)

        self.command_sub = await self.nats_client.subscribe("command.watcher.>")

    async def scan_directories(self):
        for watched_dir in self.config.values():
            for file_path in watched_dir.path.glob(watched_dir.pattern):
                await self.process_file(file_path, watched_dir.tag, initial_scan=True)

    async def process_file(self, file_path: Path, tag: str, initial_scan=False):
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
                "INSERT OR REPLACE INTO watched_files (path, hash, tag) VALUES (?, ?, "
                "?)",
                (str(file_path), file_hash, tag),
            )

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
        await self.jetstream.publish(
            f"event.{event_type}.{tag}", json.dumps(payload).encode()
        )

    async def process_events(self):
        while True:
            event_type, path, tag = await self.event_queue.get()
            if event_type in ("created", "modified"):
                await self.process_file(Path(path), tag)
            elif event_type == "deleted":
                await self.send_event("file.deleted", Path(path), tag, "")
            elif event_type == "moved":
                await self.send_event("file.moved", Path(path), tag, "")
            self.event_queue.task_done()

    async def watch_directories(self):
        observer = Observer()
        for watched_dir in self.config.values():
            event_handler = FileEventHandler(self, watched_dir.tag)
            observer.schedule(event_handler, str(watched_dir.path), recursive=True)
        observer.start()

        try:
            while True:
                try:
                    command_msg = await self.command_sub.next_msg(timeout=1)
                    command = command_msg.subject.split(".")[-1]
                    if command == "rescan":
                        await self.rescan()
                    elif command == "stop":
                        break
                except TimeoutError:
                    continue
        except KeyboardInterrupt:
            pass
        finally:
            observer.stop()
            observer.join()

    async def rescan(self):
        self.conn.close()
        self.db_path.unlink(missing_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.create_table_if_necessary()
        await self.scan_directories()

    async def run(self):
        await self.connect_nats()
        await self.scan_directories()
        event_processor = asyncio.create_task(self.process_events())
        await self.watch_directories()
        await event_processor


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, file_watcher: FileWatcher, tag: str):
        self.file_watcher = file_watcher
        self.tag = tag

    def on_created(self, event):
        self.file_watcher.event_queue.put_nowait(("created", event.src_path, self.tag))

    def on_modified(self, event):
        self.file_watcher.event_queue.put_nowait(("modified", event.src_path, self.tag))

    def on_deleted(self, event):
        self.file_watcher.event_queue.put_nowait(("deleted", event.src_path, self.tag))

    def on_moved(self, event):
        self.file_watcher.event_queue.put_nowait(("moved", event.dest_path, self.tag))


@click.command()
@click.option(
    "--config",
    type=click.Path(exists=True),
    required=True,
    help="Path to the configuration file",
)
@click.option(
    "--nats-url",
    default="nats://localhost:4222",
    help="URL of the NATS server",
)
@click.option(
    "--db-path",
    type=click.Path(),
    required=False,
    default="watched_files.db",
    help="Path to the SQLite database file",
)
def main(config, nats_url, db_path):
    absolute_db_path = Path(db_path).resolve()
    file_watcher = FileWatcher(
        Path(config), nats_url=nats_url, db_path=absolute_db_path
    )
    asyncio.run(file_watcher.run())


if __name__ == "__main__":
    main()
