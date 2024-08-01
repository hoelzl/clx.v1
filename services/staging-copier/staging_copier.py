import asyncio
import json
import logging
import os
import shutil
import signal
import sys
from pathlib import Path

import nats
from nats.aio.msg import Msg
from nats.errors import NoServersError
from nats.js.client import JetStreamContext


def string_to_list(string: str) -> list[str]:
    return [s.strip() for s in string.split(",")]


# Configuration
INPUT_DIR = os.environ.get("INPUT_DIR", "C:/tmp/input")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "C:/tmp/staging")
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.environ.get("STREAM_NAME", "EVENTS")
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "FILE_COPIER")
SUBJECT = os.environ.get("SUBJECT", "event.file.*.staging.>")
QUEUE_GROUP = os.environ.get("QUEUE_GROUP", "FILE_COPIER")
NOTEBOOK_PREFIX = os.environ.get("NOTEBOOK_PREFIX", "slides_")
NOTEBOOK_EXTENSION = os.environ.get("NOTEBOOK_EXTENSION", ".py")
LANGUAGES = string_to_list(os.environ.get("LANGUAGES", "de,en"))
NOTEBOOK_FORMATS = string_to_list(
    os.environ.get("NOTEBOOK_FORMATS", "notebook,code,html")
)
OUTPUT_TYPES = string_to_list(
    os.environ.get("OUTPUT_TYPES", "completed,codealong,speaker")
)

# Logging setup
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - staging-copier - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = asyncio.Event()


def path_fragment(lang, notebook_format, target):
    return f"{lang}/{notebook_format}/{target}"


def output_dir(lang, notebook_format, output_type) -> Path:
    return Path(OUTPUT_DIR) / path_fragment(lang, notebook_format, output_type)


async def is_notebook_file(input_path):
    return (
        input_path.name.startswith(NOTEBOOK_PREFIX)
        and input_path.suffix == NOTEBOOK_EXTENSION
    )


async def connect_client_with_retry(nats_url: str, num_retries: int = 5):
    for i in range(num_retries):
        try:
            logger.debug(f"Trying to connect to NATS at {nats_url}")
            nc = await nats.connect(nats_url)
            logger.info(f"Connected to NATS at {nats_url}")
            return nc
        except Exception as e:
            logger.error(f"Error connecting to NATS: {e}")
            await asyncio.sleep(2**i)
    raise OSError("Could not connect to NATS")


async def connect_jetstream(nats_url: str) -> tuple[nats.NATS, JetStreamContext]:
    try:
        nc = await connect_client_with_retry(nats_url)
        js = nc.jetstream()
        logger.info(f"Connected to JetStream at {nats_url}")
        return nc, js
    except NoServersError:
        logger.fatal(f"Could not connect to NATS server at {nats_url}.")
        raise
    except Exception as e:
        logger.fatal(f"Error connecting to NATS server: {str(e)}")
        raise


async def copy_file(
    input_path: str,
    relative_path: str,
    lang: str,
    notebook_format: str,
    output_type: str,
):
    input_path = Path(input_path)
    if not input_path.exists():
        logger.error(f"Input file does not exist: {input_path}")
        return

    if await is_notebook_file(input_path):
        logger.debug(f"Skipping notebook file: {input_path}")
        return

    output_path = output_dir(lang, notebook_format, output_type) / relative_path
    logger.debug(f"Copying {input_path} to {output_path}")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if input_path.is_dir():
            shutil.copytree(input_path, output_path)
        else:
            shutil.copy2(input_path, output_path)
    except Exception as e:
        logger.error(f"Error copying file {input_path} to {output_path}: {str(e)}")


async def process_message(msg: Msg):
    try:
        data = json.loads(msg.data.decode())
        logger.debug(f"Received message: {data}")
        absolute_path = data.get("absolute_path")
        relative_path = data.get("relative_path")
        if absolute_path and relative_path:
            logger.debug(f"Processing message for file: {absolute_path}")
            action = get_event_action(msg)
            if action in ["created", "modified"]:
                await copy_file_to_staging_dirs(absolute_path, relative_path)
            elif action == "deleted":
                await delete_file_in_staging_dirs(relative_path)
            elif action == "moved":
                old_relative_path = data.get("old_relative_path")
                await move_file_in_staging_dirs(old_relative_path, relative_path)
            else:
                logger.debug(f"Skipping event action: {action}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        # await msg.nak()


async def copy_file_to_staging_dirs(absolute_path, relative_path):
    logger.debug(f"Copying file to staging directories: {relative_path}")
    for lang in LANGUAGES:
        for notebook_format in NOTEBOOK_FORMATS:
            for output_type in OUTPUT_TYPES:
                await copy_file(
                    absolute_path, relative_path, lang, notebook_format, output_type
                )


async def delete_file_in_staging_dirs(relative_path):
    logger.debug(f"Deleting file in staging dirs: {relative_path}")
    for lang in LANGUAGES:
        for notebook_format in NOTEBOOK_FORMATS:
            for output_type in OUTPUT_TYPES:
                output_path = (
                    output_dir(lang, notebook_format, output_type) / relative_path
                )
                if output_path.exists():
                    logger.debug(f"Deleting {output_path}")
                    if output_path.is_dir():
                        shutil.rmtree(output_path)
                    else:
                        output_path.unlink()


async def move_file_in_staging_dirs(old_relative_path, relative_path):
    logger.debug(f"Moving file in staging dirs: {relative_path}")
    if old_relative_path:
        logger.debug(f"Found old path: {old_relative_path} -> {relative_path}")
        for lang in LANGUAGES:
            for notebook_format in NOTEBOOK_FORMATS:
                for output_type in OUTPUT_TYPES:
                    old_output_path = (
                        output_dir(lang, notebook_format, output_type)
                        / old_relative_path
                    )
                    output_path = (
                        output_dir(lang, notebook_format, output_type) / relative_path
                    )
                    if old_output_path.exists():
                        logger.debug(
                            "Moving file in staging dir: "
                            f"{old_output_path} -> {output_path}"
                        )
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(old_output_path, output_path)
    else:
        logger.error(f"No old path found when moving {relative_path}")


async def run_consumer(js: JetStreamContext):
    sub = None
    try:
        logger.debug(f"Trying to subscribe to {SUBJECT!r}")
        sub = await js.subscribe(
            SUBJECT,
            stream=STREAM_NAME,
            queue=QUEUE_GROUP,
        )
        logger.info(f"Subscribed to {SUBJECT!r} on stream {STREAM_NAME!r}")
        while not shutdown_flag.is_set():
            try:
                msg = await sub.next_msg(timeout=1)
                logger.debug(f"Received message: {msg}")
                await msg.ack()
                await process_message(msg)
            except nats.errors.TimeoutError:
                # logger.debug("No messages available")
                continue
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        if sub:
            logger.debug("Unsubscribing from subscription")
            await sub.unsubscribe()


def get_event_action(msg):
    try:
        return msg.subject.split(".")[2]
    except IndexError:
        return "<error-action>"


async def shutdown_handler():
    logger.info("Received shutdown command. Initiating graceful shutdown...")
    shutdown_flag.set()


def signal_handler():
    logger.info("Received interrupt signal. Initiating graceful shutdown...")
    asyncio.create_task(shutdown_handler())


def restart_handler(_signum, _frame):
    logger.info("Received restart signal. Restarting application...")
    os.execv(sys.executable, ["python"] + sys.argv)


async def main():
    nc, js = await connect_jetstream(NATS_URL)

    consumer_task = asyncio.create_task(run_consumer(js))

    await shutdown_flag.wait()
    await consumer_task
    await nc.close()


if __name__ == "__main__":
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: signal_handler())
    signal.signal(signal.SIGUSR1, restart_handler)

    asyncio.run(main())
