import asyncio
import logging
import os

import nats
from nats.js.api import RetentionPolicy
from nats.js.errors import NotFoundError

# Set up logging

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - nats-init - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


FORCE_DELETE_STREAMS = os.environ.get("FORCE_DELETE_STREAMS", True)


async def create_streams():
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")
    nc = await nats.connect(nats_url)

    try:
        logger.info(f"Connected to NATS at {nats_url}")
        js = nc.jetstream()
        await create_stream(js, "EVENTS", "event")
        await create_stream(
            js,
            "COMMANDS",
            "command",
            retention=RetentionPolicy.WORK_QUEUE,
        )
    except Exception as e:
        logger.error(f"Error connecting to NATS: {e}")
    finally:
        await nc.close()
        logger.info("NATS connection closed")


async def create_stream(js, stream_name, event_prefix, **kwargs):
    try:
        if FORCE_DELETE_STREAMS:
            try:
                logger.info(f"Force-deleting {stream_name} stream")
                await js.delete_stream(stream_name)
            except NotFoundError:
                logger.info(f"{stream_name} stream does not exist")
        for i in range(5):
            try:
                logger.debug(f"Trying to determine if stream {stream_name}  exists")
                if await does_stream_exist(js, stream_name):
                    logger.debug(f"Stream {stream_name} exists")
                    return
                logger.debug(f"Stream {stream_name} does not exist, trying to create")
                await js.add_stream(
                    name=stream_name,
                    subjects=[f"{event_prefix}.>"],
                    **kwargs,
                )
                logger.info(f"{stream_name} stream created successfully")
                break
            except TimeoutError as e:
                logger.info(f"Timeout creating {stream_name} stream: {e}")
                await asyncio.sleep(1)
            except NotFoundError as e:
                logger.info(
                    f"No NATS server found when creating stream {stream_name}: {e}"
                )
            await asyncio.sleep(i)
    except Exception as e:
        logger.error(f"Error creating {stream_name} stream: {e}")


async def does_stream_exist(js, name):
    try:
        await js.stream_info(name)
        logger.info(f"{name} stream already exists")
        return True
    except Exception as e:
        logger.debug(f"{name} stream does not exist: {e}")
    return False


async def main():
    logger.info("Starting NATS initialization service")
    await create_streams()
    logger.info("NATS initialization complete. Shutting down initialization service")


if __name__ == "__main__":
    asyncio.run(main())
