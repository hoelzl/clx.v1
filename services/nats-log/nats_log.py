import asyncio
import logging
import os
import signal

import nats
from nats.aio.msg import Msg
from nats.errors import NoServersError

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - nats-log - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = asyncio.Event()


async def connect_client_with_retry(nats_url, num_retries=5):
    await asyncio.sleep(1)
    for i in range(num_retries):
        try:
            nc = await nats.connect(nats_url)
            logger.info(f"Connected to NATS at {nats_url}")
            return nc
        except TimeoutError as e:
            logger.info(f"Timeout connecting to NATS: {e}")
            await asyncio.sleep(2**i)
        except Exception as e:
            logger.error(f"Error connecting to NATS: {e}")
            break
    raise OSError("Could not connect to NATS")


async def shutdown_handler(msg: Msg):
    logger.info("Received shutdown command. Initiating graceful shutdown...")
    shutdown_flag.set()


async def main():
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")

    try:
        nc = await connect_client_with_retry(nats_url)

        async def message_handler(msg: Msg):
            subject = msg.subject
            reply = msg.reply
            data = msg.data.decode()
            logger.info(f"Received a message on '{subject} {reply}': {data}")
            await msg.ack()

        # Subscribe to all subjects
        await nc.subscribe("event.>", cb=message_handler)
        await nc.subscribe("command.>", cb=message_handler)

        # Subscribe to the stop command
        await nc.subscribe("command.nats_log.shutdown", cb=shutdown_handler)

        logger.info("Listening for messages...")

        # Keep the connection open until shutdown is requested
        while not shutdown_flag.is_set():
            await asyncio.sleep(1)

        logger.info("Shutting down...")
        await nc.close()

    except NoServersError as e:
        logger.error(f"Could not connect to NATS: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")


def signal_handler():
    logger.info("Received interrupt signal. Initiating graceful shutdown...")
    shutdown_flag.set()


if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: signal_handler())

    asyncio.run(main())
