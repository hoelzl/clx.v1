import asyncio
import json
import logging
import os
import signal
from pathlib import Path

import nats
import yaml
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy

# Configuration
CONFIG_PATH = os.environ.get("CONFIG_PATH", "config.yaml")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
QUEUE_GROUP = os.environ.get("QUEUE_GROUP", "PLANTUML_CONVERTER")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - plantuml-converter - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PlantUMLConverter:
    def __init__(self, config_path: str, nats_url: str):
        self.config_path = config_path
        self.nats_url = nats_url
        self.plantuml_tags = {}
        self.nats_client: nats.NATS | None = None
        self.jetstream: JetStreamContext | None = None
        self.shutdown_event = asyncio.Event()
        self.load_config()

    def load_config(self):
        try:
            with open(self.config_path, "r") as config_file:
                config = yaml.safe_load(config_file)
                self.plantuml_tags = config.get("plantuml_tags", {})
            logger.info(f"Loaded configuration from {self.config_path}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    async def connect_nats(self):
        try:
            self.nats_client = await nats.connect(self.nats_url)
            self.jetstream = self.nats_client.jetstream()
            logger.info(f"Connected to NATS at {self.nats_url}")
        except Exception as e:
            logger.error(f"Error connecting to NATS: {e}")
            raise

    async def subscribe_to_events(self):
        for tag in self.plantuml_tags:
            subject = f"event.file.*.{tag}"
            queue = f"{QUEUE_GROUP}_{tag}"
            logger.debug(f"Subscribing to subject: {subject} on queue group {queue}")
            await self.jetstream.subscribe(
                subject,
                cb=self.handle_event,
                deliver_policy=DeliverPolicy.ALL,
                queue=queue,
                stream="EVENTS",
            )
            logger.info(f"Subscribed to subject: {subject}")

    async def handle_event(self, msg):
        logger.debug(f"Handling event {msg.subject}")
        try:
            if self.shutdown_event.is_set():
                return
            if msg.subject.split(".")[2] in ["created", "updated"]:
                data = json.loads(msg.data.decode())
                if data["file_extension"] in [".pu", ".puml", ".plantuml"]:
                    await self.process_plantuml_file(data)
                else:
                    logger.debug(
                        f"Skipping file with extension: {data['file_extension']}"
                    )
            else:
                logger.debug(f"Skipping event type: {msg.subject.split('.')[2]}")
        except json.JSONDecodeError:
            logger.error("Failed to decode message data")
        except KeyError:
            logger.error("Missing required fields in message data")

    async def process_plantuml_file(self, data):
        input_path = Path(data["absolute_path"])
        output_dir = input_path.parent.parent / "img"
        logger.debug(f"Creating output directory: {output_dir}")
        output_dir.mkdir(exist_ok=True, parents=True)
        logger.debug(f"Output directory exists: {output_dir.exists()}")

        output_path = output_dir / f"{input_path.stem}.png"
        await self.convert_plantuml(input_path, output_path)

    @staticmethod
    async def convert_plantuml(input_path: Path, output_path: Path):
        try:
            cmd = [
                "java",
                "-jar",
                "/app/plantuml.jar",
                "-tpng",
                "-Sdpi=600",
                "-o",
                str(output_path.parent),
                str(input_path),
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info(f"Converted {input_path} to {output_path}")
            else:
                logger.error(f"Error converting {input_path}: {stderr.decode()}")
        except Exception as e:
            logger.error(f"Error during conversion: {e}")

    async def run(self):
        await self.connect_nats()
        await self.subscribe_to_events()

        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            await self.shutdown_event.wait()
        finally:
            await self.cleanup()

    async def shutdown(self):
        logger.info("Initiating shutdown...")
        self.shutdown_event.set()

    async def cleanup(self):
        if self.nats_client:
            await self.nats_client.close()
        logger.info("PlantUML Converter shut down successfully")


if __name__ == "__main__":
    converter = PlantUMLConverter(CONFIG_PATH, NATS_URL)
    asyncio.run(converter.run())
