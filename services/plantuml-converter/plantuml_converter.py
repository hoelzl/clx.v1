import asyncio
import json
import logging
import os
from pathlib import Path

import nats
import yaml
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy

# Set up logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
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
            await self.jetstream.subscribe(
                subject, cb=self.handle_event, deliver_policy=DeliverPolicy.ALL
            )
            logger.info(f"Subscribed to subject: {subject}")

    async def handle_event(self, msg):
        try:
            data = json.loads(msg.data.decode())
            if data["file_extension"] in [".pu", ".puml", ".plantuml"]:
                await self.process_plantuml_file(data)
            else:
                logger.debug(f"Skipping file with extension: {data['file_extension']}")
        except json.JSONDecodeError:
            logger.error("Failed to decode message data")
        except KeyError:
            logger.error("Missing required fields in message data")

    async def process_plantuml_file(self, data):
        input_path = Path(data["full_path"])
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
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt, shutting down...")
        finally:
            if self.nats_client:
                await self.nats_client.close()


if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH", "config.yaml")
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")

    converter = PlantUMLConverter(config_path, nats_url)
    asyncio.run(converter.run())
