import asyncio
import json
import logging
import os
from pathlib import Path

import nats
import yaml
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy

# Configuration
QUEUE_GROUP = os.environ.get("QUEUE_GROUP", "DRAWIO_CONVERTER")

# Set up logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - drawio-converter - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DrawioConverter:
    def __init__(self, config_path: str, nats_url: str):
        self.config_path = config_path
        self.nats_url = nats_url
        self.drawio_tags = {}
        self.output_formats = []
        self.nats_client: nats.NATS | None = None
        self.jetstream: JetStreamContext | None = None
        self.load_config()

    def load_config(self):
        try:
            with open(self.config_path, "r") as config_file:
                config = yaml.safe_load(config_file)
                self.drawio_tags = config.get("drawio_tags", {})
                self.output_formats = config.get("output_formats", [])
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
        for tag in self.drawio_tags:
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
            logger.info(f"Subscribed to subject: {subject} on queue group {queue}")

    async def handle_event(self, msg):
        try:
            if msg.subject.split(".")[2] in ["created", "updated"]:
                data = json.loads(msg.data.decode())
                if data["file_extension"] == ".drawio":
                    await self.process_drawio_file(data)
                else:
                    logger.debug(
                        f"Skipping file with extension: {data['file_extension']}"
                    )
            else:
                logger.debug(f"Ignoring message with subject: {msg.subject}")
        except json.JSONDecodeError:
            logger.error("Failed to decode message data")
        except KeyError:
            logger.error("Missing required fields in message data")

    async def process_drawio_file(self, data):
        input_path = Path(data["absolute_path"])
        output_dir = input_path.parent.parent / "img"
        logger.debug(f"Creating output directory: {output_dir}")
        output_dir.mkdir(exist_ok=True, parents=True)
        logger.debug(f"Output directory exists: {output_dir.exists()}")

        for output_format in self.output_formats:
            output_path = output_dir / f"{input_path.stem}.{output_format}"
            await self.convert_drawio(input_path, output_path, output_format)

    @staticmethod
    async def convert_drawio(input_path: Path, output_path: Path, output_format: str):
        try:
            # Base command
            cmd = [
                "drawio",
                "--no-sandbox",
                "--export",
                str(input_path),
                "--format",
                output_format,
                "--output",
                str(output_path),
                "--border",
                "20",
                # Add a 10px border
            ]

            # Format-specific options
            if output_format == "png":
                cmd.extend(["--scale", "3"])  # Increase resolution (roughly 300 DPI)
            elif output_format == "svg":
                cmd.append("--embed-svg-images")  # Embed fonts in SVG

            env = os.environ.copy()
            env["DISPLAY"] = ":99"
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info(f"Converted {input_path} to {output_path}")

                # If the output is SVG, optimize it and embed the font
                if output_format.lower() == "svg":
                    await DrawioConverter.optimize_svg(output_path)
            else:
                logger.error(f"Error converting {input_path}: {stderr.decode()}")
        except Exception as e:
            logger.error(f"Error during conversion: {e}")

    @staticmethod
    async def optimize_svg(output_path):
        optimize_cmd = [
            "svgo",
            "-i",
            str(output_path),
            "-o",
            str(output_path),
        ]
        optimize_process = await asyncio.create_subprocess_exec(
            *optimize_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        optimize_stdout, optimize_stderr = await optimize_process.communicate()
        if optimize_process.returncode == 0:
            logger.info(f"Optimized SVG: {output_path}")
        else:
            logger.error(
                f"Error optimizing SVG {output_path}: {optimize_stderr.decode()}"
            )

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

    converter = DrawioConverter(config_path, nats_url)
    asyncio.run(converter.run())
