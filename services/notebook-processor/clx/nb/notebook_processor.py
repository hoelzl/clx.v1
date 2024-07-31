import asyncio
import json
import logging
import os
import signal
import sys
from hashlib import sha3_224
from pathlib import Path

import aiofiles
import nats
from jinja2 import Environment, PackageLoader, StrictUndefined
from jupytext import jupytext
from nats.aio.msg import Msg
from nats.errors import NoServersError
from nats.js.client import JetStreamContext
from nbformat import NotebookNode
from nbformat.validator import normalize

from .output_spec import OutputSpec, create_output_specs
from .utils.jupyter_utils import (
    Cell,
    get_cell_type,
    get_slide_tag,
    get_tags,
    is_answer_cell,
    is_code_cell,
    is_markdown_cell,
    warn_on_invalid_code_tags,
    warn_on_invalid_markdown_tags,
)
from .utils.prog_lang_utils import kernelspec_for, language_info

# Configuration
INPUT_DIR = os.environ.get("INPUT_DIR", "/input")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/output")
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.environ.get("STREAM_NAME", "EVENTS")
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "NOTEBOOK_PROCESSOR")
SUBJECT = os.environ.get("SUBJECT", "event.file.*.notebooks.>")
NOTEBOOK_PREFIX = os.environ.get("NOTEBOOK_PREFIX", "module")
NOTEBOOK_EXTENSION = os.environ.get("NOTEBOOK_EXTENSION", ".py")
JINJA_LINE_STATEMENT_PREFIX = os.environ.get("JINJA_LINE_STATEMENT_PREFIX", "# j2")
JINJA_TEMPLATES_FOLDER = os.environ.get("JINJA_TEMPLATES_FOLDER", "templates_python")
PROG_LANG = os.environ.get("PROG_LANG", "python")

# Logging setup
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - notebook-processor - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = asyncio.Event()


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


def get_jinja_loader():
    raise NotImplementedError("Jinja Template Loader not implemented.")


async def is_notebook_file(input_path):
    return (
        not input_path.name.startswith(NOTEBOOK_PREFIX)
        or input_path.suffix != NOTEBOOK_EXTENSION
    )


class CellIdGenerator:
    def __init__(self):
        self.unique_ids: set[str] = set()
        self.id_uniquifier: int = 1

    def set_cell_id(self, cell: Cell, index: int) -> None:
        cell_hash = sha3_224()
        cell_source: str = cell["source"]
        hash_text = cell_source
        while True:
            cell_hash.update(hash_text.encode("utf-8"))
            cell_id = cell_hash.hexdigest()[:16]
            if cell_id in self.unique_ids:
                hash_text = f"{index}:{cell_source}"
                index += 1
            else:
                self.unique_ids.add(cell_id)
                cell.id = cell_id
                break


async def process_file(absolute_path: str, relative_path: str):
    absolute_path = Path(absolute_path)
    if not absolute_path.exists():
        logger.error(f"Input file does not exist: {absolute_path}")
        return

    if await is_notebook_file(absolute_path):
        logger.info(f"Skipping non-notebook file: {absolute_path}")
        return

    logger.info(f"Processing notebook: {absolute_path}")

    try:
        async with aiofiles.open(absolute_path, "r", encoding="utf-8") as file:
            notebook_text = await file.read()

        for output_spec in create_output_specs():
            processor = NotebookProcessor(output_spec)
            await processor.process_notebook(relative_path, notebook_text)
    except Exception as e:
        logger.error(f"Error processing notebook {absolute_path}: {str(e)}")


class NotebookProcessor:
    def __init__(self, output_spec: OutputSpec):
        self.output_spec = output_spec
        self.id_generator = CellIdGenerator()

    @property
    def output_dir(self) -> Path:
        return Path(OUTPUT_DIR) / self.output_spec.path_fragment

    async def process_notebook(self, file_path, notebook_text):
        expanded_nb = self.load_and_expand_jinja_template(notebook_text)
        output_path = self.output_dir / file_path
        processed_nb = self.process_notebook_for_spec(expanded_nb)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as file:
            jupytext.write(processed_nb, file, fmt=NOTEBOOK_EXTENSION[1:])
        logger.info(f"Processed notebook written to: {output_path}")

    def load_and_expand_jinja_template(self, notebook_text: str) -> str:
        jinja_env = self._create_jinja_environment()
        nb_template = jinja_env.from_string(
            notebook_text,
            globals=self._create_jinja_globals(self.output_spec),
        )
        expanded_nb = nb_template.render()
        return expanded_nb

    @staticmethod
    def _create_jinja_environment():
        jinja_env = Environment(
            loader=(PackageLoader("clx.nb", JINJA_TEMPLATES_FOLDER)),
            autoescape=False,
            undefined=StrictUndefined,
            line_statement_prefix=JINJA_LINE_STATEMENT_PREFIX,
            keep_trailing_newline=True,
        )
        return jinja_env

    @staticmethod
    def _create_jinja_globals(output_spec):
        return {
            "is_notebook": output_spec.file_suffix == "ipynb",
            "is_html": output_spec.file_suffix == "html",
            "lang": output_spec.lang,
        }

    def process_notebook_for_spec(self, expanded_nb: str) -> NotebookNode:
        nb = jupytext.reads(expanded_nb, fmt=NOTEBOOK_EXTENSION[1:])
        processed_nb = self._process_notebook_node(nb)
        return processed_nb

    def _process_notebook_node(self, nb: NotebookNode) -> NotebookNode:
        new_cells = [
            self._process_cell(cell, index)
            for index, cell in enumerate(nb.get("cells", []))
            if self.output_spec.is_cell_included(cell)
        ]
        nb.cells = new_cells
        nb.metadata["language_info"] = language_info("python")
        nb.metadata["kernelspec"] = kernelspec_for("python")
        _, normalized_nb = normalize(nb)
        return normalized_nb

    def _process_cell(self, cell: Cell, index: int) -> Cell:
        self._generate_cell_metadata(cell, index)
        logging.debug(f"Processing cell {cell}")
        if is_code_cell(cell):
            return self._process_code_cell(cell)
        elif is_markdown_cell(cell):
            return self._process_markdown_cell(cell)
        else:
            logger.warning(f"Keeping unknown cell type {get_cell_type(cell)!r}.")
            return cell

    def _generate_cell_metadata(self, cell, index):
        self.id_generator.set_cell_id(cell, index)
        self._process_slide_tag(cell)

    @staticmethod
    def _process_slide_tag(cell):
        slide_tag = get_slide_tag(cell)
        if slide_tag:
            cell["metadata"]["slideshow"] = {"slide_type": slide_tag}

    def _process_code_cell(self, cell: Cell):
        if not self.output_spec.is_cell_contents_included(cell):
            cell["source"] = ""
            cell["outputs"] = []
        warn_on_invalid_code_tags(get_tags(cell))
        return cell

    def _process_markdown_cell(self, cell: Cell):
        tags = get_tags(cell)
        warn_on_invalid_markdown_tags(tags)
        self._process_markdown_cell_contents(cell)
        return cell

    def _process_markdown_cell_contents(self, cell: Cell):
        tags = get_tags(cell)
        if "notes" in tags:
            contents = cell["source"]
            cell["source"] = "<div style='background:yellow'>\n" + contents + "\n</div>"
        if is_answer_cell(cell):
            answer_text = "Answer" if self.output_spec.lang == "en" else "Antwort"
            prefix = f"*{answer_text}:* "
            if self.output_spec.is_cell_contents_included(cell):
                cell["source"] = prefix + cell["source"]
            else:
                cell["source"] = prefix


async def process_message(msg: Msg):
    try:
        # await msg.in_progress()
        data = json.loads(msg.data.decode())
        logging.debug(f"Received message: {data}")
        absolute_path = data.get("absolute_path")
        relative_path = data.get("relative_path")
        # await msg.ack()
        if absolute_path and relative_path:
            await process_file(absolute_path, relative_path)
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        # await msg.nak()


async def run_consumer(js: JetStreamContext):
    sub = None
    try:
        logging.debug(f"Trying to subscribe to {SUBJECT!r}")
        sub = await js.pull_subscribe("event.file.*.notebooks", stream=STREAM_NAME)
        logging.info(f"Subscribed to {SUBJECT!r} on stream {STREAM_NAME!r}")
        while not shutdown_flag.is_set():
            try:
                messages = await sub.fetch(1, timeout=5)
                for msg in messages:
                    await msg.ack()
                    logging.debug(f"Received message: {msg}")
                    await process_message(msg)
                pass
            except nats.errors.TimeoutError:
                logging.debug("No messages available")
                continue
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        if sub:
            logger.debug("Unsubscribing from subscription")
            await sub.unsubscribe()


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
