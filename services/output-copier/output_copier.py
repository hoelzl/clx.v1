import asyncio
import json
import logging
import os
import re
import shutil
import signal
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import nats
from nats.aio.msg import Msg
from nats.errors import NoServersError
from nats.js.client import JetStreamContext

ROOT_DIR = Path(
    os.getenv(
        "ROOT_DIR", r"C:\Users\tc\Programming\Python\Courses\Own\PythonCoursesNew"
    )
)
SPEC_DIR = Path(os.getenv("SPEC_FILE", ROOT_DIR / "course-specs/"))
STAGING_DIR = Path(os.getenv("STAGING_DIR", ROOT_DIR / "staging"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", ROOT_DIR / "output-test"))
REGEN_DELAY = int(os.getenv("REGEN_DELAY", 5))
REGEN_COOLDOWN = int(os.getenv("REGEN_COOLDOWN", 10))
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
EVENT_STREAM_NAME = os.environ.get("STREAM_NAME", "EVENTS")
COMMAND_STREAM_NAME = os.environ.get("COMMAND_STREAM_NAME", "COMMANDS")
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "OUTPUT_GENERATOR")
FILE_SUBJECT = os.environ.get("SUBJECT", "event.file.*.output")
COMMAND_SUBJECT = os.environ.get("COMMAND_SUBJECT", "command.output.>")
FILE_QUEUE_GROUP = os.environ.get("QUEUE_GROUP", "OUTPUT_GENERATOR_FILE_QUEUE")
COMMAND_QUEUE_GROUP = os.environ.get(
    "COMMAND_QUEUE_GROUP", "OUTPUT_GENERATOR_COMMAND_QUEUE"
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

SLIDES_REGEX = re.compile(r"^(\d+) ")
NAME_MAPPINGS = {
    "code": "Python",
    "html": "Html",
    "notebook": "Notebooks",
    "code_along": "Code-Along",
    "completed": "Completed",
    "speaker": "Speaker",
}
SLIDE_NAME_MAPPINGS = {"de": "folien", "en": "slides"}


# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - output-generator - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = asyncio.Event()

# Courses and their last generation timestamps
courses: dict[str, "Course"] = {}

# Nats client and jetstream
nats_client: nats.NATS | None = None
jetstream: JetStreamContext | None = None


@dataclass
class Course:
    name: dict[str, str]
    prog_lang: str
    description: dict[str, str]
    certificate: dict[str, str]
    sections: list["Section"]
    is_generation_in_progress: bool = False
    last_generation_time: float | None = None
    github_repo: dict[str, str] = field(default_factory=dict)

    @property
    def topics(self) -> list[str]:
        return [topic for section in self.sections for topic in section.topics]

    def output_dir(self, lang, public_or_speaker) -> Path:
        return (
            OUTPUT_DIR
            / lang
            / public_or_speaker
            / self.name[lang]
            / SLIDE_NAME_MAPPINGS[lang]
        )


@dataclass
class Section:
    name: dict[str, str]
    topics: list[str]


def parse_multilang(root, tag) -> dict[str, str]:
    return {element.tag: element.text for element in root.find(tag)}


def parse_sections(root) -> list[Section]:
    sections = []
    for i, section_elem in enumerate(root.findall("sections/section"), start=1):
        name = parse_multilang(root, f"sections/section[{i}]/name")
        topics = [slide.text for slide in section_elem.find("topics").findall("topic")]
        sections.append(Section(name=name, topics=topics))
    return sections


def parse_course(xml_file: Path) -> Course:
    tree = ET.parse(xml_file)
    root = tree.getroot()

    return Course(
        name=parse_multilang(root, "name"),
        prog_lang=root.find("prog-lang").text,
        description=parse_multilang(root, "description"),
        certificate=parse_multilang(root, "certificate"),
        github_repo=parse_multilang(root, "github"),
        sections=parse_sections(root),
    )


# noinspection PyTypeChecker
def build_topic_mapping(
    staging_dir: Path,
) -> dict[str, dict[tuple[str, str, str], Path]]:
    topic_mapping = {}
    logging.debug(f"Building topic mapping from {staging_dir}")
    for lang_dir in filter(Path.is_dir, staging_dir.iterdir()):
        lang = lang_dir.name
        for format_dir in filter(Path.is_dir, lang_dir.iterdir()):
            output_format = format_dir.name
            for mode_dir in filter(Path.is_dir, format_dir.iterdir()):
                mode = mode_dir.name
                for module_dir in filter(Path.is_dir, mode_dir.iterdir()):
                    for topic_dir in filter(Path.is_dir, module_dir.iterdir()):
                        topic_name = simplify_topic_name(topic_dir.name)
                        selector = (lang, output_format, mode)
                        topic_mapping.setdefault(topic_name, {})[selector] = topic_dir
    return topic_mapping


def simplify_topic_name(topic_dir: str) -> str:
    return "_".join(topic_dir.split("_")[2:])


def is_slide_file(file: Path) -> bool:
    return bool(SLIDES_REGEX.match(file.name))


def replace_slide_number(file: Path, new_number: int) -> str:
    return f"{new_number:02d} {file.name.split(' ', 1)[1]}"


def copy_files(
    course, source_dir: Path, target_dir: Path, slide_counter: int = 1
) -> int:
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_file in source_dir.iterdir():
        if source_file.is_dir():
            logger.debug(f"Copying directory {source_file} to {target_dir}")
            target_subdir = target_dir / source_file.name
            target_subdir.mkdir(exist_ok=True)
            shutil.copytree(source_file, target_subdir, dirs_exist_ok=True)
        elif source_file.is_file():
            if is_slide_file(source_file):
                new_slide_name = replace_slide_number(source_file, slide_counter)
                logger.debug(
                    f"{course.name['en']}: Copying slide {source_file} to "
                    f"{target_dir} as {new_slide_name}"
                )
                shutil.copy(source_file, target_dir / new_slide_name)
                slide_counter += 1
            else:
                logger.debug(f"Copying file {source_file} to {target_dir}")
                shutil.copy(source_file, target_dir)
    return slide_counter


def copy_and_rename_files(
    course: Course, topic_mapping: dict[str, dict[tuple[str, str, str], Path]]
):
    copy_public_files(course, topic_mapping)
    copy_speaker_files(course, topic_mapping)


def copy_public_files(course, topic_mapping):
    for lang in ["de", "en"]:
        lang_output_dir = course.output_dir(lang, "public")

        for format_ in ["html", "notebook", "code"]:
            format_output_dir = lang_output_dir / NAME_MAPPINGS[format_]

            for mode in ["code_along", "completed"]:
                lang_mode_output_dir = format_output_dir / NAME_MAPPINGS[mode]

                for section in course.sections:
                    section_dir = lang_mode_output_dir / section.name[lang]
                    slide_counter = 1

                    for topic_name in section.topics:
                        if topic_name not in topic_mapping:
                            logger.warning(
                                f"{course.name[lang]}: Warning: Topic {topic_name} "
                                f"not found in topic mapping."
                            )
                            continue

                        topic_dirs = topic_mapping[topic_name]
                        topic_dir = topic_dirs[(lang, format_, mode)]
                        slide_counter = copy_files(
                            course, topic_dir, section_dir, slide_counter
                        )


def copy_speaker_files(course, topic_mapping):
    for lang in ["de", "en"]:
        speaker_output_dir = course.output_dir(lang, "speaker")
        logger.debug(f"{course.name[lang]}: Speaker output dir: {speaker_output_dir}")
        for format_ in ["html", "notebook"]:
            format_output_dir = speaker_output_dir / NAME_MAPPINGS[format_]

            for section in course.sections:
                section_dir = format_output_dir / section.name[lang]
                slide_counter = 1

                for topic_name in section.topics:
                    if topic_name not in topic_mapping:
                        logger.warning(
                            f"Warning: Topic {topic_name} not found "
                            f"in staging directory."
                        )
                        continue

                    topic_dirs = topic_mapping[topic_name]
                    topic_dir = topic_dirs[(lang, format_, "speaker")]
                    slide_counter = copy_files(
                        course, topic_dir, section_dir, slide_counter
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


async def process_file_event(msg: Msg):
    logger.debug(f"Received file-change event: {msg.subject}")
    try:
        data = json.loads(msg.data.decode())
        logger.debug(f"Message data: {data}")
        absolute_path = data.get("absolute_path")
        relative_path = data.get("relative_path")
        topic = simplify_topic_name(data.get("containing_dir", "topic_999_"))
        if absolute_path and relative_path:
            logger.debug(f"Processing message for file: {absolute_path}")
            action = get_event_action(msg)
            if action in ["created", "modified"]:
                await process_file_change(relative_path, topic)
            elif action == "deleted":
                await process_file_deletion(relative_path, topic)
            else:
                logger.debug(f"Skipping event action: {action}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")


async def process_command(msg: Msg):
    logger.debug(f"Received command: {msg.subject}")
    try:
        command = msg.subject.split(".")[2]
        if command == "clear-timers":
            logger.info("Clearing generation timers for all courses")
            for course in courses.values():
                course.last_generation_time = None
        elif command == "copy-staged-files":
            course_name = get_name_from_message(msg)
            if course_name:
                await copy_staged_files_for(course_name)
            else:
                logger.error("Invalid command payload: missing 'name' field")
        elif command == "delete":
            course_name = get_name_from_message(msg)
            if course_name:
                await delete_course(course_name)
            else:
                logger.error("Invalid command payload: missing 'name' field")
        elif command == "create-course":
            course_name = get_name_from_message(msg)
            if course_name:
                await create_course(course_name)
            else:
                logger.error("Invalid command payload: missing 'name' field")
        else:
            logger.warning(f"Unknown command: {command}")
    except Exception as e:
        logger.error(f"Error processing command: {str(e)}")


def get_name_from_message(msg: Msg) -> str:
    try:
        data = json.loads(msg.data.decode())
        return data.get("name")
    except json.JSONDecodeError:
        return msg.data.decode()


async def delete_course(course_name: str):
    logger.info(f"Deleting course: {course_name}")
    try:
        course = courses.get(course_name)
        if course:
            for lang in ["de", "en"]:
                public_course_dir = course.output_dir(lang, "public")
                private_course_dir = course.output_dir(lang, "speaker")

                delete_course_dir(public_course_dir)
                delete_course_dir(private_course_dir)
        else:
            logger.warning(f"Course {course_name} not found in loaded courses")
    except Exception as e:
        logger.error(f"Error deleting course {course_name}: {str(e)}")


def delete_course_dir(course_dir: Path):
    preserve_git_dir(course_dir)
    try:
        logger.debug(f"Deleting directory: {course_dir}")
        shutil.rmtree(course_dir, ignore_errors=True)
        course_dir.mkdir(exist_ok=True)
    finally:
        restore_git_dir(course_dir)


def preserve_git_dir(course_dir: Path):
    """
    Move the .git directory out of the course directory temporarily,
    to be restored after deleting the course contents.
    """
    git_dir = course_dir / ".git"
    if git_dir.exists():
        target = saved_git_dir(course_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Moving git directory to {git_dir} to " f"{target}")
        shutil.move(str(git_dir.resolve()), str(target.resolve()))


def restore_git_dir(course_dir: Path):
    saved_dir = saved_git_dir(course_dir)
    if saved_dir.exists():
        logger.debug(f"Restoring saved git directory {saved_dir} to {course_dir}")
        target_dir = (course_dir / ".git").resolve()
        if target_dir.exists():
            logger.warning(
                f"Git directory already exists: {target_dir}. " "Not restoring."
            )
        logger.debug(f"Moving git directory from {saved_dir} to {course_dir}")
        shutil.move(str(saved_dir.resolve()), str(target_dir))


def saved_git_dir(course_dir) -> Path:
    return Path("/tmp/saved-git") / course_dir.name


async def create_course(course_name: str):
    logger.info(f"Force creating course: {course_name}")
    course = courses.get(course_name)
    if course:
        try:
            logger.debug("Found course. Deleting and regenerating...")
            await delete_course(course_name)
            for topic in course.topics:
                await nats_client.publish(
                    "command.watcher.rescan",
                    json.dumps({"topic": f"{topic}"}).encode("utf-8"),
                )
            logger.debug(f"Waiting {REGEN_DELAY} seconds before regenerating course")
            await asyncio.sleep(REGEN_DELAY)
            await copy_staged_files_for(course_name)
        except Exception as e:
            logger.error(f"Error force creating course {course_name}: {str(e)}")
    else:
        logger.error(f"Course {course_name} not found in loaded courses")
        logger.error(f"Available courses: {list(courses.keys())}")


async def process_file_change(relative_path: str, topic: str):
    logger.debug(f"Processing file change: {relative_path}")
    logger.debug(f"Available courses: {list(courses.keys())}")
    for course_name, course in courses.items():
        logger.debug(f"Checking {course_name} for regeneration (file changed)")
        if should_regenerate_course(course, relative_path, topic):
            logger.debug(f"Regenerating course {course_name}")
            await regenerate_course(course_name, course)


async def process_file_deletion(relative_path: str, topic: str):
    logger.debug(f"Processing file deletion: {relative_path}")
    for course_name, course in courses.items():
        logger.debug(f"Checking {course_name} for regeneration (file deleted)")
        if should_regenerate_course(course, relative_path, topic):
            logger.debug(f"Regenerating course {course_name}")
            await regenerate_course(course_name, course)


def should_regenerate_course(course, _relative_path: str, topic: str) -> bool:
    if topic not in course.topics:
        logger.debug(f"Skipping course {course.name['en']}: topic not found")
        return False
    if course.is_generation_in_progress:
        logger.debug(
            f"Skipping course {course.name['en']}: generation already in progress"
        )
        return False
    last_generation_time = course.last_generation_time or 0
    if time.time() - last_generation_time < REGEN_COOLDOWN:
        logger.debug(
            f"Skipping course {course.name['en']}: "
            f"last generation was less than {REGEN_COOLDOWN} seconds ago"
        )
        return False
    return True


async def regenerate_course(course_name: str, course: Course):
    logger.info(f"Regenerating course: {course_name}")
    course.is_generation_in_progress = True
    try:
        # Delaying generation since not all course files are likely to be updated
        logger.debug(f"Waiting {REGEN_DELAY} seconds before regenerating course")
        await asyncio.sleep(REGEN_DELAY)
        logger.debug(f"Regen delay over. Generating course {course_name}")
        topic_mapping = build_topic_mapping(STAGING_DIR)
        copy_and_rename_files(course, topic_mapping)
        course.last_generation_time = time.time()
        logger.info(f"Course {course_name} regenerated successfully")
    except Exception as e:
        logger.error(f"Error regenerating course {course_name}: {str(e)}")
    finally:
        course.is_generation_in_progress = False


async def copy_staged_files_for(course_name: str):
    logger.info(f"Creating new course: {course_name}")
    try:
        course = parse_course(Path(f"course-specs/{course_name}.xml"))
        courses[course_name] = course
        await regenerate_course(course_name, course)
    except Exception as e:
        logger.error(f"Error creating course {course_name}: {str(e)}")


async def run_consumers(nc: nats.NATS, js: JetStreamContext):
    file_sub = await nc.subscribe(
        FILE_SUBJECT,
        queue=FILE_QUEUE_GROUP,
        cb=process_file_event,
    )
    command_sub = await js.pull_subscribe(
        COMMAND_SUBJECT,
        stream=COMMAND_STREAM_NAME,
    )
    logger.info(f"Subscribed to {FILE_SUBJECT!r} and {COMMAND_SUBJECT!r}")
    try:
        while not shutdown_flag.is_set():
            try:
                command_msgs = await command_sub.fetch(1)
                logger.debug(f"Received {len(command_msgs)} command message(s)")
                for command_msg in command_msgs:
                    await command_msg.ack()
                    command = command_msg.subject.split(".")[2]
                    logger.info(f"Received command: {command} {command_msg.data}")
                    await process_command(command_msg)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("Output copier task cancelled")
                break
            except Exception as e:
                logger.error(f"Error processing command: {e}")
                raise
    finally:
        logger.info("Shutting down consumers")
        if file_sub:
            await file_sub.drain()
        # if command_sub:
        #     await command_sub.drain()


async def main():
    global nats_client
    global jetstream
    nats_client, jetstream = await connect_jetstream(NATS_URL)

    # Load initial courses from spec files
    for spec_file in Path("/course-specs").glob("*.xml"):
        logging.debug(f"Loading course spec: {spec_file}")
        try:
            course_name = spec_file.stem
            course = parse_course(spec_file)
            courses[course_name] = course
            logging.info(f"Loaded course: {course_name}")
        except Exception as e:
            logger.error(f"Error loading course spec {spec_file}: {e}")

    consumer_task = asyncio.create_task(run_consumers(nats_client, jetstream))

    await shutdown_flag.wait()
    await consumer_task
    await nats_client.close()


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


if __name__ == "__main__":
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: signal_handler())

    asyncio.run(main())
