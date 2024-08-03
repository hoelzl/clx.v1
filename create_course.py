import os
import re
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(
    os.getenv(
        "ROOT_DIR", r"C:\Users\tc\Programming\Python\Courses\Own\PythonCoursesNew"
    )
)
SPEC_FILE = Path(os.getenv("SPEC_FILE", ROOT_DIR / "course-specs/test-course.xml"))
STAGING_DIR = Path(os.getenv("STAGING_DIR", ROOT_DIR / "staging"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", ROOT_DIR / "output-test"))

SLIDES_REGEX = re.compile(r"^(\d+) ")
NAME_MAPPINGS = {
    "code": "python",
    "html": "html",
    "notebook": "notebooks",
    "code_along": "code-along",
    "completed": "completed",
}
COURSE_NAME_MAPPINGS = {"de": "mein-kurs", "en": "my-course"}
SLIDE_NAME_MAPPINGS = {"de": "folien", "en": "slides"}


@dataclass
class Course:
    name: dict[str, str]
    prog_lang: str
    description: dict[str, str]
    certificate: dict[str, str]
    sections: list["Section"]
    github_repo: dict[str, str] = field(default_factory=dict)


@dataclass
class Section:
    name: dict[str, str]
    slides: list[str]


def parse_multilang(root, tag) -> dict[str, str]:
    return {element.tag: element.text for element in root.find(tag)}


def parse_sections(root) -> list[Section]:
    sections = []
    for i, section_elem in enumerate(root.findall("sections/section"), start=1):
        name = parse_multilang(root, f"sections/section[{i}]/name")
        slides = [slide.text for slide in section_elem.find("slides").findall("slide")]
        sections.append(Section(name=name, slides=slides))
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
def build_slide_mapping(
    staging_dir: Path,
) -> dict[str, dict[tuple[str, str, str], Path]]:
    slide_mapping = {}
    for lang_dir in filter(Path.is_dir, staging_dir.iterdir()):
        lang = lang_dir.name
        for format_dir in filter(Path.is_dir, lang_dir.iterdir()):
            output_format = format_dir.name
            for mode_dir in filter(Path.is_dir, format_dir.iterdir()):
                mode = mode_dir.name
                for module_dir in filter(Path.is_dir, mode_dir.iterdir()):
                    for topic_dir in filter(Path.is_dir, module_dir.iterdir()):
                        slide_name = "_".join(topic_dir.name.split("_")[2:])
                        selector = (lang, output_format, mode)
                        slide_mapping.setdefault(slide_name, {})[selector] = topic_dir
    return slide_mapping


def is_slide_file(file: Path) -> bool:
    return bool(SLIDES_REGEX.match(file.name))


def replace_slide_number(file: Path, new_number: int) -> str:
    return f"{new_number:02d} {file.name.split(' ', 1)[1]}"


def copy_files(
    source_dir: Path,
    target_dir: Path,
    slide_counter: int = 1,
) -> int:
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_file in source_dir.iterdir():
        if source_file.is_dir():
            print(f"Copying directory {source_file} to {target_dir}")
            target_subdir = target_dir / source_file.name
            target_subdir.mkdir(exist_ok=True)
            shutil.copytree(source_file, target_subdir, dirs_exist_ok=True)
        elif source_file.is_file():
            if is_slide_file(source_file):
                new_slide_name = replace_slide_number(source_file, slide_counter)
                print(
                    f"Copying slide {source_file} to {target_dir} as {new_slide_name}"
                )
                shutil.copy(source_file, target_dir / new_slide_name)
                slide_counter += 1
            else:
                print(f"Copying file {source_file} to {target_dir}")
                shutil.copy(source_file, target_dir)
    return slide_counter


def copy_and_rename_files(
    course: Course,
    slide_mapping: dict[str, dict[tuple[str, str, str], Path]],
    output_dir: Path,
):
    copy_public_files(course, output_dir, slide_mapping)
    copy_speaker_files(course, output_dir, slide_mapping)


def copy_public_files(course, output_dir, slide_mapping):
    for lang in ["de", "en"]:
        lang_output_dir = (
            output_dir
            / lang
            / "public"
            / COURSE_NAME_MAPPINGS[lang]
            / SLIDE_NAME_MAPPINGS[lang]
        )

        for format_ in ["html", "notebook", "code"]:
            format_output_dir = lang_output_dir / NAME_MAPPINGS[format_]

            for mode in ["code_along", "completed"]:
                lang_mode_output_dir = format_output_dir / NAME_MAPPINGS[mode]

                for section in course.sections:
                    section_dir = lang_mode_output_dir / section.name[lang]
                    slide_counter = 1

                    for slide_name in section.slides:
                        if slide_name not in slide_mapping:
                            print(
                                f"Warning: Slide {slide_name} not found in "
                                "staging directory."
                            )
                            continue

                        slide_dirs = slide_mapping[slide_name]
                        slide_dir = slide_dirs[(lang, format_, mode)]
                        slide_counter = copy_files(
                            slide_dir, section_dir, slide_counter
                        )


def copy_speaker_files(course, output_dir, slide_mapping):
    for lang in ["de", "en"]:
        speaker_output_dir = (
            output_dir
            / lang
            / "speaker"
            / COURSE_NAME_MAPPINGS[lang]
            / SLIDE_NAME_MAPPINGS[lang]
        )

        for format_ in ["html", "notebook"]:
            format_output_dir = speaker_output_dir / NAME_MAPPINGS[format_]

            for section in course.sections:
                section_dir = format_output_dir / section.name[lang]
                slide_counter = 1

                for slide_name in section.slides:
                    if slide_name not in slide_mapping:
                        print(
                            f"Warning: Slide {slide_name} not found in "
                            "staging directory."
                        )
                        continue

                    slide_dirs = slide_mapping[slide_name]
                    slide_dir = slide_dirs[(lang, format_, "speaker")]
                    slide_counter = copy_files(slide_dir, section_dir, slide_counter)


def main():
    course = parse_course(SPEC_FILE)
    slide_mapping = build_slide_mapping(STAGING_DIR)
    copy_and_rename_files(course, slide_mapping, OUTPUT_DIR)


if __name__ == "__main__":
    main()
