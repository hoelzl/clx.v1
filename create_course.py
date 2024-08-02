# %%
import os
import re
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from pprint import pprint

# %%
ROOT_DIR = r"C:\Users\tc\Programming\Python\Courses\Own\PythonCoursesNew"
SPEC_FILE = Path(os.getenv("SPEC_FILE", ROOT_DIR + r"\course-specs\test-course.xml"))
STAGING_DIR = Path(os.getenv("STAGING_DIR", ROOT_DIR + r"\staging"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", ROOT_DIR + r"\output-test"))


# %%
@dataclass
class Course:
    name: dict[str, str]
    prog_lang: str
    description: dict[str, str]
    certificate: dict[str, str]
    sections: list["Section"]
    github_repo: dict[str, str] = field(default_factory=dict)


# %%
@dataclass
class Section:
    name: dict[str, str]
    slides: list[str]


# %%
def parse_course(xml_file: Path) -> Course:
    tree = ET.parse(xml_file)
    root = tree.getroot()

    def parse_multilang(tag) -> dict[str, str]:
        return {element.tag: element.text for element in root.find(tag)}

    def parse_sections() -> list[Section]:
        sections = []
        for i, section_elem in enumerate(root.findall("sections/section"), start=1):
            name = parse_multilang(f"sections/section[{i}]/name")
            slides = [
                slide.text for slide in section_elem.find("slides").findall("slide")
            ]
            sections.append(Section(name=name, slides=slides))
        return sections

    return Course(
        name=parse_multilang("name"),
        prog_lang=root.find("prog-lang").text,
        description=parse_multilang("description"),
        certificate=parse_multilang("certificate"),
        github_repo=parse_multilang("github"),
        sections=parse_sections(),
    )


# %%
_course = parse_course(SPEC_FILE)
pprint(_course)


# %%
def build_slide_mapping(
    staging_dir: Path,
) -> dict[str, dict[tuple[str, str, str], Path]]:
    slide_mapping = {}
    for lang_dir in staging_dir.iterdir():
        if not lang_dir.is_dir():
            continue
        lang = lang_dir.name
        for format_dir in lang_dir.iterdir():
            if not format_dir.is_dir():
                continue
            format = format_dir.name
            for mode_dir in format_dir.iterdir():
                if not mode_dir.is_dir():
                    continue
                mode = mode_dir.name
                for module_dir in mode_dir.iterdir():
                    if not module_dir.is_dir():
                        continue
                    for topic_dir in module_dir.iterdir():
                        if not topic_dir.is_dir():
                            continue
                        slide_name = "_".join(topic_dir.name.split("_")[2:])
                        selector = (lang, format, mode)
                        slide_mapping.setdefault(slide_name, {})[selector] = topic_dir
    return slide_mapping


# %%
_slide_mapping = build_slide_mapping(STAGING_DIR)
pprint(_slide_mapping)


# %%
SLIDES_REGEX = re.compile(r"^(\d+) ")


# %%
def is_slide_file(file: Path) -> bool:
    return bool(SLIDES_REGEX.match(file.name))


# %%
print(is_slide_file(Path("01 Foo.py")))
print(not is_slide_file(Path("foo.md")))


# %%
def replace_slide_number(file: Path, new_number: int) -> str:
    return f"{new_number:02d} {file.name.split(' ', 1)[1]}"


# %%
print(replace_slide_number(Path("01 Foo.py"), 2))


# %%
def copy_and_rename_files(
    course: Course,
    slide_mapping: dict[str, dict[tuple[str, str, str], Path]],
    output_dir: Path,
):
    name_mappings = {
        "code": course.prog_lang,
        "html": "html",
        "notebook": "notebooks",
        "code_along": "code-along",
        "completed": "completed",
    }
    course_names = course.name
    slide_names = {"de": "folien", "en": "slides"}

    for lang in ["de", "en"]:
        lang_output_dir = (
            output_dir / lang / "public" / course_names[lang] / slide_names[lang]
        )
        lang_output_dir.mkdir(parents=True, exist_ok=True)

        for format_ in ["html", "notebook", "code"]:
            format_output_dir = lang_output_dir / name_mappings[format_]

            for mode in ["code_along", "completed"]:
                lang_mode_output_dir = format_output_dir / name_mappings[mode]
                lang_mode_output_dir.mkdir(parents=True, exist_ok=True)

                for section in course.sections:
                    section_dir = lang_mode_output_dir / section.name[lang]
                    section_dir.mkdir(parents=True, exist_ok=True)
                    slide_counter = 1

                    for slide_name in section.slides:
                        if slide_name not in slide_mapping:
                            print(
                                f"Warning: Slide {slide_name} not found "
                                "in staging directory."
                            )
                            continue

                        slide_dirs = slide_mapping[slide_name]
                        slide_dir = slide_dirs[(lang, format_, mode)]
                        source_files = list(slide_dir.glob("*"))
                        for source_file in source_files:
                            if source_file.is_dir():
                                print(
                                    f"Copying directory {source_file} to {section_dir}"
                                )
                                target_dir = section_dir / source_file.name
                                target_dir.mkdir(exist_ok=True)
                                shutil.copytree(
                                    source_file, target_dir, dirs_exist_ok=True
                                )
                            elif source_file.is_file():
                                if is_slide_file(source_file):
                                    new_slide_name = replace_slide_number(
                                        source_file, slide_counter
                                    )
                                    print(
                                        f"Copying slide {source_file} to {section_dir} "
                                        f"as {new_slide_name}"
                                    )
                                    shutil.copy(
                                        source_file,
                                        section_dir / new_slide_name,
                                    )
                                    slide_counter += 1
                                else:
                                    print(
                                        f"Copying file {source_file} to {section_dir}"
                                    )
                                    shutil.copy(source_file, section_dir)


# %%
copy_and_rename_files(_course, _slide_mapping, OUTPUT_DIR)


# %%
def main():
    course = parse_course(SPEC_FILE)
    slide_mapping = build_slide_mapping(STAGING_DIR)
    copy_and_rename_files(course, slide_mapping, OUTPUT_DIR)


# %%
if __name__ == "__main__":
    main()
