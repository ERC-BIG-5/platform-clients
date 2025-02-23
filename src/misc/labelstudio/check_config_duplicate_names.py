"""
deprecated. use MyLaberlstudioHelper.config_helper

"""

import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

def find_all_names(root):

    unique_names = {}

    def find_name(element, current_path):
        # Print current element's path
        path = f"{current_path}/{element.tag}"
        # print(path, element.get("name"))
        if _name := element.get("name"):
            unique_names.setdefault(_name, []).append(path)

        # Recurse through all children
        for child in element:
            find_name(child, path)

    # Start from root
    find_name(root, "")

    return unique_names

def find_tag_name_refs(root):
    refs = {}

    def find_name(element, current_path):
        # Print current element's path
        path = f"{current_path}/{element.tag}"
        # print(path, element.get("name"))
        if _name := element.get("whenTagName"):
            refs.setdefault(_name, []).append(path)

        # Recurse through all children
        for child in element:
            find_name(child, path)

    # Start from root
    find_name(root, "")

    return refs

def find_duplicates(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    unique_names = {}

    def find_name(element, current_path):
        # Print current element's path
        path = f"{current_path}/{element.tag}"
        # print(path, element.get("name"))
        if _name := element.get("name"):
            unique_names.setdefault(_name, []).append(path)

        # Recurse through all children
        for child in element:
            find_name(child, path)

    # Start from root
    find_name(root, "")

    return {
        k: v for k, v in unique_names.items() if len(v) > 1
    }

def check_references(root):
    names = list(find_all_names(root).keys())
    # print(names)
    refs = list(find_tag_name_refs(root).keys())
    for ref in refs:
        if ref not in names:
            print(ref)


def complete_config(xml_file: Path):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    img1 = root.findall(".//{*}Image")[0]
    print(img1)

if __name__ == "__main__":
    # dupl = find_duplicates(Path("/home/rsoleyma/projects/platforms-clients/data/labelstudio_configs/test_session_1_2025.xml"))
    # print(dupl)
    #complete_config((Path("/home/rsoleyma/projects/platforms-clients/data/labelstudio_configs/final1_t/config.xml")))

    # tree = ET.parse(Path("/home/rsoleyma/projects/platforms-clients/data/labelstudio_configs/test_session_1_2025.xml"))
    tree = ET.parse(Path("/home/rsoleyma/projects/platforms-clients/src/misc/labelstudio/output.xml"))
    root = tree.getroot()

    check_references(root)
