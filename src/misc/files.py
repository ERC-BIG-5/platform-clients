import json
from csv import DictReader
from pathlib import Path
from typing import Optional, Sequence

import openpyxl
import yaml
from yaml import Loader

from src.const import MAIN_DIRS, PROJECT_PATH



def relative_to_project_path(path: Path, parenthesis: bool = True) -> str:
    p = str(path.relative_to(PROJECT_PATH))
    if parenthesis:
        return f"'{p}'"
    else:
        return p


def get_abs_path(path: Path, base_dir: Optional[Path] = None) -> Path:
    if path.is_absolute():
        return path
    else:
        if base_dir is None:
            return PROJECT_PATH / path
        else:
            return base_dir / path


def read_data(path: Path, config: Optional[dict] = None, encoding: str = "utf-8",
              required_suffix: Optional[Sequence[str]] = None):
    """
    Read data from file. Formats supported: json, csv, excel
    - json is read straight into a dict
    - csv is read into a DictReader object
    - excel is read into a dict of sheet names and lists of rows

    :return:
    """
    if required_suffix and path.suffix not in required_suffix:
        raise ValueError(f"'{path}' is not a supported file type")
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding=encoding))
    elif path.suffix == ".csv":
        if not config:
            config = {}
        return list(DictReader(path.open(encoding=encoding), **config))
    # excel
    elif path.suffix == ".xlsx":
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        return {sheet.title: list(sheet.values) for sheet in workbook.worksheets}
    elif path.suffix == ".yaml" or path.suffix == ".yml":
        return yaml.load(path.read_text(encoding=encoding), Loader=Loader)
    # xml
    elif path.suffix == ".xml":
        try:
            import xmltodict
        except ImportError:
            raise ImportError("xmltodict not installed")
        return xmltodict.parse(path.read_text(encoding=encoding))
    else:
        raise NotImplementedError(f"File format {path.suffix} not supported")