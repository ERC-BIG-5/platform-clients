from pathlib import Path


def dict_to_folders(base_dir: str, folder_dict: dict):
    base_path = Path(base_dir)
    for folder, subfolders in folder_dict.items():
        current_path = base_path / folder
        current_path.mkdir(parents=True, exist_ok=True)
        if isinstance(subfolders, dict):
            dict_to_folders(current_path, subfolders)
