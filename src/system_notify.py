import shutil
from typing import Optional


def is_notify_send_available():
    return shutil.which("notify-send") is not None


def send_notify(header: str, msg: Optional[str] = None):
    if is_notify_send_available():
        print("notify-send is available")
        import subprocess
        subprocess.run(['notify-send', header, msg or ""])


