import platform
import sys
import os
import subprocess
import tempfile
import sysconfig
import zipfile
import tarfile
from urllib.request import urlretrieve
from pathlib import Path


def is_rosetta_translated():
    try:
        result = subprocess.run(
            ["sysctl", "-n", "sysctl.proc_translated"], capture_output=True, check=True
        )
        return result.stdout.strip() == b"1"
    except subprocess.CalledProcessError:
        return False


def get_release_asset_name():
    machine = platform.machine()
    if sys.maxsize <= 2**32:
        raise Exception("32-bit not supported: " + machine)
    system = platform.system()
    if system == "Windows":
        if machine != "x86_64":
            raise Exception("Unsupported processor: " + machine)
        return "aqora-windows-x86_64-msvc.zip"
    elif system == "Linux":
        if machine != "x86_64":
            raise Exception("Unsupported processor: " + machine)
        libc, _ = platform.libc_ver()
        if libc == "glibc":
            return "aqora-linux-x86_64-gnu.tar.gz"
        else:
            raise Exception("Unsupported libc: " + libc)
    elif system == "Darwin":
        if machine == "arm64":
            return "aqora-darwin-aarch64.tar.gz"
        elif machine == "x86_64":
            if is_rosetta_translated():
                return "aqora-darwin-aarch64.tar.gz"
            else:
                return "aqora-darwin-x86_64.tar.gz"
        else:
            raise Exception("Unsupported processor: " + machine)
    else:
        raise Exception("Unsupported platform: " + system)


def download_progress(count, block_size, total_size):
    blocks = 24
    downloaded = int(count * block_size * blocks / total_size)
    left = int(blocks - downloaded)
    print(
        f"\rDownloading ▐{'█' * downloaded}{'░' * left}▌",
        end="",
        flush=True,
    )


def user_install_dir():
    system = platform.system()
    if system == "Linux":
        xdg_bin_home = os.getenv("XDG_BIN_HOME")
        if xdg_bin_home:
            return Path(xdg_bin_home).absolute()
        xdg_data_home = os.getenv("XDG_DATA_HOME")
        if xdg_data_home:
            return Path(xdg_data_home).parent.absolute() / "bin"
        home = os.getenv("HOME")
        if home:
            return Path(home).absolute() / ".local" / "bin"
    return None


def do_install(install_dir=None):
    install_dir = install_dir or user_install_dir() or sysconfig.get_path("scripts")
    asset_name = get_release_asset_name()
    url = f"https://github.com/aqora-io/cli/releases/latest/download/{asset_name}"
    with tempfile.TemporaryDirectory() as temp_dir:
        download_path = Path(temp_dir) / asset_name
        print(f"Downloading...", end="", flush=True)
        urlretrieve(url, download_path, download_progress)
        print(f"\rDownloaded!")
        if asset_name.endswith(".zip"):
            with zipfile.ZipFile(download_path, "r") as zip_ref:
                zip_ref.extractall(install_dir)
        else:
            with tarfile.open(download_path, "r:gz") as tar_ref:
                tar_ref.extractall(install_dir)
        print(f"Installed to {install_dir}!")
    print("Run `aqora --help` to get started!")


def install(install_dir=None):
    try:
        do_install(install_dir)
    except Exception as e:
        print(f"Failed to install: {e}")
        print("Please try again or manually download the latest release from")
        print("https://github.com/aqora-io/cli/releases/latest")
        sys.exit(1)


if __name__ == "__main__":
    install()
