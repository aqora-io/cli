import platform
import sys
import os
import subprocess
import tempfile
import sysconfig
import zipfile
import tarfile
import argparse
from urllib.request import urlretrieve
from pathlib import Path
from ctypes.util import find_library


def is_rosetta_translated():
    try:
        result = subprocess.run(
            ["sysctl", "-n", "sysctl.proc_translated"], capture_output=True, check=True
        )
        return result.stdout.strip() == b"1"
    except subprocess.CalledProcessError:
        return False


def get_python_clib():
    system = platform.system()
    for major, minor in [(3, 12), (3, 11), (3, 10), (3, 9), (3, 8)]:
        if system == "Windows":
            name = f"python{major}{minor}"
        else:
            name = f"python{major}.{minor}"
        lib = find_library(name)
        if lib:
            return (major, minor)
    return None


def get_release_asset_name():
    machine = platform.machine()

    # Check 64-bit
    if sys.maxsize <= 2**32:
        raise Exception("32-bit not supported: " + machine)

    # Get Python version
    py_version = get_python_clib()
    if py_version is None:
        raise Exception(
            "Python bindings not found. Make sure Python >= 3.8 shared library is installed."
        )
    py_version = f"py{py_version[0]}_{py_version[1]}"

    # Check system config
    system = platform.system()
    if system == "Windows":
        if machine != "AMD64":
            raise Exception("Unsupported processor: " + machine)
        return f"aqora-windows-x86_64-msvc-{py_version}.zip"
    elif system == "Linux":
        if machine != "x86_64":
            raise Exception("Unsupported processor: " + machine)
        libc, _ = platform.libc_ver()
        if libc == "glibc":
            return f"aqora-linux-x86_64-gnu-{py_version}.tar.gz"
        else:
            raise Exception("Unsupported libc: " + libc)
    elif system == "Darwin":
        if machine == "arm64":
            return f"aqora-darwin-aarch64-{py_version}.tar.gz"
        elif machine == "x86_64":
            if is_rosetta_translated():
                return f"aqora-darwin-aarch64-{py_version}.tar.gz"
            else:
                return f"aqora-darwin-x86_64-{py_version}.tar.gz"
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


def get_env_path():
    path = os.getenv("PATH")
    if path is None:
        return []
    return list(
        map(Path, filter(lambda path: os.access(path, os.W_OK), path.split(os.pathsep)))
    )


def pick_install_dir(dirs):
    env_path = get_env_path()
    for dir in dirs:
        if dir in env_path:
            return dir
    return None


def default_install_dir():
    system = platform.system()
    paths = [Path(sysconfig.get_path("scripts"))]
    xdg_bin_home = os.getenv("XDG_BIN_HOME")
    if xdg_bin_home:
        paths.append(Path(xdg_bin_home))
    xdg_data_home = os.getenv("XDG_DATA_HOME")
    if xdg_data_home:
        paths.append(Path(xdg_data_home).parent / "bin")
    if platform.system() == "Windows":
        paths.append(Path.home() / "AppData" / "Local" / "Programs")
        paths.append(Path.home() / "AppData" / "Local" / "Microsoft" / "WindowsApps")
    paths.append(Path.home() / ".local" / "bin")
    paths.append(Path.home() / "bin")
    if system in ["Linux", "Darwin"]:
        paths.append(Path("/usr") / "local" / "bin")
        paths.append(Path("/usr") / "bin")
        paths.append(Path("/bin"))
        paths.append(Path("/opt") / "homebrew" / "bin")
        paths.append(Path("/usr") / "local" / "opt")
        paths.append(Path("/opt") / "bin")
        paths.append(Path("/opt") / "local" / "bin")
        paths.append(Path("/usr") / "local" / "sbin")
        paths.append(Path("/usr") / "sbin")
        paths.append(Path("/sbin"))
    paths.append(Path.home() / ".local" / "sbin")
    paths.append(Path.home() / "sbin")
    return pick_install_dir(paths)


def do_install(install_dir):
    if install_dir is None:
        raise Exception(
            "No suitable installation directory found. Please specify one with --install-dir."
        )
    if not install_dir.exists() or not os.access(install_dir, os.W_OK):
        raise Exception(f"Cannot write to installation directory: {install_dir}")
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


def install(install_dir):
    try:
        do_install(install_dir)
    except Exception as e:
        print(f"Failed to install: {e}")
        print("Please try again or manually download the latest release from")
        print("https://github.com/aqora-io/cli/releases/latest")
        sys.exit(1)


def help():
    print("Usage: python install.py [options]")
    print("Options:")
    print("  --install-dir=DIR  Installation directory")
    print("  --help             Show this message")


def main():
    parser = argparse.ArgumentParser(
        prog="aqora CLI installer", description="Installs the aqora CLI"
    )
    parser.add_argument(
        "--install-dir",
        help="Installation directory",
        default=default_install_dir(),
        type=Path,
    )
    args = parser.parse_args()
    install(args.install_dir)


if __name__ == "__main__":
    main()
