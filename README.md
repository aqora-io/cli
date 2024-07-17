# Installation

To install the CLI, you can use pip. Note that the CLI requires Python 3.9 or greater

```bash
# with pip
pip install aqora-cli

# with pipx
pipx install aqora-cli

# on Windows with Python installed from python.org
py -m pip install aqora-cli

# on Windows with Python installed from the Windows Store
python -m pip install aqora-cli
```

## Notes for Windows users

If pip gives you a warning that the script is not on your PATH, you can copy the
directory in the warning and add it to your path by [following the instructions
here](https://www.java.com/en/download/help/path.html)

You may also need to install the latest Visual C++ Redistributable Version. You
can find [the latest version here](https://learn.microsoft.com/cpp/windows/latest-supported-vc-redist?view=msvc-170#latest-microsoft-visual-c-redistributable-version)

# Getting Started

To verify the installation, the following should output helpful information

```bash
aqora help
```

Once the CLI has installed you can login to your account with the following command

```bash
aqora login
```

Browse the competitions on [aqora.io](https://aqora.io) and climb the leaderboards!

# Contributing

We strongly recommend you to install a stable Rust toolchain using [Rustup](https://rustup.rs/), and a
[stable Python](https://www.python.org/downloads/) runtime through official distributions.

Our main branch is called `main`. Please open a pull-request up-to-date to that branch
when you request our feedback.

Please follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/), which allows our
project to have beautiful changelogs based on your commit messages. We strongly
encourage you to install [Cocogitto](https://docs.cocogitto.io/):

```bash
$ cargo install cocogitto cargo-edit
$ cog install-hook --all

