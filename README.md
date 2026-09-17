# Installation

To install the CLI, we recommend using [uv](https://docs.astral.sh/uv/). Note
that the CLI requires Python 3.10 or greater.

If you don't already have `uv`, follow the [installation
instructions](https://docs.astral.sh/uv/getting-started/installation/). Then
install the CLI as a tool:

```bash
uv tool install aqora
```

This makes the `aqora` command available globally. To upgrade later, run:

```bash
uv tool upgrade aqora
```

## Notes for Windows users

If `uv` gives you a warning that the script is not on your PATH, you can copy the
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

# Storage

Every account comes with private S3 storage. The credentials are short-lived
and derived from your login, so they stop working when you log out.

Export them for any S3 client, including Rust's `object_store`, the AWS SDKs and
the AWS CLI:

```bash
eval "$(aqora store credentials --format env)"
```

Or install an AWS profile that mints fresh credentials whenever they expire:

```bash
aqora store configure-aws
aws --profile aqora s3 ls s3://<your username>/
```

`--format json`, `--format aws-process` and `--format duckdb` are also
available. From Python, `aqora.Store` hands back ready-made clients that renew
their credentials on their own:

```python
from aqora import Store

store = Store()
s3 = store.obstore()          # obstore.store.S3Store
client = store.boto3()        # boto3 S3 client
fs = store.s3fs()             # s3fs.S3FileSystem
store.duckdb(con)             # registers an S3 secret on a DuckDB connection
creds = store.credentials()   # or `await store.credentials_async()`
```

Each helper needs its library installed, for example `pip install aqora[obstore]`.
Inside a workspace runner no login is needed. Viewer and API-key sessions need
the `read:storage` scope, and `write:storage` to write.

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

