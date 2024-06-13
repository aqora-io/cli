# Changelog
All notable changes to this project will be documented in this file. See [conventional commits](https://www.conventionalcommits.org/) for commit guidelines.

- - -
## v0.1.16 - 2024-06-13
#### Bug Fixes
- fix versioning by using cargo.toml version - (7cdd3d5) - Julian Popescu
- review suggestions - (b652952) - Julian Popescu
- allow top level async statements in python notebooks - (e2b1569) - Julian Popescu
- QoL improvements for python & shell commands - (4682050) - Antoine Chauvin
- make find_project_version_file more clear - (5d3e5c4) - Julian Popescu
#### Features
- override ipython in runner - (532bedc) - Julian Popescu
- basic ipynb conversion using jupyter nbconvert - (02c47d7) - Julian Popescu
- add `aqora lab` command - (832b916) - Antoine Chauvin
- browserless login - (68e764b) - Antoine Chauvin
- add multipart upload - (aff1aec) - Julian Popescu
#### Miscellaneous Chores
- **(version)** 0.1.16 - (78b237c) - Julian Popescu
- add contributing guidelines and make bumping versions easier - (f30e191) - Julian Popescu

- - -

## 0.1.16 - 2024-06-13
#### Bug Fixes
- review suggestions - (b652952) - Julian Popescu
- allow top level async statements in python notebooks - (e2b1569) - Julian Popescu
- QoL improvements for python & shell commands - (4682050) - Antoine Chauvin
- make find_project_version_file more clear - (5d3e5c4) - Julian Popescu
- fix versions in pyproject and lock file - (6cbe855) - Julian Popescu
- allow org to create a submission version - (2a2b97f) - Julian Popescu
- use venv/bin/activate_this.py and add more context to errors - (3588895) - Julian Popescu
- don't dim text - (c144b03) - Julian Popescu
- properly inject sentry dsn - (c77f21f) - Julian Popescu
- add docker options for linux sentry dsn environment - (4896653) - Julian Popescu
- add sentry DSN to github workflow - (fc11a38) - Julian Popescu
- remove unnescessary warning flags - (7849f3c) - Julian Popescu
- import importlib.util directly - (130fe64) - Julian Popescu
- typo in error display - (1ead5b6) - Julian Popescu
- do not delete today's logfile - (83942ce) - Antoine Chauvin
- remove termcolor - (78c9fe3) - Antoine Chauvin
- clippy warnings - (f421c12) - Antoine Chauvin
- remove useless tracing - (d06ec58) - Antoine Chauvin
- use manifest_version() - (7801296) - Antoine Chauvin
- use python site configuration - (70fed4f) - Julian Popescu
- pip uninstall removed deps - (17df066) - Julian Popescu
- less intrusive removals - (4fc94c9) - Julian Popescu
- better requirement extras check - (3664453) - Julian Popescu
- more performant remove deps - (4f20be8) - Julian Popescu
- add better README installation instructions - (13473c7) - Julian Popescu
- use copy instead of rename - (e3e60ce) - Julian Popescu
- add sync_all to file replacement for credentials - (b2ebfe6) - Julian Popescu
- show authorize_url when logging-in - (6d6c6fa) - Antoine Chauvin
- print score so that user can see score before continuing with version update - (df47221) - Julian Popescu
- allow for windows style virtual environment site-packages - (ab3670f) - Julian Popescu
- remove symlink dir reference in clean - (811b041) - Julian Popescu
- use windows naming for virtual environment subdirectories - (c885180) - Julian Popescu
- better debug messages for pip install - (e5dfba4) - Julian Popescu
- use pipx list command to find venv - (7d3082e) - Julian Popescu
- use default .venv path for virtual environment - (5d184a6) - Julian Popescu
- just check if has_ref instead of is_ignored - (4668df9) - Julian Popescu
#### Build system
- emit warning when sentry integration is disabled - (880352c) - Antoine Chauvin
- fail build on malformed sentry dsn - (cd3ae71) - Antoine Chauvin
#### Features
- override ipython in runner - (532bedc) - Julian Popescu
- basic ipynb conversion using jupyter nbconvert - (02c47d7) - Julian Popescu
- add `aqora lab` command - (832b916) - Antoine Chauvin
- browserless login - (68e764b) - Antoine Chauvin
- add multipart upload - (aff1aec) - Julian Popescu
- show decompress progression bar - (41272f0) - Antoine Chauvin
- implement download progress bar - (4e7c17a) - Antoine Chauvin
- do install after template - (7160c22) - Julian Popescu
- better sentry context and event filtering - (157a4bc) - Julian Popescu
- only clean ignored files - (b27e8ca) - Julian Popescu
- better clean function - (3ee8483) - Julian Popescu
- use case notebook conversion and better print statements - (bc9eea2) - Julian Popescu
- enable tracing env-filter - (6a679c0) - Antoine Chauvin
- garbage collect logging files - (224e7b1) - Antoine Chauvin
- document DO_NOT_TRACK - (2e31516) - Antoine Chauvin
- replace log with tracing - (4e339fb) - Antoine Chauvin
- sentry integration - (672f822) - Antoine Chauvin
- add gets latest version and always overwrites on merge if dependency given - (d7a61a7) - Julian Popescu
- merge dependencies if possible - (a5b5b07) - Julian Popescu
- aqora remove command - (fb1b477) - Julian Popescu
- aqora add command - (41395b4) - Julian Popescu
- tell the user where to accept competition rules - (09bfaff) - Julian Popescu
- add clean command - (24c6e48) - Julian Popescu
- look in pipx venvs for uv - (0f7998a) - Julian Popescu
- handle ctrl-c gracefully and add info for debugging - (86405c6) - Julian Popescu
- add aqora install and aqora test for use cases - (65cab27) - Julian Popescu
- install dependencies: uv, build, setuptools - (e01594b) - Antoine Chauvin
- enable cd - (06573b8) - Antoine Chauvin
- declare extension-module - (35ace55) - Antoine Chauvin
- implement extension-module - (076be45) - Antoine Chauvin
#### Miscellaneous Chores
- add contributing guidelines and make bumping versions easier - (f30e191) - Julian Popescu
#### Refactoring
- more readable cli exit - (a5fe1c0) - Antoine Chauvin
- simplify fn do_not_track - (5c34649) - Antoine Chauvin
- explicits do_not_track() - (328e5c6) - Antoine Chauvin

- - -

Changelog generated by [cocogitto](https://github.com/cocogitto/cocogitto).