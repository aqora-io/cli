# Changelog
All notable changes to this project will be documented in this file. See [conventional commits](https://www.conventionalcommits.org/) for commit guidelines.

- - -
## v0.18.0 - 2025-09-10
#### Bug Fixes
- update serde_arrow and use large types as default - (17887e8) - Julian Popescu
- force flush first record batch and increase row group size and compression - (e0d2bcc) - Julian Popescu

- - -

## v0.17.0 - 2025-09-05
#### Bug Fixes
- remove low precision blob slicing - (238d79c) - Julian Popescu
- suspend version prompt - (d946e25) - Angel-Dijoux
#### Features
- add MergeRangesAsyncFileReader - (4b738ee) - Julian Popescu
- prompt for a version on upload - (0b50f36) - Angel-Dijoux
- allow uploading parquet and directories with cli - (e238577) - Julian Popescu

- - -

## v0.16.0 - 2025-09-02
#### Bug Fixes
- prettify output - (5b5e4ec) - Angel-Dijoux
- only send credentials to descendants of a domain, not to every descendant of corresponding TLD - (66b62ca) - Antoine Chauvin
- update gql contract - (a6ffbef) - Angel-Dijoux
- hide progress when prompting for dataset creation - (740ecd8) - Antoine Chauvin
- improve `allow_request_url` & its tests - (60d9660) - Antoine Chauvin
- make `dataset upload` look more like `dataset convert` - (056c4a7) - Antoine Chauvin
- use a custom body for wasm targets - (9de8496) - Julian Popescu
#### Features
- allow raw parquet upload - (d39a428) - Julian Popescu
- create dataset - (1426721) - Angel-Dijoux
- create a dataset version - (defa00e) - Angel-Dijoux
- list dataset versions - (e6543b3) - Angel-Dijoux
- dir reader - (7b7ea21) - Julian Popescu
- implement easier way to read datasets with polars - (5cd381f) - Antoine Chauvin
- fsspec implementation - (a717227) - Antoine Chauvin
- graphql python client - (7a91f5c) - Antoine Chauvin
- provide some extent of configuration when uploading datasets - (2572ec1) - Antoine Chauvin
- upload dataset - (de6fbcb) - Antoine Chauvin
- add ipc support and abstract RecordBatchStream - (2eb5118) - Julian Popescu
- implement a part uploader with aqora-client - (caf2bab) - Julian Popescu
- data utils - (3cd9e6f) - Julian Popescu
#### Miscellaneous Chores
- update version patch for npm release - (3a8dd2f) - Julian Popescu
- fix clippy errors - (87e4497) - Julian Popescu
- update arrow/parquet deps - (7a0e5b3) - Julian Popescu
#### Refactoring
- cleanup dataset command module files - (327e632) - Julian Popescu
- fsspec & pyarrow - (64c846a) - Antoine Chauvin
- use tower instead of homegrown middleware - (15736e7) - Julian Popescu
- AsyncWrite for multipart uploads - (74f5529) - Julian Popescu
#### Tests
- fix compilation issues - (a993e65) - Antoine Chauvin

- - -

## v0.15.1 - 2025-06-07
#### Bug Fixes
- multipart upload - (ea159f3) - Antoine Chauvin
- do not retry on forbidden S3 responses - (8430d67) - Antoine Chauvin

- - -

## v0.15.0 - 2025-06-06
#### Bug Fixes
- use project name from backend - (6ee6e68) - Antoine Chauvin
- deserialize issues for serde_json arbitrary precision - (ca4e4ce) - Julian Popescu
#### Build system
- reset version to 0.14.3 - (682989c) - Antoine Chauvin
#### Continuous Integration
- use official sccache workflow - (08c4fdf) - Julian Popescu
#### Features
- add version to user-agent - (d50e947) - Antoine Chauvin
- competition stages - (0c284d5) - Antoine Chauvin
#### Miscellaneous Chores
- switch to pyo3 v0.24 - (996c684) - Antoine Chauvin
#### Refactoring
- move client to separate package - (92c0549) - Julian Popescu

- - -

## v0.14.3 - 2025-04-25
#### Bug Fixes
- remove deprecated pyo3 functions - (477e2a3) - Julian Popescu
- apply suggestions - (4f8d9df) - Angel Dijoux
- migrate from into_py to into_pyobject function - (2040cd7) - Angel Dijoux
#### Features
- query Competition::show_metric - (e30adf5) - Antoine Chauvin
#### Miscellaneous Chores
- switch to pyo3 v0.24 - (f3e3988) - Angel Dijoux
- update rust toolchain - (d55b031) - Julian Popescu

- - -

## v0.14.2 - 2025-04-09
#### Bug Fixes
- add client_secret to oauth refresh - (d8006bb) - Julian Popescu

- - -

## v0.14.1 - 2025-04-04
#### Bug Fixes
- FileRef::chunks was not sound - (5024f74) - Antoine Chauvin

- - -

## v0.14.0 - 2025-04-01
#### Bug Fixes
- retry upload on checksum failure - (59fa0e9) - Antoine Chauvin
#### Features
- disable auto tick - (0f96505) - Julian Popescu
- unpack in existing directory - (687ee50) - Julian Popescu
- make aqora config directory configurable - (20d8167) - Julian Popescu
#### Refactoring
- simplify file reading - (ed866b0) - Antoine Chauvin
- improve upload retry log message - (7b875de) - Antoine Chauvin
- simplify checksums - (2db63ce) - Antoine Chauvin

- - -

## v0.13.0 - 2025-03-26
#### Bug Fixes
- **(ci)** uplaod artifact - (b935392) - Angel Dijoux
- upgrade zip & other dependencies - (a50b07f) - Antoine Chauvin
#### Continuous Integration
- update actions - (5f6e88c) - Antoine Chauvin
#### Features
- login with subscription - (b721c35) - Julian Popescu

- - -

## v0.12.0 - 2025-01-15
#### Bug Fixes
- template command creating a faulty directory - (3881a12) - Antoine Chauvin
#### Features
- skip when say : - (732662e) - Angel Dijoux
#### Miscellaneous Chores
- fix maturin 1.8.0 build issue - (58845bf) - Angel Dijoux
#### Refactoring
- add convenience methods for no-prompt - (4d1443e) - Julian Popescu

- - -

## v0.11.0 - 2024-12-10
#### Features
- accept rules with cli - (94fb62c) - Julian Popescu
- add organization prompt for template download - (987aa0d) - Julian Popescu
- prompt user login on template - (bea1c4a) - Julian Popescu

- - -

## v0.10.0 - 2024-12-07
#### Bug Fixes
- upload templates without ignoring files - (ecd8e3d) - Julian Popescu
#### Features
- init a Git repo for default use_case template (#97) - (0d7408d) - Angel Dijoux
- q3as in default use case template (#105) - (e2203c8) - Angel Dijoux
#### Miscellaneous Chores
- clippy errors (#106) - (b55bf69) - Angel Dijoux

- - -

## v0.9.0 - 2024-11-21
#### Features
- symlink use_case in template - (445bccc) - Julian Popescu

- - -

## v0.8.0 - 2024-11-13
#### Bug Fixes
- add attr magic functions to overriden python builtins - (3548cd3) - Julian Popescu
- update pep508_rs which had unmaintained dependencies - (2131abe) - Antoine Chauvin
- load correct config dir - (69f89fe) - Angel Dijoux
- naming stuff, use dialoguer - (daf3e7b) - Angel Dijoux
#### Features
- use aqora-cli in venv only if version is greater - (49f396f) - Julian Popescu
- add aqora-cli to install command as dep - (0565355) - Julian Popescu
- hide score if no leaderboard - (1a032dd) - Julian Popescu
- ask to install vscode extensions - (5e9cc02) - Angel Dijoux
#### Miscellaneous Chores
- create our own config file - (9928829) - Angel Dijoux
- add aqora vscode extension - (1f034d7) - Angel Dijoux
#### Refactoring
- vscode load file module - (592d3c5) - Angel Dijoux

- - -

## v0.7.0 - 2024-10-25
#### Features
- zstd (#93) - (fd752c8) - Antoine Chauvin

- - -

## v0.6.1 - 2024-10-18
#### Bug Fixes
- Fix templates to wait on AsyncIterator - (41926ef) - Julian Popescu

- - -

## v0.6.0 - 2024-10-17
#### Features
- aqora new (#90) - (420f622) - Julian Popescu

- - -

## v0.5.2 - 2024-10-14

- - -

## v0.5.1 - 2024-10-11
#### Continuous Integration
- add bin in release details - (a193d1b) - Angel Dijoux
#### Features
- parallel gzip (#88) - (3a7a522) - Antoine Chauvin

- - -

## v0.5.0 - 2024-08-26
#### Features
- Add link mode as option to CLI - (f42edf3) - Julian Popescu
- Add link mode to PyEnvOptions and PipOptions - (8525753) - Julian Popescu
#### Refactoring
- Use options struct and short hand for env init - (0ccc23d) - Julian Popescu

- - -

## v0.4.0 - 2024-08-25
#### Bug Fixes
- Use dunce to canonicalize paths - (22d172e) - Julian Popescu
#### Features
- Add virtual environment info to aqora info - (5615e19) - Julian Popescu

- - -

## v0.3.4 - 2024-08-24
#### Bug Fixes
- Use custom template for python exporter - (d3d9ee4) - Julian Popescu

- - -

## v0.3.3 - 2024-08-23
#### Bug Fixes
- Upgrade python dependencies - (e4fb124) - Julian Popescu

- - -

## v0.3.2 - 2024-08-01
#### Bug Fixes
- Properly dispose of temp directory - (a8391f1) - Julian Popescu

- - -

## v0.3.1 - 2024-07-30
#### Bug Fixes
- Add allow threads in module call - (a1e54c8) - Julian Popescu

- - -

## v0.3.0 - 2024-07-26
#### Bug Fixes
- **(cli)** Add warning for installed version differing from selected - (412f48e) - Julian Popescu
- Prevent infinite loop on venv aqora search - (ccf7145) - Julian Popescu
- Add exit code back in - (fb3b461) - Julian Popescu
- sigsegv on low resource machines - (34105bf) - Antoine Chauvin
#### Features
- Use aqora in venv if available - (8e75c17) - Julian Popescu
- Add ability to configure python - (1a4c055) - Julian Popescu
#### Refactoring
- Simplify and isolate asyncio run - (9a24d88) - Julian Popescu

- - -

## v0.2.1 - 2024-07-18
#### Bug Fixes
- **(cli)** loaded python kernel - (92d6b4d) - Angel Dijoux
- build for aarch64 - (9161b61) - Angel Dijoux
#### Miscellaneous Chores
- **(version)** v0.2.1 - (6b2adf8) - Angel Dijoux
- change endpoint - (90a1132) - Angel Dijoux
- Update simple deps - (ee9fdbe) - Julian Popescu

- - -

## v0.2.1 - 2024-07-17
#### Bug Fixes
- **(cli)** loaded python kernel - (92d6b4d) - Angel Dijoux
- build for aarch64 - (9161b61) - Angel Dijoux
#### Miscellaneous Chores
- Update simple deps - (ee9fdbe) - Julian Popescu

- - -

## v0.2.0 - 2024-06-28
#### Features
- **(cli)** run aqora test asynchronously - (973682b) - Julian Popescu
- load .venv by default when opening the project - (9d07f8d) - Angel Dijoux
- operate from 'pyproject.toml' - (1a40ca3) - Angel Dijoux
- open in vscode by default - (f0b2168) - Angel Dijoux
#### Miscellaneous Chores
- **(cli)** clippy lint - (9e9e769) - Julian Popescu
#### Refactoring
- simplify vs code opening and create virtual environment - (c4048e1) - Julian Popescu

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