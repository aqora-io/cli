#!/usr/bin/env bash
# End-to-end check for the model-reuse / price-estimation feature.
#
#   ./test-model-reuse.sh                 # offline tiers only (0 + A)
#   AQORA_URL=https://app-pr-123.aqora-internal.io ./test-model-reuse.sh   # + tier B
#
# Tier 0  cross-repo qio parity        (fast, catches the CLI/platform skew)
# Tier A  offline test suite           (mirrors .github/workflows/ci.yaml exactly)
# Tier B  live PR environment          (only if AQORA_URL is set)

set -uo pipefail

CLI_DIR="${CLI_DIR:-$HOME/Development/cli}"
PLATFORM_DIR="${PLATFORM_DIR:-$HOME/Development/platform}"

FAILED=()
step()  { printf '\n\033[1m── %s\033[0m\n' "$1"; }
ok()    { printf '   \033[32mok\033[0m   %s\n' "$1"; }
bad()   { printf '   \033[31mFAIL\033[0m %s\n' "$1"; FAILED+=("$1"); }
run()   { local d="$1"; shift; if "$@"; then ok "$d"; else bad "$d"; fi; }

# ── Tier 0 ── qio parity ─────────────────────────────────────────────────────
# The CLI pins qio by exact rev; platform floats on branch=main and records the
# resolved sha in Cargo.lock. They must agree or you ship a combination that was
# never tested together.
step "Tier 0 — qio revision parity"

cli_rev=$(grep -oP 'qio-rs", rev = "\K[0-9a-f]{40}' "$CLI_DIR/Cargo.toml" || true)
plat_rev=$(grep -oP 'qio-rs\?branch=main#\K[0-9a-f]{40}' "$PLATFORM_DIR/backend/Cargo.lock" || true)

printf '   cli      %s\n   platform %s\n' "${cli_rev:-<none>}" "${plat_rev:-<none>}"
if [[ -n "$cli_rev" && "$cli_rev" == "$plat_rev" ]]; then
  ok "revisions match"
else
  bad "qio revision skew — bump $CLI_DIR/Cargo.toml then: cargo update -p qio"
  echo "   (continuing; later failures may be caused by this)"
fi

# ── Tier A ── offline suite ──────────────────────────────────────────────────
# Everything here is mocked: test_universal_qpu.py fakes qiskit/pytket/hugr and
# the aqora package, keeping only the real native extension, because the format
# constants come from the Rust qio crate. So this needs no backend — and it is
# precisely what exercises the qio bump above.
step "Tier A — offline suite (CI parity)"
cd "$CLI_DIR" || exit 1

# uv sync builds the native extension through maturin and installs the dev group
# (pytest + pytest-asyncio). uv fetches its own CPython, so no system python3.
run "uv sync" uv sync

# The cargo targets below build WITHOUT the extension-module feature (default
# features are empty), so pyo3 links a real libpython — point it at uv's.
export PYO3_PYTHON="$CLI_DIR/.venv/bin/python"

# NixOS only: uv's manylinux CPython loads wheels (nbconvert) that need
# libstdc++.so.6, which isn't on the default search path here. Without this,
# ipynb::tests::test_notebook_to_script fails on ImportError. No-op elsewhere.
if [[ -e /etc/NIXOS ]] && gcclib=$(nix eval --raw nixpkgs#stdenv.cc.cc.lib 2>/dev/null); then
  export LD_LIBRARY_PATH="$gcclib/lib:${LD_LIBRARY_PATH:-}"
fi

run "cargo fmt"     cargo fmt --all --check
run "cargo clippy"  cargo clippy --workspace --all-targets -- -D warnings
run "cargo test"    cargo test --workspace
run "aqora --version" cargo run -- --version

run "pytest (provider + qpu)" uv run --no-sync pytest -q \
  test/test_qiskit_provider.py \
  test/test_pytket_provider.py \
  test/test_guppy_provider.py \
  test/test_universal_qpu.py \
  test/test_exports.py \
  test/test_viewer_login.py \
  test/test_store.py \
  test/test_kv.py

# ── Tier B ── live PR environment ────────────────────────────────────────────
step "Tier B — live PR environment"

if [[ -z "${AQORA_URL:-}" ]]; then
  echo "   skipped (AQORA_URL unset)"
else
  echo "   target $AQORA_URL"

  run "frontend reachable" curl -fsS -o /dev/null --max-time 20 "$AQORA_URL/"

  # Proves YOUR platform branch is what's deployed, not main: the reworded
  # createProviderJob description only exists on your branch.
  desc=$(curl -fsS --max-time 20 -X POST "$AQORA_URL/graphql" \
    -H 'content-type: application/json' \
    -d '{"query":"{__type(name:\"Mutation\"){fields{name description}}}"}' 2>/dev/null \
    | grep -o '"name":"createProviderJob","description":"[^"]*"' || true)

  if [[ -z "$desc" ]]; then
    bad "could not introspect createProviderJob (env down, or introspection disabled)"
  elif grep -q 'wasmModuleId` is deprecated' <<<"$desc"; then
    ok "deployed schema carries your branch (wasmModuleId deprecated)"
  else
    bad "deployed schema is STALE — pr-<N> image predates your platform commits"
  fi

  # `info` runs a `viewer` GraphQL query against AQORA_URL — non-interactive,
  # and it fails if the stored credentials don't cover this host.
  run "authenticated (viewer query)" env AQORA_URL="$AQORA_URL" cargo run -- info
fi

# ── summary ──────────────────────────────────────────────────────────────────
step "Summary"
if ((${#FAILED[@]} == 0)); then
  printf '   \033[32mall checks passed\033[0m\n\n'
  exit 0
fi
printf '   \033[31m%d failed:\033[0m\n' "${#FAILED[@]}"
printf '     - %s\n' "${FAILED[@]}"
echo
exit 1
