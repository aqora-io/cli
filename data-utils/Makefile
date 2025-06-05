SCOPE?=aqora-io
BUILDFLAGS?=
CARGOFLAGS?=
RUSTFLAGS?=

BUILDFLAGS+= --scope ${SCOPE}

ifeq ($(PROFILE),release)
	BUILDFLAGS+= --release
else ifeq ($(PROFILE),profiling)
	BUILDFLAGS+= --profiling
	CARGOFLAGS+= --features console_error_panic_hook
else
	BUILDFLAGS+= --dev
	CARGOFLAGS+= --features console_error_panic_hook
endif

BROWSER?=chrome

.PHONY: wasm
wasm: ts wasm-build wasm-pkg

.PHONY: wasm-pkg
wasm-pkg:
	@sh configure-pkg.sh

.PHONY: wasm-build
wasm-build:
	RUSTFLAGS="${RUSTFLAGS}" wasm-pack build --target web --out-name index --weak-refs --reference-types \
		${BUILDFLAGS} --no-default-features --features default-wasm ${CARGOFLAGS}

.PHONY: ts
ts:
	cargo test export_bindings --no-default-features --features "wasm-test"

.PHONY: watch
watch:
	cargo watch -s "make wasm"

.PHONY: test
test:
	wasm-pack test --${BROWSER} --no-default-features --features "wasm-test"

.PHONY: clean
clean:
	rm -rf pkg
