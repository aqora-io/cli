BUILDFLAGS?=
RUSTFLAGS?=

ifeq ($(RELEASE),)
	BUILDFLAGS+= --dev
else
	BUILDFLAGS+= --release
	# RUSTFLAGS+= -C opt-level=s
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
		${BUILDFLAGS} --no-default-features --features default-wasm

.PHONY: ts
ts:
	TS_RS_EXPORT_DIR="pkg" cargo test export_bindings --features "wasm-test"

.PHONY: watch
watch:
	cargo watch -s "make wasm"

.PHONY: test
test:
	wasm-pack test --${BROWSER} --no-default-features --features "wasm-test"

.PHONY: clean
clean:
	rm -rf pkg
