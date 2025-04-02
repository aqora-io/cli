BUILDFLAGS?=
RUSTFLAGS?=

ifeq ($(RELEASE),)
	BUILDFLAGS+= --dev
else
	BUILDFLAGS+= --release
	RUSTFLAGS+= -C opt-level=s
endif

BROWSER?=chrome

.PHONY: wasm
wasm: ts
	RUSTFLAGS="${RUSTFLAGS}" wasm-pack build --target web --out-name index  ${BUILDFLAGS} --no-default-features --features default-wasm

.PHONY:
ts:
	TS_RS_EXPORT_DIR="pkg" cargo test export_bindings --features "default-wasm wasm-bindgen-test"

.PHONY: watch
watch:
	cargo watch -s "make wasm"

.PHONY: test
test:
	wasm-pack test --${BROWSER} --no-default-features --features "default-wasm wasm-bindgen-test"

.PHONY: clean
clean:
	rm -rf pkg
