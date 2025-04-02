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
wasm:
	RUSTFLAGS="${RUSTFLAGS}" wasm-pack build --target web --out-name index  ${BUILDFLAGS} --features default-wasm

.PHONY: watch
watch:
	cargo watch -s "make wasm"

.PHONY: test
test:
	wasm-pack test --${BROWSER} --features "default-wasm wasm-bindgen-test"

.PHONY: clean
clean:
	rm -rf pkg
