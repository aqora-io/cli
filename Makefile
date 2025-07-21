.PHONY: sync-schema
sync-schema:
	make -s -C ../platform/backend print-schema > schema.graphql

.PHONY: lint
lint:
	cargo clippy --workspace --all-targets
