.PHONY: sync-schema
sync-schema:
	make -s -C ../platform/backend print-schema > src/graphql/schema.graphql
