DOCKER_BIN ?= $(shell which docker)
YDB_IMAGE=artifactory.nebius.dev/ghcr-mirror/ydb-platform/local-ydb:nightly

guard-%:
	@#$(or ${$*}, $(error $* is not set))

ydb: guard-DOCKER_BIN guard-YDB_IMAGE
	@make stop-ydb
	$(DOCKER_BIN) pull $(YDB_IMAGE)
	$(DOCKER_BIN) rm ydb || true
	$(DOCKER_BIN) run --hostname localhost -dp 2135:2135 -p 2136:2136 -p 8765:8765 --name ydb --ulimit nofile=90000:90000 --env 'YDB_YQL_SYNTAX_VERSION=1' --env 'GRPC_PORT=2135' --env 'GRPC_TLS_PORT=2136' --env 'YDB_USE_IN_MEMORY_PDISKS=true' -d $(YDB_IMAGE)

	@sleep 3
	@echo "YDB Server listening on localhost:2135"
	@echo "YDB Viewer on http://localhost:8765"

stop-ydb: guard-DOCKER_BIN
	@echo "Stopping ydb"
	$(DOCKER_BIN) stop ydb -t0 >/dev/null 2>&1 || true
	@echo "Done"
