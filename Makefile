
check:
	cargo check

fmt:
	cargo +nightly fmt

# Test-build the addon image the same way CI does (amd64 only).
# Override the source repo/ref with e.g. PVIEW_REPO=https://github.com/wez/pview.git
addon:
	docker build \
		--build-arg BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.21 \
		$(if $(PVIEW_REPO),--build-arg PVIEW_REPO=$(PVIEW_REPO)) \
		$(if $(PVIEW_REF),--build-arg PVIEW_REF=$(PVIEW_REF)) \
		-f addon/Dockerfile \
		addon

# This will start hass on http://localhost:7123
container:
	npm install @devcontainers/cli
	npx @devcontainers/cli up --workspace-folder .
	npx @devcontainers/cli exec --workspace-folder . supervisor_run

.PHONY: addon fmt check hass
