
check:
	cargo check

fmt:
	cargo +nightly fmt

# Test-build the addon image the same way CI does (amd64 only).
addon:
	docker build \
		--build-arg BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.21 \
		-f addon/Dockerfile \
		.

# This will start hass on http://localhost:7123
container:
	npm install @devcontainers/cli
	npx @devcontainers/cli up --workspace-folder .
	npx @devcontainers/cli exec --workspace-folder . supervisor_run

.PHONY: addon fmt check hass
