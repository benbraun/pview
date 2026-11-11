
check:
	cargo check

fmt:
	cargo +nightly fmt

# Test-build the app image the same way CI does (amd64 only).
addon:
	docker buildx build --platform linux/amd64 -f addon/Dockerfile .

# This will start hass on http://localhost:7123
container:
	npm install @devcontainers/cli
	npx @devcontainers/cli up --workspace-folder .
	npx @devcontainers/cli exec --workspace-folder . supervisor_run

.PHONY: addon fmt check hass
