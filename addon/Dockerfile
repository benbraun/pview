# Runtime base image. The multi-arch Home Assistant base resolves to the build
# platform, so no per-arch override is needed (build.yaml is deprecated).
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24

####################################################################################################
## Builder
####################################################################################################
FROM --platform=$BUILDPLATFORM alpine:3.24 AS builder
ARG TARGETPLATFORM

RUN case "$TARGETPLATFORM" in \
      "linux/amd64")  echo x86_64-unknown-linux-musl ;; \
      "linux/arm64")  echo aarch64-unknown-linux-musl ;; \
      *) echo "Unsupported platform: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac > /rust_target.txt

# gcc supplies `cc`, which links build scripts and the amd64 target; Alpine's
# clang stopped pulling it in after 3.21.
RUN apk --no-cache add clang gcc git curl rustup musl-dev lld
RUN rustup-init -y --no-modify-path --profile minimal --default-toolchain stable
ENV PATH=/root/.cargo/bin:$PATH
RUN rustup target add "$(cat /rust_target.txt)"

WORKDIR /work
COPY . .

# .cargo/config.toml links aarch64/armv7 with bare lld for cross builds. On a
# native builder cargo applies that to host proc-macros too, and bare lld cannot
# find -lc or -lgcc_s, so link with cc when building for the build machine.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/work/target \
    target="$(cat /rust_target.txt)" && \
    if [ "$(rustc -vV | sed -n 's/^host: //p')" = "$target" ]; then \
        export "CARGO_TARGET_$(echo "$target" | tr 'a-z-' 'A-Z_')_LINKER=cc"; \
    fi && \
    CC=clang cargo build --locked --release --target "$target" --bin pview && \
    cp "target/$target/release/pview" /pview

####################################################################################################
## Runtime image
####################################################################################################
FROM $BUILD_FROM

COPY addon/run.sh /run.sh
COPY --from=builder /pview /pview

LABEL \
    org.opencontainers.image.title="Home Assistant App: PowerView MQTT Bridge" \
    org.opencontainers.image.description="Acts as a bridge between a Hunter Douglas PowerView Hub and Home Assistant, via the Home Assistant MQTT Integration." \
    org.opencontainers.image.source="https://github.com/benbraun/pview" \
    org.opencontainers.image.licenses="MIT"

CMD ["/run.sh"]
