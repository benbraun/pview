ARG BUILD_FROM
# Git ref (branch or tag) to build from. Override in CI to pin a release.
ARG PVIEW_REF=main

####################################################################################################
## Builder
####################################################################################################
FROM --platform=$BUILDPLATFORM alpine:3.21 AS builder
ARG TARGETPLATFORM
ARG PVIEW_REF

RUN case "$TARGETPLATFORM" in \
      "linux/arm/v7") echo armv7-unknown-linux-musleabihf ;; \
      "linux/amd64")  echo x86_64-unknown-linux-musl ;; \
      "linux/arm64")  echo aarch64-unknown-linux-musl ;; \
      *) echo "Unsupported platform: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac > /rust_target.txt

RUN apk --no-cache add clang git curl rustup musl-dev lld
RUN rustup-init -y --no-modify-path --default-toolchain stable
ENV PATH=/root/.cargo/bin:$PATH
RUN rustup target add "$(cat /rust_target.txt)"

WORKDIR /work
RUN git clone --depth=1 --branch "$PVIEW_REF" https://github.com/wez/pview.git .

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/work/target \
    CC=clang cargo build --locked --release \
        --target "$(cat /rust_target.txt)" \
        --bin pview --no-default-features --features mqtt-no-tls && \
    cp "target/$(cat /rust_target.txt)/release/pview" /pview

####################################################################################################
## Runtime image
####################################################################################################
FROM $BUILD_FROM

COPY run.sh /run.sh
COPY --from=builder /pview /pview

CMD ["/run.sh"]
