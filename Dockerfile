# Server image: a static musl binary on an empty base.
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src src
RUN cargo build --release --locked && mkdir /data

FROM scratch
COPY --from=build /src/target/release/postino /postino
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
ENV POSTINO_HOST=0.0.0.0 \
    POSTINO_DATA_DIR=/data
VOLUME /data
EXPOSE 2790
ENTRYPOINT ["/postino"]
CMD ["serve"]
