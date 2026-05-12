# Build Lakekeeper from a local checkout of the labels-crud-verb branch
# (laskoviymishka/lakekeeper#labels-crud-verb). The build context is the
# Lakekeeper source root; pass it in via docker-compose's build.context.

FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -yqq \
        libclang-dev cmake build-essential libpq-dev pkg-config \
        --no-install-recommends

WORKDIR /app
COPY . .

ENV SQLX_OFFLINE=true

# Build without --all-features (skips UI/console which needs vue-tsc + node).
# The labels-management demo only needs the IRC server.
RUN cargo build --release --locked --bin lakekeeper

# Runtime
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --chmod=555 --from=builder /app/target/release/lakekeeper /home/nonroot/lakekeeper

ENTRYPOINT ["/home/nonroot/lakekeeper"]
