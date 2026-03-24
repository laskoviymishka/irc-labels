FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update -qq && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yqq libclang-dev cmake build-essential libpq-dev pkg-config --no-install-recommends

WORKDIR /app
COPY . .

ENV SQLX_OFFLINE=true
# Build WITHOUT --all-features (skips UI/console which needs vue-tsc)
RUN cargo build --release --locked --bin lakekeeper

# Runtime
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --chmod=555 --from=builder /app/target/release/lakekeeper /home/nonroot/lakekeeper

ENTRYPOINT ["/home/nonroot/lakekeeper"]
