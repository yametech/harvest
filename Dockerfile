FROM rustlang/rust:nightly-stretch as cargo-build

RUN apt-get update

RUN apt-get install musl-tools -y

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/harvest

COPY Cargo.toml Cargo.toml

ADD . .

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

RUN rm -f target/x86_64-unknown-linux-musl/release/deps/harvest*

ADD . .

# RUN mv config ${HOME}/.cargo/config

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM alpine:latest

COPY --from=cargo-build /usr/src/harvest/target/x86_64-unknown-linux-musl/release/harvest /usr/local/bin/harvest

ENTRYPOINT ["harvest"]