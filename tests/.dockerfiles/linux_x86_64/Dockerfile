FROM rust:1.84.1
WORKDIR /root
ENV RUSTUP_UPDATE_ROOT=https://mirrors.aliyun.com/rustup/rustup
ENV RUSTUP_DIST_SERVER=https://mirrors.aliyun.com/rustup
RUN mkdir -p /root/.cargo
RUN echo "[source.crates-io]\nreplace-with = 'aliyun'\n[source.aliyun]\nregistry = 'sparse+https://mirrors.aliyun.com/crates.io-index/'" > /root/.cargo/config
RUN rustup toolchain install nightly
RUN cargo install cargo-llvm-cov cargo2junit