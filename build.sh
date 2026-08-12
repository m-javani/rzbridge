#!/usr/bin/env bash

set -euo pipefail

# cargo clean --release
cargo build  --release

strip --strip-all target/release/rzbridge

upx --best --lzma target/release/rzbridge

ls -lh target/release/rzbridge

cp target/release/rzbridge .