#!/usr/bin/env bash
# // SPDX-License-Identifier: BUSL-1.1
# // Copyright (c) 2026 M. Javani
# //
# // This file is part of rzbridge.
# //
# // Use of this software is governed by the Business Source License 1.1
# // included in the LICENSE file in the root of this repository.


set -euo pipefail

# cargo clean --release
cargo build  --release

strip --strip-all target/release/rzbridge

upx --best --lzma target/release/rzbridge

ls -lh target/release/rzbridge

cp target/release/rzbridge .