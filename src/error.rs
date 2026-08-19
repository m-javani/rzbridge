// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RZError {
    #[error("Operation cancelled")]
    Cancelled,
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Config error: {0}")]
    Config(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Internal error: {0}")]
    System(String),
    #[error("no leader found in cluster")]
    NoLeaderAvailable,
    #[error("no follower node found in cluster")]
    NoFollowerNodeAvailable,
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Identity resolution error: {0}")]
    Resolver(String),
    #[error("Request timeout")]
    Timeout,
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("internal error: {0}")]
    Io(#[from] io::Error),
}
