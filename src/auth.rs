use arc_swap::ArcSwap;
use std::{sync::Arc, time::Duration};
use tokio::{fs, time};
use tokio_util::sync::CancellationToken;

/// Static token using ArcSwap for lock-free reads and reloads
static TOKEN: tokio::sync::OnceCell<ArcSwap<String>> = tokio::sync::OnceCell::const_new();

#[derive(Debug, serde::Deserialize)]
struct AuthFile {
    roomzin_token: String,
}

/// Initialize token from auth file (called once at startup)
pub async fn init_token(tokens_path: &str) -> Result<(), String> {
    let auth_file = load_auth_file(tokens_path).await?;
    TOKEN
        .set(ArcSwap::from_pointee(auth_file.roomzin_token))
        .map_err(|_| "Token already loaded".to_string())?;
    tracing::info!("Auth token loaded from: {}", tokens_path);
    Ok(())
}

/// Load and parse the YAML file
async fn load_auth_file(path: &str) -> Result<AuthFile, String> {
    let contents = fs::read_to_string(path)
        .await
        .map_err(|e| format!("Failed to read auth file: {}", e))?;

    let auth_file: AuthFile =
        serde_yml::from_str(&contents).map_err(|e| format!("Failed to parse auth file: {}", e))?;

    Ok(auth_file)
}

/// Get Roomzin token (for connecting to the external backend server)
pub fn get_roomzin_token() -> String {
    let arc_swap = TOKEN.get().expect("Auth token not initialized");
    let token = arc_swap.load();
    token.to_string()
}

/// Watch auth file for changes and reload token automatically
pub async fn start_watcher(tokens_path: String, cancel_token: CancellationToken) {
    let mut interval = time::interval(Duration::from_secs(5));
    let mut last_modified = match fs::metadata(&tokens_path).await {
        Ok(metadata) => metadata
            .modified()
            .unwrap_or_else(|_| std::time::SystemTime::now()),
        Err(e) => {
            tracing::error!("Failed to get auth file metadata: {}", e);
            return;
        }
    };

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Ok(metadata) = fs::metadata(&tokens_path).await {
                    if let Ok(modified) = metadata.modified() {
                        if modified > last_modified {
                            match reload_token(&tokens_path).await {
                                Ok(_) => {
                                    last_modified = modified;
                                    tracing::info!("Auth token reloaded successfully");
                                }
                                Err(e) => tracing::error!("Failed to reload auth token: {}", e),
                            }
                        }
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                tracing::info!("Auth watcher cancelled");
                break;
            }
        }
    }
}

/// Reload token from file and update the ArcSwap store
async fn reload_token(path: &str) -> Result<(), String> {
    let auth_file = load_auth_file(path).await?;
    let arc_swap = TOKEN.get().ok_or("Auth token not initialized")?;
    arc_swap.store(Arc::new(auth_file.roomzin_token));
    Ok(())
}

/// For testing/debugging: Get current token info
#[cfg(test)]
pub fn debug_token() -> Option<String> {
    TOKEN.get().map(|arc_swap| {
        let token = arc_swap.load();
        format!("roomzin_token: {}", token)
    })
}
