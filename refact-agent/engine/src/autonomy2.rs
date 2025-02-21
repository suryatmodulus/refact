use std::time::{SystemTime, Duration};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use reqwest::Client;
use serde_json::{json, Value};
use futures_util::stream::StreamExt;

use crate::global_context::GlobalContext;
use crate::agent_db::db_structs::{CThread, CMessage};

const BASE_URL: &str = "http://127.0.0.1:8001";
const LOCK_TOO_OLD_SEC: f64 = 600.0;


// TODO
// 1. continue after tool calls
// 2. do the calls
// 3. replace scratchpad?


pub async fn _cthread_lock(
    reqwest_client: Client,
    worker_name: &String,
    last_known_cthread_rec: &Value,
) -> Result<(Value, Value), String> {
    let cthread_id = last_known_cthread_rec["cthread_id"].as_str().unwrap_or("");
    tracing::info!("lock {}", cthread_id);

    let lock_request = json!({
        "cthread_id": cthread_id,
        "worker_name": worker_name,
    });
    let lock_response = reqwest_client.post(format!("{}/db_v1/cthread-lock", BASE_URL)).json(&lock_request).send().await;

    match lock_response {
        Ok(response) if response.status().is_success() => {
            let response_json = response.json::<Value>().await.unwrap_or_default();
            match response_json["locked_result"].as_str() {
                Some("success") => {
                    let cthread = response_json.get("cthread").cloned().unwrap_or_default();
                    let messages = response_json.get("messages").cloned().unwrap_or_default();
                    tracing::info!("/lock {} success", cthread_id);
                    Ok((cthread, messages))
                }
                Some("busy") => {
                    tracing::info!("/lock {} refuse (busy)", cthread_id);
                    Err(format!("Thread {} is locked by another worker", cthread_id))
                }
                Some("nothing_to_do") => {
                    tracing::info!("/lock {} nothing to do", cthread_id);
                    Err(format!("Thread {} has no work to do", cthread_id))
                }
                _ => {
                    tracing::warn!("/lock {} failed (unknown result)", cthread_id);
                    Err(format!("Failed to lock thread {}: unknown result", cthread_id))
                }
            }
        }
        Ok(response) => {
            tracing::info!("/lock {} failed\n{:?}", cthread_id, response);
            Err(format!("Failed to lock thread {}: HTTP {}", cthread_id, response.status()))
        }
        Err(e) => {
            tracing::info!("/lock {} failed", cthread_id);
            Err(format!("Failed to lock thread {}: {}", cthread_id, e))
        }
    }
}

pub async fn _cthread_unlock(
    reqwest_client: Client,
    cthread_id: &String,
) -> Result<Value, String> {
    tracing::info!("unlock {}", cthread_id);

    // let cthread_id = last_known_cthread_rec["cthread_id"].as_str().unwrap_or("");
    // let locked_by = last_known_cthread_rec["cthread_locked_by"].as_str().unwrap_or("");
    // if locked_by != worker_name {
    //     tracing::info!("/unlock {} refuse (not locked by us)", cthread_id);
    //     return Err(format!("Thread {} is not locked by {}", cthread_id, worker_name));
    // }

    let unlock_update = json!({
        "cthread_id": cthread_id,
        "cthread_locked_by": "",
        "cthread_locked_ts": 0.0,
    });
    let unlock_response = reqwest_client.post(format!("{}/db_v1/cthread-update", BASE_URL)).json(&unlock_update).send().await;

    match unlock_response {
        Ok(response) if response.status().is_success() => {
            let response_json = response.json::<Value>().await.unwrap_or_default();
            let cthread_unlocked = response_json.get("cthread").cloned().unwrap_or_default();
            // tracing::info!("cthread_unlocked {:?}", cthread_unlocked);
            let actually_locked_by = cthread_unlocked.get("cthread_locked_by").and_then(|v| v.as_str()).unwrap_or("");
            if actually_locked_by.is_empty() {
                tracing::info!("/unlock {} success", cthread_id);
                return Ok(cthread_unlocked.clone());
            }
            tracing::info!("/unlock {} failed - still locked by {}", cthread_id, actually_locked_by);
            Err(format!("Thread {} is still locked by {}", cthread_id, actually_locked_by))
        }
        Ok(response) => {
            tracing::info!("/unlock {} failed\n{:?}", cthread_id, response);
            Err(format!("Failed to unlock thread {}: HTTP {}", cthread_id, response.status()))
        }
        Err(e) => {
            tracing::info!("/unlock {} failed", cthread_id);
            Err(format!("Failed to unlock thread {}: {}", cthread_id, e))
        }
    }
}


pub async fn advance_chat_thread(
    gcx: Arc<RwLock<GlobalContext>>,
    worker_n: usize,
) {
    let worker_pid = std::process::id();
    let worker_name = format!("aworker-{}-{}", worker_pid, worker_n);
    let reqwest_client = Client::new();

    let cthreads_map: Arc<RwLock<HashMap<String, Value>>> = Arc::new(RwLock::new(HashMap::new()));
    let cthreads_map_clone = cthreads_map.clone();
    let client_clone = reqwest_client.clone();
    tokio::spawn(async move {
        loop {
            match _keep_cthreads_updated(&client_clone, &cthreads_map_clone).await {
                Ok(_) => break,
                Err(e) => {
                    tracing::error!("CThread subscription error: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let my_cthread = {
            let mut cthreads = cthreads_map.write().await;
            let x = cthreads.drain().next();
            x
        };

        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();

        let (should_process, cthread_id) = if let Some((cthread_id, cthread)) = &my_cthread {
            let locked_by = cthread.get("cthread_locked_by").and_then(|v| v.as_str()).unwrap_or("");
            let locked_ts = cthread.get("cthread_locked_ts").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let archived_ts = cthread.get("cthread_archived_ts").and_then(|v| v.as_f64()).unwrap_or(0.0);
            (
                archived_ts == 0.0 && (locked_by.is_empty() || locked_ts + LOCK_TOO_OLD_SEC <= now),
                cthread_id.clone(),
            )
        } else {
            (false, "".to_string())
        };

        if should_process {
            if let Some((_, cthread)) = &my_cthread {
                match _cthread_lock(reqwest_client.clone(), &worker_name, cthread).await {
                    Ok((locked_cthread_value, messages_value)) => {
                        tracing::info!("WHOOOO HOOO LOCKED");
                        let _ = _advance_chat(
                            reqwest_client.clone(),
                            locked_cthread_value,
                            messages_value,
                        );
                        if let Err(e) = _cthread_unlock(reqwest_client.clone(), &cthread_id).await {
                            tracing::error!("Failed to unlock {}: {}", cthread_id, e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to unlock {}: {}", cthread_id, e);
                    }
                }
            }
        }
    }
}

fn _advance_chat(
    reqwest_client: Client,
    locked_cthread: Value,
    messages: Value,
) -> Result<(), String> {
    let cthread: CThread = serde_json::from_value(locked_cthread)
        .map_err(|e| format!("Failed to parse CThread: {}", e))?;
    let messages: Vec<CMessage> = serde_json::from_value(messages)
        .map_err(|e| format!("Failed to parse messages: {}", e))?;
    tracing::info!("cthread = \n{:?}", cthread);
    tracing::info!("messages = \n{:?}", messages);
    Ok(())
}

async fn _keep_cthreads_updated(
    client: &Client,
    cthreads_map: &Arc<RwLock<HashMap<String, Value>>>,
) -> Result<(), String> {
    loop {
        tracing::info!("Establishing cthreads subscription connection...");

        let response = match client
            .post(format!("{}/db_v1/cthreads-sub", BASE_URL))
            .json(&json!({"quicksearch": "", "limit": 100}))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("Failed to connect: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if !response.status().is_success() {
            tracing::error!("Subscription failed: {}", response.status());
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        let mut stream = response.bytes_stream();
        tracing::info!("Connected to cthreads subscription stream");

        while let Some(item) = stream.next().await {
            let line = match item {
                Ok(line) => line,
                Err(e) => {
                    tracing::error!("Stream error: {}", e);
                    break;
                }
            };

            let line_str = String::from_utf8_lossy(&line);

            if line_str.starts_with("data: ") {
                let data = &line_str[6..];
                let event: serde_json::Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("Parse error: {}", e);
                        continue;
                    }
                };

                let sub_event = event.get("sub_event").and_then(|v| v.as_str()).unwrap_or("");

                match sub_event {
                    "cthread_update" => {
                        if let Some(cthread_rec) = event.get("cthread_rec") {
                            let cthread_id = cthread_rec
                                .get("cthread_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            tracing::info!("cthread_update {}", cthread_id);
                            let mut map_locked = cthreads_map.write().await;
                            map_locked.insert(cthread_id, cthread_rec.clone());
                        }
                    },
                    "cthread_delete" => {
                        if let Some(cthread_id) = event.get("cthread_id").and_then(|v| v.as_str()) {
                            tracing::info!("cthread_delete {}", cthread_id);
                            let mut map_locked = cthreads_map.write().await;
                            map_locked.remove(cthread_id);
                        }
                    },
                    _ => {},
                }

            } else {
                tracing::warn!("Unknown packet: {:?}", line_str);
                break;
            }
        }

        tracing::warn!("Connection lost, attempting to reconnect...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub async fn create_advance_chat_thread(
    gcx: Arc<RwLock<GlobalContext>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for n in 0..1 {
        let handle = tokio::spawn(advance_chat_thread(gcx.clone(), n));
        handles.push(handle);
    }
    handles
}
