use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tokio::sync::RwLock;
use reqwest::Client;
use serde_json::{json, Value};
use futures_util::stream::StreamExt;

use crate::global_context::GlobalContext;

const BASE_URL: &str = "http://127.0.0.1:8001";
const LOCK_TOO_OLD_SEC: f64 = 600.0;


pub async fn _cthread_lock(
    reqwest_client: Client,
    worker_name: &String,
    last_known_cthread_rec: &Value,
) -> Result<Value, String> {
    let cthread_id = last_known_cthread_rec["cthread_id"].as_str().unwrap_or("");
    tracing::info!("lock {}", cthread_id);
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();

    let locked_by = last_known_cthread_rec["cthread_locked_by"].as_str().unwrap_or("");
    let locked_ts = last_known_cthread_rec["cthread_locked_ts"].as_f64().unwrap_or(0.0);
    let archived_ts = last_known_cthread_rec["cthread_archived_ts"].as_f64().unwrap_or(0.0);
    let error = last_known_cthread_rec["cthread_error"].as_str().unwrap_or("");

    if archived_ts > 0.0 {
        tracing::info!("/lock {} refuse (1)", cthread_id);
        return Err(format!("Thread {} is archived", cthread_id));
    }

    if !error.is_empty() {
        tracing::info!("/lock {} refuse (2)", cthread_id);
        return Err(format!("Thread {} has error: {}", cthread_id, error));
    }

    if !locked_by.is_empty() && locked_ts + LOCK_TOO_OLD_SEC > now {
        tracing::info!("/lock {} refuse (3)", cthread_id);
        return Err(format!("Thread {} is locked by {} and lock is not too old", cthread_id, locked_by));
    }

    let lock_update = json!({
        "cthread_id": cthread_id,
        "cthread_locked_by": &worker_name,
        "cthread_locked_ts": now,
    });
    let lock_response = reqwest_client.post(format!("{}/db_v1/cthread-update", BASE_URL)).json(&lock_update).send().await;

    match lock_response {
        Ok(response) if response.status().is_success() => {
            let response_json = response.json::<Value>().await.unwrap_or_default();
            let cthread_might_be_locked = response_json.get("cthread").cloned().unwrap_or_default();
            tracing::info!("cthread_might_be_locked {:?}", cthread_might_be_locked);
            let actually_locked_by = cthread_might_be_locked.get("cthread_locked_by").and_then(|v| v.as_str()).unwrap_or("");
            if actually_locked_by == worker_name {
                tracing::info!("/lock {} success", cthread_id);
                return Ok(cthread_might_be_locked.clone());
            }
            tracing::info!("/lock {} other worker got the lock first", cthread_id);
            Err(format!("Thread {} was locked by {} instead", cthread_id, actually_locked_by))
        }
        Ok(response) => {
            tracing::info!("/lock {} failed (4)\n{:?}", cthread_id, response);
            Err(format!("Failed to lock thread {}: HTTP {}", cthread_id, response.status()))
        }
        Err(e) => {
            tracing::info!("/lock {} failed (5)", cthread_id);
            Err(format!("Failed to lock thread {}: {}", cthread_id, e))
        }
    }
}

pub async fn _cthread_unlock(
    reqwest_client: Client,
    worker_name: &String,
    last_known_cthread_rec: &Value,
) -> Result<Value, String> {
    let cthread_id = last_known_cthread_rec["cthread_id"].as_str().unwrap_or("");
    tracing::info!("unlock {}", cthread_id);

    let locked_by = last_known_cthread_rec["cthread_locked_by"].as_str().unwrap_or("");
    if locked_by != worker_name {
        tracing::info!("/unlock {} refuse (not locked by us)", cthread_id);
        return Err(format!("Thread {} is not locked by {}", cthread_id, worker_name));
    }

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

        let cthreads = cthreads_map.read().await;
        for (cthread_id, cthread) in cthreads.iter() {
            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();

            let locked_by = cthread.get("cthread_locked_by").and_then(|v| v.as_str()).unwrap_or("");
            let locked_ts = cthread.get("cthread_locked_ts").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let archived_ts = cthread.get("cthread_archived_ts").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let error = cthread.get("cthread_error").and_then(|v| v.as_str()).unwrap_or("");

            if archived_ts > 0.0 {
                continue;
            }

            if !locked_by.is_empty() && locked_ts + LOCK_TOO_OLD_SEC > now {
                continue;
            }

            match _cthread_lock(reqwest_client.clone(), &worker_name, &cthread).await {
                Ok(locked_cthread) => {
                    tracing::info!("WHOOOO HOOO LOCKED");
                    // match fetch_cmessages(&reqwest_client, cthread_id).await {
                    //     Ok(cmessages) => {
                    //         if let Some(last_msg) = cmessages.last() {
                    //             let role = last_msg.get("role").and_then(|v| v.as_str()).unwrap_or("");
                    //             if role == "user" && error.is_empty() {
                    //                 tracing::info!("here I advance the chat for {}!", cthread_id);
                    //                 // XXX
                    //             }
                    //         }
                    //     }
                    //     Err(e) => {
                    //         tracing::error!("Failed to fetch cmessages for {}: {}", cthread_id, e);
                    //     }
                    // }

                    if let Err(e) = _cthread_unlock(reqwest_client.clone(), &worker_name, &locked_cthread).await {
                        tracing::error!("Failed to unlock {}: {}", cthread_id, e);
                    }
                }
                Err(_) => {
                    continue;
                }
            }
        }
    }
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
                    break; // Break inner loop to trigger reconnection
                }
            };

            let line_str = String::from_utf8_lossy(&line);

            if line_str.starts_with("data: ") {
                let data = &line_str[6..];
                let event: serde_json::Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("Parse error: {}", e);
                        continue; // Skip this message but keep connection
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

async fn fetch_cmessages(client: &Client, cthread_id: &str) -> Result<Vec<Value>, String> {
    let response = client
        .post(format!("{}/db_v1/cmessages-sub", BASE_URL))
        .json(&json!({"cmessage_belongs_to_cthread_id": cthread_id}))
        .send()
        .await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("Failed to fetch messages: {}", response.status()));
    }

    let mut stream = response.bytes_stream();
    let mut cmessages = Vec::new();
    let start_time = std::time::Instant::now();

    // Collect messages for up to 1 second (assuming initial data is sent quickly)
    while let Some(item) = stream.next().await {
        if start_time.elapsed() > Duration::from_secs(1) {
            break;
        }
        let line = item.map_err(|e| format!("Stream error: {}", e))?;
        let line_str = String::from_utf8_lossy(&line);

        if line_str.starts_with("data: ") {
            let data = &line_str[6..];
            let event: Value = serde_json::from_str(data).map_err(|e| format!("Parse error: {}", e))?;
            if let Some(sub_event) = event.get("sub_event").and_then(|v| v.as_str()) {
                if sub_event == "cmessage_update" {
                    if let Some(cmessage_rec) = event.get("cmessage_rec") {
                        cmessages.push(cmessage_rec.clone());
                    }
                }
            }
        }
    }

    // Sort messages by cmessage_num and cmessage_alt
    cmessages.sort_by(|a, b| {
        let num_a = a.get("cmessage_num").and_then(|v| v.as_i64()).unwrap_or(0);
        let num_b = b.get("cmessage_num").and_then(|v| v.as_i64()).unwrap_or(0);
        let alt_a = a.get("cmessage_alt").and_then(|v| v.as_i64()).unwrap_or(0);
        let alt_b = b.get("cmessage_alt").and_then(|v| v.as_i64()).unwrap_or(0);
        (num_a, alt_a).cmp(&(num_b, alt_b))
    });

    Ok(cmessages)
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
