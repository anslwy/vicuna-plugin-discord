//! Vicuna Discord Bridge Plugin
//!
//! Speaks Vicuna's newline-delimited JSON-RPC bridge protocol over stdin/stdout.
//! Maintains one Discord Gateway v10 WebSocket connection per configured &
//! enabled Work session, using only the non-privileged DIRECT_MESSAGES intent
//! (`1 << 12`) so users don't need to toggle privileged intents to DM the bot.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMsg;

const MAX_MSG: usize = 1900;
const MAX_FILE: usize = 20 * 1024 * 1024;
/// Discord Gateway intent: DIRECT_MESSAGES (1 << 12). Non-privileged; delivers
/// full DM message content without enabling Message Content Intent.
const DM_INTENT: u64 = 1 << 12;

// ---------- config store ----------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeConfig {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_id: String,
    #[serde(default)]
    pub bot_username: String,
    /// Paired Discord user ID and DM channel ID
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub user_name: String,
    #[serde(default)]
    pub pair_code: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Store {
    #[serde(default)]
    pub bridges: HashMap<String, BridgeConfig>,
}

fn config_path() -> PathBuf {
    if let Ok(p) = std::env::var("VICUNA_BRIDGE_CONFIG_PATH") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".into());
    PathBuf::from(home)
        .join("vicuna")
        .join("config")
        .join("plugins")
        .join("discord.json")
}

fn load_store() -> Store {
    let p = config_path();
    std::fs::read_to_string(p)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn save_store(s: &Store) -> Result<(), String> {
    let p = config_path();
    if s.bridges.is_empty() {
        let _ = std::fs::remove_file(&p);
        bump_all();
        return Ok(());
    }
    if let Some(d) = p.parent() {
        std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
    }
    let tmp = p.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(s).unwrap_or_default())
        .map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, &p).map_err(|e| e.to_string())?;
    bump_all();
    Ok(())
}

fn new_pair_code() -> String {
    let mut b = [0u8; 4];
    let _ = getrandom::fill(&mut b);
    format!("{:06}", u32::from_le_bytes(b) % 1_000_000)
}

pub fn token_tail(t: &str) -> String {
    let n = t.chars().count();
    if n <= 4 {
        "••••".into()
    } else {
        format!("••••{}", t.chars().skip(n - 4).collect::<String>())
    }
}

pub fn invite_url(bot_id: &str) -> String {
    if bot_id.is_empty() {
        String::new()
    } else {
        // permissions=2048 (Send Messages) lets the user add the bot to a private server so they can DM it.
        format!("https://discord.com/oauth2/authorize?client_id={bot_id}&scope=bot&permissions=2048")
    }
}

pub fn is_pair_attempt(text: &str, code: &str) -> bool {
    if code.is_empty() {
        return false;
    }
    let t = text.trim();
    let t = t.strip_prefix("!pair").map(str::trim).unwrap_or(t);
    t == code
}

// ---------- runtime state ----------

#[derive(Default, Clone)]
struct BridgeRt {
    generation: u64,
    connected: bool,
    last_error: String,
    model_menu: Option<(String, Vec<String>)>,
    /// turn_id -> progress Discord message_id
    progress_msg: HashMap<String, String>,
}

#[derive(Default)]
struct Runtime {
    store_gen: u64,
    bridges: HashMap<String, BridgeRt>,
}

fn rt() -> &'static Mutex<Runtime> {
    static R: OnceLock<Mutex<Runtime>> = OnceLock::new();
    R.get_or_init(Default::default)
}

fn bump_all() {
    if let Ok(mut r) = rt().lock() {
        r.store_gen += 1;
    }
}

fn bump_session(sid: &str) {
    if let Ok(mut r) = rt().lock() {
        r.store_gen += 1;
        r.bridges.entry(sid.to_string()).or_default().generation += 1;
    }
}

fn set_error(sid: &str, e: impl Into<String>) {
    if let Ok(mut r) = rt().lock() {
        r.bridges.entry(sid.to_string()).or_default().last_error = e.into();
    }
}

fn set_connected(sid: &str, connected: bool, clear_err: bool) {
    if let Ok(mut r) = rt().lock() {
        let b = r.bridges.entry(sid.to_string()).or_default();
        b.connected = connected;
        if clear_err {
            b.last_error.clear();
        }
    }
}

// ---------- Host JSON-RPC client ----------

struct HostClient {
    tx: mpsc::UnboundedSender<String>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    next_id: AtomicU64,
}

impl HostClient {
    fn new(tx: mpsc::UnboundedSender<String>) -> Self {
        Self {
            tx,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        if let Ok(mut p) = self.pending.lock() {
            p.insert(id, tx);
        }
        let line = json!({ "id": id, "method": method, "params": params }).to_string();
        if self.tx.send(line).is_err() {
            if let Ok(mut p) = self.pending.lock() {
                p.remove(&id);
            }
            return Err("host stdout closed".into());
        }
        match tokio::time::timeout(Duration::from_secs(60), rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err("host disconnected".into()),
            Err(_) => {
                if let Ok(mut p) = self.pending.lock() {
                    p.remove(&id);
                }
                Err(format!("host timed out on {method}"))
            }
        }
    }

    fn resolve(&self, id: u64, res: Result<Value, String>) {
        if let Some(w) = self.pending.lock().ok().and_then(|mut p| p.remove(&id)) {
            let _ = w.send(res);
        }
    }
}

// ---------- Discord REST helpers ----------

fn api_base() -> String {
    std::env::var("VICUNA_DISCORD_API").unwrap_or_else(|_| "https://discord.com/api/v10".into())
}

fn http() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .user_agent("VicunaDiscordBridge (https://github.com/vicuna, 0.1.0)")
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default()
    })
}

async fn discord_req(
    method: reqwest::Method,
    token: &str,
    path: &str,
    body: Option<Value>,
) -> Result<Value, String> {
    let url = format!("{}{}", api_base(), path);
    let mut req = http()
        .request(method, &url)
        .header("Authorization", format!("Bot {token}"));
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("discord {path}: {}", e.without_url()))?;
    let status = resp.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let msg = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_else(|| format!("HTTP {status}"));
        return Err(format!("discord {path}: {msg}"));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

async fn send_message(
    token: &str,
    channel_id: &str,
    content: &str,
    components: Option<Value>,
) -> Option<String> {
    let mut body = json!({ "content": content });
    if let Some(c) = components {
        body["components"] = c;
    }
    discord_req(
        reqwest::Method::POST,
        token,
        &format!("/channels/{channel_id}/messages"),
        Some(body),
    )
    .await
    .ok()?
    .get("id")
    .and_then(Value::as_str)
    .map(str::to_string)
}

async fn edit_message(
    token: &str,
    channel_id: &str,
    msg_id: &str,
    content: &str,
    components: Option<Value>,
) {
    let mut body = json!({ "content": content });
    if let Some(c) = components {
        body["components"] = c;
    }
    let _ = discord_req(
        reqwest::Method::PATCH,
        token,
        &format!("/channels/{channel_id}/messages/{msg_id}"),
        Some(body),
    )
    .await;
}

async fn ack_interaction_update(
    token: &str,
    interaction_id: &str,
    interaction_token: &str,
    content: &str,
    components: Value,
) {
    let body = json!({
        "type": 7, // UPDATE_MESSAGE
        "data": { "content": content, "components": components }
    });
    let _ = discord_req(
        reqwest::Method::POST,
        token,
        &format!("/interactions/{interaction_id}/{interaction_token}/callback"),
        Some(body),
    )
    .await;
}

async fn ack_interaction_defer_update(
    token: &str,
    interaction_id: &str,
    interaction_token: &str,
) {
    let _ = discord_req(
        reqwest::Method::POST,
        token,
        &format!("/interactions/{interaction_id}/{interaction_token}/callback"),
        Some(json!({ "type": 6 })), // DEFERRED_UPDATE_MESSAGE
    )
    .await;
}

async fn trigger_typing(token: &str, channel_id: &str) {
    let _ = discord_req(
        reqwest::Method::POST,
        token,
        &format!("/channels/{channel_id}/typing"),
        None,
    )
    .await;
}

/// Split at line breaks into Discord-sized pieces (≤ 1900 chars).
pub fn chunks(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in text.split_inclusive('\n') {
        if cur.len() + line.len() > MAX_MSG && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        if line.len() > MAX_MSG {
            let mut s = line;
            while s.len() > MAX_MSG {
                let mut cut = MAX_MSG;
                while !s.is_char_boundary(cut) {
                    cut -= 1;
                }
                out.push(s[..cut].to_string());
                s = &s[cut..];
            }
            cur.push_str(s);
        } else {
            cur.push_str(line);
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

fn button_rows(items: Vec<(String, String)>) -> Option<Value> {
    if items.is_empty() {
        return None;
    }
    // Discord allows up to 5 ActionRows × 5 buttons = 25 buttons per message.
    let rows: Vec<Value> = items
        .into_iter()
        .take(25)
        .collect::<Vec<_>>()
        .chunks(5)
        .map(|chunk| {
            let btns: Vec<Value> = chunk
                .iter()
                .map(|(label, custom_id)| {
                    json!({
                        "type": 2, // Button
                        "style": 2, // Secondary
                        "label": label.chars().take(80).collect::<String>(),
                        "custom_id": custom_id,
                    })
                })
                .collect();
            json!({ "type": 1, "components": btns })
        })
        .collect();
    Some(Value::Array(rows))
}

// ---------- Bridge API handlers (called by Vicuna host) ----------

fn status_from_cfg(sid: &str, c: Option<&BridgeConfig>) -> Value {
    let empty = BridgeConfig::default();
    let c = c.unwrap_or(&empty);
    let (err, connected) = rt()
        .lock()
        .ok()
        .and_then(|r| r.bridges.get(sid).map(|b| (b.last_error.clone(), b.connected)))
        .unwrap_or_default();
    json!({
        "sessionId": sid,
        "configured": !c.token.is_empty(),
        "enabled": c.enabled,
        "botUsername": c.bot_username,
        "inviteUrl": if c.user_id.is_none() { invite_url(&c.bot_id) } else { String::new() },
        "tokenTail": if c.token.is_empty() { String::new() } else { token_tail(&c.token) },
        "paired": c.user_id.as_ref().map(|id| json!({ "id": id, "name": c.user_name })),
        "pairCode": if c.user_id.is_none() { c.pair_code.clone() } else { String::new() },
        "connected": connected && c.enabled && !c.token.is_empty(),
        "lastError": err,
    })
}

fn status_for(sid: &str) -> Value {
    let s = load_store();
    status_from_cfg(sid, s.bridges.get(sid))
}

fn status_all() -> Value {
    let s = load_store();
    let mut map = serde_json::Map::new();
    for (sid, cfg) in &s.bridges {
        if cfg.token.is_empty() {
            continue;
        }
        map.insert(sid.clone(), status_from_cfg(sid, Some(cfg)));
    }
    json!({ "bridges": map })
}

fn drop_for(sid: &str) {
    let mut s = load_store();
    if s.bridges.remove(sid).is_some() {
        let _ = save_store(&s);
    }
    if let Ok(mut r) = rt().lock() {
        r.store_gen += 1;
        r.bridges.remove(sid);
    }
}

async fn handle_bridge_rpc(method: &str, params: &Value) -> Result<Value, String> {
    let sid = || {
        params
            .get("sessionId")
            .or_else(|| params.get("session_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "missing sessionId".to_string())
    };

    match method {
        "bridge_list" => Ok(status_all()),
        "bridge_status" => Ok(status_for(&sid()?)),
        "bridge_save" => {
            let sid = sid()?;
            let token = params
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if token.len() < 30 || token.contains(char::is_whitespace) {
                return Err("That doesn't look like a Discord bot token. Copy it from the Discord Developer Portal → Bot → Reset Token.".into());
            }
            let mut s = load_store();
            if let Some((_, other)) = s.bridges.iter().find(|(k, v)| k.as_str() != sid && v.token == token) {
                let label = if other.bot_username.is_empty() {
                    "This bot".to_string()
                } else {
                    format!("This bot (@{})", other.bot_username)
                };
                return Err(format!(
                    "{label} is already connected to another session. Remove it there first or create a new Discord bot."
                ));
            }
            let me = discord_req(reqwest::Method::GET, &token, "/users/@me", None).await?;
            let bot_id = me.get("id").and_then(Value::as_str).unwrap_or("").to_string();
            let bot_username = me.get("username").and_then(Value::as_str).unwrap_or("").to_string();
            if bot_id.is_empty() {
                return Err("Discord returned an invalid bot user.".into());
            }
            let c = s.bridges.entry(sid.clone()).or_default();
            if c.token != token {
                c.user_id = None;
                c.channel_id = None;
                c.user_name.clear();
            }
            c.token = token;
            c.enabled = true;
            c.bot_id = bot_id;
            c.bot_username = bot_username;
            if c.user_id.is_none() {
                c.pair_code = new_pair_code();
            }
            save_store(&s)?;
            bump_session(&sid);
            set_error(&sid, "");
            Ok(status_for(&sid))
        }
        "bridge_set" => {
            let sid = sid()?;
            let mut s = load_store();
            let c = s.bridges.get_mut(&sid).ok_or("no bot token saved for this session")?;
            if let Some(e) = params.get("enabled").and_then(Value::as_bool) {
                c.enabled = e;
            }
            save_store(&s)?;
            bump_session(&sid);
            Ok(status_for(&sid))
        }
        "bridge_unpair" => {
            let sid = sid()?;
            let mut s = load_store();
            let c = s.bridges.get_mut(&sid).ok_or("no bot token saved for this session")?;
            c.user_id = None;
            c.channel_id = None;
            c.user_name.clear();
            c.pair_code = new_pair_code();
            save_store(&s)?;
            bump_session(&sid);
            Ok(status_for(&sid))
        }
        "bridge_remove" => {
            let sid = sid()?;
            drop_for(&sid);
            Ok(status_for(&sid))
        }
        _ => Err(format!("unknown bridge method {method}")),
    }
}

// ---------- Turn event mirroring (host -> Discord) ----------

async fn handle_turn_event(params: &Value) {
    let Some(sid) = params.get("sessionId").and_then(Value::as_str) else { return };
    let Some(event) = params.get("event").and_then(Value::as_str) else { return };
    let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or("").to_string();

    let Some(cfg) = load_store().bridges.remove(sid) else { return };
    if cfg.token.is_empty() || !cfg.enabled {
        return;
    }
    let Some(channel_id) = cfg.channel_id else { return };

    match event {
        "started" => {
            if let Some(mid) = send_message(&cfg.token, &channel_id, "⏳ Working…", None).await {
                if let Ok(mut r) = rt().lock() {
                    r.bridges.entry(sid.to_string()).or_default().progress_msg.insert(turn_id, mid);
                }
            }
        }
        "progress" => {
            let text = params.get("text").and_then(Value::as_str).unwrap_or("");
            if text.is_empty() {
                return;
            }
            let mid = rt()
                .lock()
                .ok()
                .and_then(|r| r.bridges.get(sid).and_then(|b| b.progress_msg.get(&turn_id).cloned()));
            if let Some(mid) = mid {
                let n = text.chars().count();
                let tail: String = text.chars().skip(n.saturating_sub(MAX_MSG - 40)).collect();
                edit_message(&cfg.token, &channel_id, &mid, &format!("⏳ {tail}"), None).await;
            }
        }
        "ask" => {
            let q = params
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("This session has a question");
            let opts: Vec<String> = params
                .get("options")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                .unwrap_or_default();
            let freeform = params.get("freeform").and_then(Value::as_bool).unwrap_or(false);
            let hint = if freeform { "\n\n(Click a choice or reply with text.)" } else { "" };
            let btns = button_rows(
                opts.iter()
                    .enumerate()
                    .map(|(i, o)| (o.clone(), format!("a:{i}")))
                    .collect(),
            );
            send_message(&cfg.token, &channel_id, &format!("❓ {q}{hint}"), btns).await;
        }
        "finished" => {
            let reply = params
                .get("reply")
                .and_then(Value::as_str)
                .unwrap_or("(no reply text)");
            let mid = rt()
                .lock()
                .ok()
                .and_then(|mut r| r.bridges.get_mut(sid).and_then(|b| b.progress_msg.remove(&turn_id)));
            let parts = chunks(reply);
            let mut it = parts.iter();
            if let (Some(m), Some(first)) = (mid, it.next()) {
                edit_message(&cfg.token, &channel_id, &m, first, None).await;
            } else if let Some(first) = parts.first() {
                send_message(&cfg.token, &channel_id, first, None).await;
            }
            for p in it {
                send_message(&cfg.token, &channel_id, p, None).await;
            }
        }
        _ => {}
    }
}

// ---------- Discord Gateway supervisor & per-session loop ----------

fn spawn_gateway_supervisor(host: Arc<HostClient>) {
    tokio::spawn(async move {
        let mut active: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
        loop {
            let s = load_store();
            let store_gen = rt().lock().map(|r| r.store_gen).unwrap_or(0);

            let to_stop: Vec<String> = active
                .keys()
                .filter(|sid| {
                    s.bridges
                        .get(sid.as_str())
                        .map(|c| c.token.is_empty() || !c.enabled)
                        .unwrap_or(true)
                })
                .cloned()
                .collect();
            for sid in to_stop {
                if let Some(h) = active.remove(&sid) {
                    h.abort();
                }
                set_connected(&sid, false, false);
            }

            for (sid, cfg) in &s.bridges {
                if cfg.token.is_empty() || !cfg.enabled {
                    continue;
                }
                let needs_spawn = match active.get(sid) {
                    Some(h) => h.is_finished(),
                    None => true,
                };
                if needs_spawn {
                    let h = tokio::spawn(run_gateway_loop(host.clone(), sid.clone()));
                    active.insert(sid.clone(), h);
                }
            }

            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if rt().lock().map(|r| r.store_gen).unwrap_or(0) != store_gen {
                    break;
                }
            }
        }
    });
}

async fn run_gateway_loop(host: Arc<HostClient>, sid: String) {
    let mut session_id_gw: Option<String> = None;
    let mut resume_url: Option<String> = None;
    let mut seq: Option<u64> = None;

    loop {
        let Some(cfg) = load_store().bridges.remove(&sid) else {
            set_connected(&sid, false, false);
            return;
        };
        if cfg.token.is_empty() || !cfg.enabled {
            set_connected(&sid, false, false);
            return;
        }
        let generation = rt()
            .lock()
            .ok()
            .and_then(|r| r.bridges.get(&sid).map(|b| b.generation))
            .unwrap_or(0);

        let gw_base = match resume_url.clone() {
            Some(u) => u,
            None => match discord_req(reqwest::Method::GET, &cfg.token, "/gateway/bot", None).await {
                Ok(v) => v
                    .get("url")
                    .and_then(Value::as_str)
                    .unwrap_or("wss://gateway.discord.gg")
                    .to_string(),
                Err(e) => {
                    set_connected(&sid, false, false);
                    set_error(&sid, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            },
        };
        let ws_url = format!("{}/?v=10&encoding=json", gw_base.trim_end_matches('/'));
        let conn = tokio_tungstenite::connect_async(&ws_url).await;
        let (ws_stream, _) = match conn {
            Ok(c) => c,
            Err(e) => {
                set_connected(&sid, false, false);
                set_error(&sid, format!("gateway connect failed: {e}"));
                resume_url = None;
                session_id_gw = None;
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<WsMsg>();

        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if ws_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let mut heartbeat_task: Option<tokio::task::JoinHandle<()>> = None;
        let seq_shared = Arc::new(AtomicU64::new(seq.unwrap_or(0)));
        let has_seq = Arc::new(std::sync::atomic::AtomicBool::new(seq.is_some()));

        let mut should_clear_resume = false;

        loop {
            let cur_gen = rt()
                .lock()
                .ok()
                .and_then(|r| r.bridges.get(&sid).map(|b| b.generation))
                .unwrap_or(0);
            if cur_gen != generation {
                break;
            }

            let next = tokio::time::timeout(Duration::from_secs(2), ws_rx.next()).await;
            let frame = match next {
                Err(_) => continue, // check generation periodically
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    set_error(&sid, format!("gateway error: {e}"));
                    break;
                }
                Ok(None) => break,
            };

            let text = match frame {
                WsMsg::Text(t) => t.to_string(),
                WsMsg::Close(c) => {
                    if let Some(cf) = c {
                        let code: u16 = cf.code.into();
                        if matches!(code, 4004 | 4010 | 4011 | 4012 | 4013 | 4014) {
                            set_error(&sid, format!("Discord closed gateway ({code}): {}", cf.reason));
                            should_clear_resume = true;
                        }
                    }
                    break;
                }
                _ => continue,
            };

            let Ok(payload) = serde_json::from_str::<Value>(&text) else { continue };
            if let Some(s) = payload.get("s").and_then(Value::as_u64) {
                seq = Some(s);
                seq_shared.store(s, Ordering::Relaxed);
                has_seq.store(true, Ordering::Relaxed);
            }
            let op = payload.get("op").and_then(Value::as_u64).unwrap_or(99);
            match op {
                10 => {
                    // Hello: start heartbeat + send IDENTIFY or RESUME
                    let interval = payload
                        .pointer("/d/heartbeat_interval")
                        .and_then(Value::as_u64)
                        .unwrap_or(41250);
                    if let Some(h) = heartbeat_task.take() {
                        h.abort();
                    }
                    let tx_hb = out_tx.clone();
                    let seq_c = seq_shared.clone();
                    let has_c = has_seq.clone();
                    heartbeat_task = Some(tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis((interval / 2).min(5000))).await;
                        loop {
                            let d = if has_c.load(Ordering::Relaxed) {
                                json!(seq_c.load(Ordering::Relaxed))
                            } else {
                                Value::Null
                            };
                            if tx_hb.send(WsMsg::Text(json!({ "op": 1, "d": d }).to_string().into())).is_err() {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(interval)).await;
                        }
                    }));

                    let init_payload = if let (Some(gsid), Some(s)) = (&session_id_gw, seq) {
                        json!({
                            "op": 6,
                            "d": { "token": cfg.token, "session_id": gsid, "seq": s }
                        })
                    } else {
                        json!({
                            "op": 2,
                            "d": {
                                "token": cfg.token,
                                "intents": DM_INTENT,
                                "properties": { "os": std::env::consts::OS, "browser": "vicuna", "device": "vicuna" }
                            }
                        })
                    };
                    let _ = out_tx.send(WsMsg::Text(init_payload.to_string().into()));
                }
                1 => {
                    // Immediate heartbeat requested
                    let d = seq.map(|s| json!(s)).unwrap_or(Value::Null);
                    let _ = out_tx.send(WsMsg::Text(json!({ "op": 1, "d": d }).to_string().into()));
                }
                7 => {
                    // Reconnect
                    break;
                }
                9 => {
                    // Invalid Session
                    should_clear_resume = true;
                    break;
                }
                0 => {
                    // Dispatch event
                    let t = payload.get("t").and_then(Value::as_str).unwrap_or("");
                    let d = payload.get("d").cloned().unwrap_or(Value::Null);
                    match t {
                        "READY" => {
                            session_id_gw = d.get("session_id").and_then(Value::as_str).map(str::to_string);
                            resume_url = d.get("resume_gateway_url").and_then(Value::as_str).map(str::to_string);
                            set_connected(&sid, true, true);
                        }
                        "RESUMED" => {
                            set_connected(&sid, true, true);
                        }
                        "MESSAGE_CREATE" => {
                            let h = host.clone();
                            let s = sid.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_discord_message(&h, &s, &d).await {
                                    set_error(&s, e);
                                }
                            });
                        }
                        "INTERACTION_CREATE" => {
                            let h = host.clone();
                            let s = sid.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_discord_interaction(&h, &s, &d).await {
                                    set_error(&s, e);
                                }
                            });
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if let Some(h) = heartbeat_task {
            h.abort();
        }
        writer.abort();
        set_connected(&sid, false, false);
        if should_clear_resume {
            session_id_gw = None;
            resume_url = None;
            seq = None;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// ---------- Inbound Discord DM & Interaction handling ----------

async fn handle_discord_interaction(
    host: &Arc<HostClient>,
    sid: &str,
    d: &Value,
) -> Result<(), String> {
    let s = load_store();
    let Some(c) = s.bridges.get(sid) else { return Ok(()) };
    let token = c.token.clone();
    let iid = d.get("id").and_then(Value::as_str).unwrap_or("");
    let itok = d.get("token").and_then(Value::as_str).unwrap_or("");
    let from = d
        .pointer("/user/id")
        .or_else(|| d.pointer("/member/user/id"))
        .and_then(Value::as_str);
    if from.is_none() || from != c.user_id.as_deref() {
        ack_interaction_defer_update(&token, iid, itok).await;
        return Ok(());
    }
    let custom_id = d.pointer("/data/custom_id").and_then(Value::as_str).unwrap_or("");
    let orig_text = d.pointer("/message/content").and_then(Value::as_str).unwrap_or("");
    let channel_id = d
        .get("channel_id")
        .and_then(Value::as_str)
        .or(c.channel_id.as_deref())
        .unwrap_or("");

    if custom_id == "sn" {
        ack_interaction_update(&token, iid, itok, orig_text, json!([])).await;
        send_now_reply(host, sid, &token, channel_id).await;
        return Ok(());
    }
    if let Some(idx_str) = custom_id.strip_prefix("a:") {
        let idx: usize = idx_str.parse().unwrap_or(usize::MAX);
        ack_interaction_defer_update(&token, iid, itok).await;
        let msg_id = d.pointer("/message/id").and_then(Value::as_str);
        match host
            .call("answer_ask", json!({ "sessionId": sid, "choice": idx }))
            .await
        {
            Ok(v) => {
                let shown = v.get("shown").and_then(Value::as_str).unwrap_or("answered");
                if let Some(mid) = msg_id {
                    edit_message(&token, channel_id, mid, &format!("{orig_text}\n\n→ {shown}"), Some(json!([]))).await;
                }
            }
            Err(e) => {
                send_message(&token, channel_id, &format!("⚠️ {e}"), None).await;
            }
        }
        return Ok(());
    }
    if let Some(eng) = custom_id.strip_prefix("e:") {
        ack_interaction_defer_update(&token, iid, itok).await;
        let (text, comps) = pick_engine(host, sid, eng).await;
        if let Some(mid) = d.pointer("/message/id").and_then(Value::as_str) {
            edit_message(&token, channel_id, mid, &text, Some(comps.unwrap_or_else(|| json!([])))).await;
        }
        return Ok(());
    }
    if let Some(idx_str) = custom_id.strip_prefix("m:") {
        ack_interaction_defer_update(&token, iid, itok).await;
        let out = pick_model(host, sid, idx_str.parse().unwrap_or(usize::MAX)).await;
        if let Some(mid) = d.pointer("/message/id").and_then(Value::as_str) {
            edit_message(&token, channel_id, mid, &out, Some(json!([]))).await;
        }
        return Ok(());
    }
    ack_interaction_defer_update(&token, iid, itok).await;
    Ok(())
}

async fn handle_discord_message(
    host: &Arc<HostClient>,
    sid: &str,
    msg: &Value,
) -> Result<(), String> {
    // Ignore bot messages (including our own) and guild messages (DMs have no guild_id)
    if msg.pointer("/author/bot").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    if msg.get("guild_id").is_some() && !msg.get("guild_id").unwrap().is_null() {
        return Ok(());
    }

    let mut s = load_store();
    let Some(c) = s.bridges.get_mut(sid) else { return Ok(()) };
    let token = c.token.clone();
    let from_id = msg.pointer("/author/id").and_then(Value::as_str).map(str::to_string);
    let channel_id = msg.get("channel_id").and_then(Value::as_str).unwrap_or("").to_string();
    let text = msg.get("content").and_then(Value::as_str).unwrap_or("").to_string();

    if c.user_id.is_none() {
        if is_pair_attempt(&text, &c.pair_code) {
            let name = msg
                .pointer("/author/global_name")
                .or_else(|| msg.pointer("/author/username"))
                .and_then(Value::as_str)
                .unwrap_or("Discord user")
                .to_string();
            c.user_id = from_id;
            c.channel_id = Some(channel_id.clone());
            c.user_name = name;
            c.pair_code.clear();
            save_store(&s)?;
            bump_session(sid);
            let info = host.call("session_info", json!({ "sessionId": sid })).await.unwrap_or(Value::Null);
            let title = info.get("title").and_then(Value::as_str).unwrap_or("this session");
            send_message(
                &token,
                &channel_id,
                &format!("✅ Paired with Vicuna. Messages you send here go to \"{title}\".\n\n`!status` · `!model` · `!send_now` · `!stop` · `!help`"),
                None,
            )
            .await;
        } else {
            send_message(
                &token,
                &channel_id,
                "This bot is private. Open Vicuna → right-click the session → Discord, and send the pairing code shown there.",
                None,
            )
            .await;
        }
        return Ok(());
    }

    if from_id.as_deref() != c.user_id.as_deref() {
        return Ok(());
    }
    // Keep channel_id synced
    if c.channel_id.as_deref() != Some(channel_id.as_str()) {
        c.channel_id = Some(channel_id.clone());
        let _ = save_store(&s);
    }

    let info = host.call("session_info", json!({ "sessionId": sid })).await?;
    if info.get("exists").and_then(Value::as_bool) != Some(true) {
        send_message(&token, &channel_id, "⚠️ The session bound to this bot no longer exists in Vicuna.", None).await;
        return Ok(());
    }
    let title = info.get("title").and_then(Value::as_str).unwrap_or("This session").to_string();
    let t = text.trim();

    if t == "!help" {
        send_message(
            &token,
            &channel_id,
            &format!("Send any message to talk to \"{title}\". Photos, files and voice messages work too.\n`!status`: what this session is doing\n`!model`: switch harness and model (`!model claude haiku`)\n`!send_now`: interrupt the running turn and send queued messages now\n`!stop`: stop the running turn"),
            None,
        )
        .await;
        return Ok(());
    }
    if t == "!status" {
        let state = if info.get("working").and_then(Value::as_bool) == Some(true) { "working" } else { "idle" };
        let engine = info.get("engine").and_then(Value::as_str).unwrap_or("?");
        let model = info.get("model").and_then(Value::as_str).unwrap_or("default");
        send_message(&token, &channel_id, &format!("\"{title}\" is {state} · {engine}/{model}"), None).await;
        return Ok(());
    }
    if t == "!stop" {
        let r = host.call("stop_turn", json!({ "sessionId": sid })).await?;
        if r.get("stopped").and_then(Value::as_bool) == Some(true) {
            send_message(&token, &channel_id, &format!("⏹ Stopping \"{title}\"…"), None).await;
        } else {
            send_message(&token, &channel_id, &format!("\"{title}\" is idle."), None).await;
        }
        return Ok(());
    }
    if t == "!send_now" {
        send_now_reply(host, sid, &token, &channel_id).await;
        return Ok(());
    }
    if t == "!model" || t.starts_with("!model ") {
        let rest: Vec<&str> = t.split_whitespace().skip(1).collect();
        let (out, comps) = match rest.as_slice() {
            [] => model_overview(host, &info).await,
            [e] => pick_engine(host, sid, e).await,
            [e, m, ..] => match host
                .call("set_model", json!({ "sessionId": sid, "engine": e, "model": m }))
                .await
            {
                Ok(v) => (v.get("message").and_then(Value::as_str).unwrap_or("Updated.").to_string(), None),
                Err(e) => (format!("⚠️ {e}"), None),
            },
        };
        send_message(&token, &channel_id, &out, comps).await;
        return Ok(());
    }

    // Free-form text reply answers a waiting ask card
    if !t.is_empty() && info.get("pendingAsk").and_then(Value::as_bool) == Some(true) {
        match host.call("answer_ask", json!({ "sessionId": sid, "text": t })).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                send_message(&token, &channel_id, &format!("⚠️ {e}"), None).await;
                return Ok(());
            }
        }
    }

    // Attachments (images, voice messages, general files)
    let mut text_out = t.to_string();
    let mut images: Vec<Value> = Vec::new();
    let mut upload_notes: Vec<String> = Vec::new();

    if let Some(atts) = msg.get("attachments").and_then(Value::as_array) {
        for att in atts {
            let Some(url) = att.get("url").and_then(Value::as_str) else { continue };
            let name = att.get("filename").and_then(Value::as_str).unwrap_or("file").to_string();
            let ctype = att.get("content_type").and_then(Value::as_str).unwrap_or("");
            let size = att.get("size").and_then(Value::as_u64).unwrap_or(0) as usize;
            if size > MAX_FILE {
                send_message(&token, &channel_id, &format!("⚠️ {name} is over 20 MB."), None).await;
                continue;
            }
            let Ok(resp) = http().get(url).send().await else {
                send_message(&token, &channel_id, &format!("⚠️ Couldn't download {name}."), None).await;
                continue;
            };
            let Ok(bytes) = resp.bytes().await else { continue };
            if bytes.len() > MAX_FILE {
                continue;
            }

            // Voice message (.ogg / audio/ogg)
            let is_voice = ctype.starts_with("audio/ogg") || name.to_ascii_lowercase().ends_with(".ogg");
            if is_voice {
                trigger_typing(&token, &channel_id).await;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                if let Ok(v) = host.call("transcribe_ogg", json!({ "base64": b64 })).await {
                    if v.get("available").and_then(Value::as_bool) == Some(true) {
                        if let Some(tx) = v.get("text").and_then(Value::as_str).map(str::trim).filter(|s| !s.is_empty()) {
                            send_message(&token, &channel_id, &format!("🎙 {tx}"), None).await;
                            text_out = if text_out.is_empty() { tx.to_string() } else { format!("{text_out}\n\n{tx}") };
                            continue;
                        }
                    }
                }
            }

            if is_image_name(&name) && bytes.len() <= 10 * 1024 * 1024 {
                images.push(json!(format!(
                    "data:{};base64,{}",
                    image_mime(&name),
                    base64::engine::general_purpose::STANDARD.encode(&bytes)
                )));
            } else {
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                match host
                    .call("stage_upload", json!({ "sessionId": sid, "name": name, "base64": b64 }))
                    .await
                {
                    Ok(v) => {
                        if let Some(note) = v.get("note").and_then(Value::as_str) {
                            upload_notes.push(note.to_string());
                        }
                    }
                    Err(e) => {
                        send_message(&token, &channel_id, &format!("⚠️ Couldn't save {name}: {e}"), None).await;
                    }
                }
            }
        }
    }

    for note in upload_notes {
        text_out.push_str(&note);
    }

    let final_text = text_out.trim();
    if final_text.is_empty() && images.is_empty() {
        return Ok(());
    }

    let res = host
        .call(
            "deliver_message",
            json!({ "sessionId": sid, "text": final_text, "images": images }),
        )
        .await;
    match res {
        Ok(v) => {
            if v.get("queued").and_then(Value::as_bool) == Some(true) {
                let btns = button_rows(vec![("⚡ Send now".into(), "sn".into())]);
                send_message(
                    &token,
                    &channel_id,
                    "📨 Queued for the running turn. Click Send now (or `!send_now`) to interrupt it and send right away.",
                    btns,
                )
                .await;
            }
        }
        Err(e) => {
            send_message(&token, &channel_id, &format!("⚠️ {e}"), None).await;
        }
    }
    Ok(())
}

async fn send_now_reply(host: &Arc<HostClient>, sid: &str, token: &str, channel_id: &str) {
    send_message(token, channel_id, "⚡ Interrupting the running turn…", None).await;
    let host = host.clone();
    let (sid, token, channel_id) = (sid.to_string(), token.to_string(), channel_id.to_string());
    tokio::spawn(async move {
        match host.call("send_now", json!({ "sessionId": sid })).await {
            Ok(v) if v.get("sent").and_then(Value::as_u64) == Some(0) => {
                send_message(&token, &channel_id, "Nothing is queued: your messages already reached the session.", None).await;
            }
            Ok(_) => {}
            Err(e) => {
                send_message(&token, &channel_id, &format!("⚠️ {e}"), None).await;
            }
        }
    });
}

async fn model_overview(host: &Arc<HostClient>, info: &Value) -> (String, Option<Value>) {
    let engine = info.get("engine").and_then(Value::as_str).unwrap_or("?");
    let model = info.get("model").and_then(Value::as_str).unwrap_or("default");
    let engines = host
        .call("list_engines", json!({}))
        .await
        .ok()
        .and_then(|v| v.get("engines").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    let items: Vec<(String, String)> = engines
        .iter()
        .filter(|e| e.get("installed") == Some(&json!(true)))
        .map(|e| {
            let id = e.get("id").and_then(Value::as_str).unwrap_or("");
            let label = e.get("label").and_then(Value::as_str).unwrap_or(id);
            let mark = if id == engine { "✓ " } else { "" };
            (format!("{mark}{label}"), format!("e:{id}"))
        })
        .collect();
    (
        format!("This session runs `{engine}/{model}`.\nPick a harness:"),
        button_rows(items),
    )
}

async fn pick_engine(host: &Arc<HostClient>, sid: &str, eng: &str) -> (String, Option<Value>) {
    let models = match host.call("list_models", json!({ "engine": eng })).await {
        Ok(v) => v
            .get("models")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect::<Vec<_>>())
            .unwrap_or_default(),
        Err(e) => return (format!("⚠️ {e}"), None),
    };
    let shown: Vec<String> = models.into_iter().take(24).collect();
    let mut items = vec![("Default".to_string(), "m:0".to_string())];
    for (i, m) in shown.iter().enumerate() {
        items.push((m.chars().take(75).collect(), format!("m:{}", i + 1)));
    }
    let mut ids = vec!["default".to_string()];
    ids.extend(shown);
    if let Ok(mut r) = rt().lock() {
        r.bridges.entry(sid.to_string()).or_default().model_menu = Some((eng.to_string(), ids));
    }
    (format!("`{eng}` models:"), button_rows(items))
}

async fn pick_model(host: &Arc<HostClient>, sid: &str, i: usize) -> String {
    let pick = rt()
        .lock()
        .ok()
        .and_then(|r| r.bridges.get(sid).and_then(|b| b.model_menu.clone()));
    let Some((engine, ids)) = pick else {
        return "⚠️ That menu expired, send `!model` again.".into();
    };
    let Some(model) = ids.get(i).cloned() else {
        return "⚠️ Unknown model.".into();
    };
    match host
        .call("set_model", json!({ "sessionId": sid, "engine": engine, "model": model }))
        .await
    {
        Ok(v) => v.get("message").and_then(Value::as_str).unwrap_or("Updated.").to_string(),
        Err(e) => format!("⚠️ {e}"),
    }
}

fn is_image_name(n: &str) -> bool {
    let l = n.to_ascii_lowercase();
    [".jpg", ".jpeg", ".png", ".webp", ".gif"].iter().any(|e| l.ends_with(e))
}

fn image_mime(n: &str) -> &'static str {
    let l = n.to_ascii_lowercase();
    if l.ends_with(".png") {
        "image/png"
    } else if l.ends_with(".webp") {
        "image/webp"
    } else if l.ends_with(".gif") {
        "image/gif"
    } else {
        "image/jpeg"
    }
}

// ---------- main stdio JSON-RPC loop ----------

#[tokio::main]
async fn main() {
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let host = Arc::new(HostClient::new(out_tx.clone()));

    // Stdout writer
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(mut line) = out_rx.recv().await {
            line.push('\n');
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    });

    spawn_gateway_supervisor(host.clone());

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        let id = v.get("id").and_then(Value::as_u64);
        let method = v.get("method").and_then(Value::as_str).map(str::to_string);

        if let (Some(req_id), Some(m)) = (id, method.clone()) {
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            let tx_c = out_tx.clone();
            tokio::spawn(async move {
                let resp = match handle_bridge_rpc(&m, &params).await {
                    Ok(res) => json!({ "id": req_id, "result": res }),
                    Err(err) => json!({ "id": req_id, "error": err }),
                };
                let _ = tx_c.send(resp.to_string());
            });
        } else if let Some(resp_id) = id {
            if let Some(err) = v.get("error").and_then(Value::as_str) {
                host.resolve(resp_id, Err(err.to_string()));
            } else {
                host.resolve(resp_id, Ok(v.get("result").cloned().unwrap_or(Value::Null)));
            }
        } else if let Some(m) = method {
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            match m.as_str() {
                "session_deleted" => {
                    if let Some(sid) = params.get("sessionId").and_then(Value::as_str) {
                        drop_for(sid);
                    }
                }
                "turn_event" => {
                    tokio::spawn(async move {
                        handle_turn_event(&params).await;
                    });
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_and_token_helpers() {
        assert!(is_pair_attempt("!pair 123456", "123456"));
        assert!(is_pair_attempt(" 123456 ", "123456"));
        assert!(!is_pair_attempt("!pair 999999", "123456"));
        assert!(!is_pair_attempt("", ""));
        assert_eq!(token_tail("abcdef1234"), "••••1234");
        assert!(invite_url("12345").contains("client_id=12345"));
    }

    #[test]
    fn chunks_respect_discord_limit() {
        let long = "a".repeat(MAX_MSG * 2 + 10);
        let p = chunks(&long);
        assert_eq!(p.len(), 3);
        assert!(p.iter().all(|s| s.len() <= MAX_MSG));
        assert_eq!(chunks("hello\nworld"), vec!["hello\nworld".to_string()]);
    }
}
