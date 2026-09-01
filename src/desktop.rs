use crate::models::{NativeAudioProgressCheckpoint, NativeAudioState};
use parking_lot::Mutex;
use rodio::{Decoder, OutputStream, OutputStreamHandle, Sink, Source};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, Manager, Runtime};

// rodio::OutputStream is !Send (!Sync) due to *mut (), but we only keep it alive
// and access via OutputStreamHandle (which is Send). Safe to mark Send.
struct SendOutputStream(OutputStream);
unsafe impl Send for SendOutputStream {}
unsafe impl Sync for SendOutputStream {}

//

// Keep OutputStream alive globally - it must outlive Sink
struct DesktopInner {
    _stream: Option<SendOutputStream>,
    handle: Option<OutputStreamHandle>,
    sink: Option<Sink>,
    state: PlaybackStateMachine,
    current_src: Option<String>,
    duration: f64,
    // checkpoint file throttling
    last_persist_ms: u128,
    last_persist_time: f64,
    last_persist_id: Option<i64>,
}

impl DesktopInner {
    fn new() -> Self {
        Self {
            _stream: None,
            handle: None,
            sink: None,
            state: PlaybackStateMachine::default(),
            current_src: None,
            duration: 0.0,
            last_persist_ms: 0,
            last_persist_time: f64::NAN,
            last_persist_id: None,
        }
    }

    fn ensure_stream(&mut self) {
        if self._stream.is_none() {
            if let Ok((stream, handle)) = OutputStream::try_default() {
                self._stream = Some(SendOutputStream(stream));
                self.handle = Some(handle);
            }
        }
    }

    fn snapshot(&mut self) -> NativeAudioState {
        let is_playing = self
            .sink
            .as_ref()
            .map(|s| !s.is_paused() && !s.empty())
            .unwrap_or(false);
        let is_empty = self.sink.as_ref().map(|s| s.empty()).unwrap_or(true);
        // rodio Sink doesn't expose duration/position directly for generic sources
        // we track duration on set_source and currentTime via sink position approximation
        // For file sources we can query, for now use state
        let buffering = false;
        let has_error = self.state.last_error.is_some();
        let ended = self.state.did_reach_end;

        let status = if has_error {
            "error"
        } else if ended {
            "ended"
        } else if is_playing {
            "playing"
        } else if buffering {
            "loading"
        } else {
            "idle"
        };

        let current_time = self.state.effective_time(is_playing);

        let duration = self.duration;

        NativeAudioState {
            status: status.to_string(),
            current_time,
            duration,
            is_playing: !has_error && !ended && is_playing,
            buffering: !has_error && !ended && buffering,
            rate: self.state.rate,
            error: self.state.last_error.clone(),
        }
    }
}

// Simplified port of PlaybackStateMachine from iOS/Swift
struct PlaybackStateMachine {
    rate: f64,
    last_error: Option<String>,
    did_reach_end: bool,
    current_id: Option<i64>,
    pending_seek: Option<f64>,
    stable_time: f64,
    desired_playing: bool,
}

impl Default for PlaybackStateMachine {
    fn default() -> Self {
        Self {
            rate: 1.0,
            last_error: None,
            did_reach_end: false,
            current_id: None,
            pending_seek: None,
            stable_time: 0.0,
            desired_playing: false,
        }
    }
}

impl PlaybackStateMachine {
    fn effective_time(&self, is_playing: bool) -> f64 {
        if let Some(target) = self.pending_seek {
            return target;
        }
        self.stable_time
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

static INNER: OnceLock<Arc<Mutex<DesktopInner>>> = OnceLock::new();

fn inner() -> Arc<Mutex<DesktopInner>> {
    INNER
        .get_or_init(|| Arc::new(Mutex::new(DesktopInner::new())))
        .clone()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn checkpoint_path<R: Runtime>(app: &AppHandle<R>) -> PathBuf {
    let base = dirs::data_local_dir()
        .or_else(|| app.path().app_data_dir().ok())
        .unwrap_or_else(|| std::env::temp_dir());
    // ensure dir exists
    let _ = std::fs::create_dir_all(&base);
    base.join("tauri_native_audio_progress.json")
}

fn read_checkpoint<R: Runtime>(app: &AppHandle<R>) -> Option<NativeAudioProgressCheckpoint> {
    let path = checkpoint_path(app);
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

fn write_checkpoint<R: Runtime>(
    app: &AppHandle<R>,
    inner: &mut DesktopInner,
    state: &NativeAudioState,
    force: bool,
) {
    let id = match inner.state.current_id {
        Some(id) if id > 0 => id,
        _ => return,
    };
    if !state.current_time.is_finite() || state.current_time <= 0.25 {
        return;
    }
    let now = now_ms();
    if !force && now - inner.last_persist_ms < 1000 {
        return;
    }
    if !force
        && inner.last_persist_id == Some(id)
        && (inner.last_persist_time - state.current_time).abs() <= 0.05
    {
        return;
    }
    let cp = NativeAudioProgressCheckpoint {
        id,
        current_time: state.current_time,
        updated_at_ms: now as i64,
        status: Some(state.status.clone()),
    };
    let path = checkpoint_path(app);
    if let Ok(json) = serde_json::to_string(&cp) {
        let _ = std::fs::write(path, json);
        inner.last_persist_ms = now;
        inner.last_persist_time = state.current_time;
        inner.last_persist_id = Some(id);
    }
}

fn safe_duration(secs: f64) -> Result<std::time::Duration, String> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("invalid duration: {secs}"));
    }
    // rodio/cpal panics if secs is NaN or too big (Duration::MAX ~ 584 years)
    // clamp to 24h to be safe - no audio is longer
    let clamped = secs.min(86400.0 * 7.0);
    // Duration::from_secs_f64 still checks is_finite, so safe now
    Ok(std::time::Duration::from_secs_f64(clamped))
}

fn emit_state<R: Runtime>(app: &AppHandle<R>, state: NativeAudioState) {
    // для addPluginListener("native-audio", "native_audio_state", ...) нужен plugin://
    let _ = app.emit("plugin:native-audio://native_audio_state", &state);
    // fallback для listen("native_audio_state")
    let _ = app.emit("native_audio_state", &state);
    if state.is_playing {
        eprintln!(
            "[native-audio] emit playing {:.1}/{:.1}",
            state.current_time, state.duration
        );
    }
}

fn resolve_src(src: &str, app: &AppHandle<impl Runtime>) -> Result<PathBuf, String> {
    let s = src.trim();
    if s.is_empty() {
        return Err("src is required".into());
    }
    // file://
    if let Some(stripped) = s.strip_prefix("file://") {
        return Ok(PathBuf::from(stripped));
    }
    // asset://localhost or http://asset.localhost
    if s.starts_with("asset://") || s.contains("asset.localhost") {
        // try to extract path after host
        if let Some(idx) = s.find("://") {
            let after = &s[idx + 3..];
            if let Some(slash) = after.find('/') {
                let path = &after[slash..];
                // Try app resource dir
                if let Ok(resolved) = app.path().resolve(
                    path.trim_start_matches('/'),
                    tauri::path::BaseDirectory::Resource,
                ) {
                    if resolved.exists() {
                        return Ok(resolved);
                    }
                }
                return Ok(PathBuf::from(path));
            }
        }
    }
    // plain file path
    let p = PathBuf::from(s);
    if p.exists() {
        return Ok(p);
    }
    // remote https:// - stream via http download to cache (like Android ExoPlayer does for progressive)
    if s.starts_with("http://") || s.starts_with("https://") {
        // HLS/DASH not supported by rodio - reject early with clear message
        if s.contains(".m3u8") || s.contains(".mpd") {
            return Err("HLS/DASH (.m3u8/.mpd) not supported on desktop rodio backend, use progressive mp3/mp4/ogg/wav".into());
        }
        return download_to_cache(s, app);
    }
    Err(format!("file not found: {}", s))
}

fn download_to_cache<R: Runtime>(url: &str, app: &AppHandle<R>) -> Result<PathBuf, String> {
    use sha2::{Digest, Sha256};
    use std::io::Write;

    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let hash = hex::encode(hasher.finalize());
    // ext из URL, fallback .mp3 - универсально (не только mp3-128), rodio детектит по содержимому
    let url_ext = url
        .split('?')
        .next()
        .unwrap_or(url)
        .rsplit('.')
        .next()
        .filter(|e| e.len() <= 5 && e.chars().all(|c| c.is_ascii_alphanumeric()))
        .map(|e| format!(".{}", e.to_lowercase()))
        .filter(|e| matches!(e.as_str(), ".mp3" | ".ogg" | ".flac" | ".wav" | ".m4a" | ".aac" | ".opus" | ".mp4"))
        .unwrap_or_else(|| ".mp3".to_string());

    let cache_dir = dirs::cache_dir()
        .or_else(|| app.path().app_cache_dir().ok())
        .unwrap_or_else(|| std::env::temp_dir().join("tauri-native-audio"));
    let _ = std::fs::create_dir_all(&cache_dir);
    let mut cached_path = cache_dir.join(format!("{hash}{url_ext}"));

    // reuse if already cached and < 24h
    if let Ok(meta) = std::fs::metadata(&cached_path) {
        if let Ok(modified) = meta.modified() {
            if let Ok(elapsed) = modified.elapsed() {
                if elapsed.as_secs() < 24 * 3600 && meta.len() > 0 {
                    return Ok(cached_path);
                }
            }
        }
    }

    // download (blocking) - with timeout 30s
    let resp = ureq::get(url)
        .timeout(std::time::Duration::from_secs(30))
        .call()
        .map_err(|e| format!("http fetch failed: {e}"))?;

    if resp.status() != 200 {
        return Err(format!("http status {}", resp.status()));
    }

    // уточнить ext по Content-Type если URL без расширения
    if url_ext == ".mp3" {
        if let Some(ct) = resp.header("content-type") {
            let ct = ct.to_lowercase();
            let ct_ext = if ct.contains("ogg") || ct.contains("opus") {
                ".ogg"
            } else if ct.contains("flac") {
                ".flac"
            } else if ct.contains("wav") {
                ".wav"
            } else if ct.contains("mp4") || ct.contains("m4a") || ct.contains("aac") {
                ".m4a"
            } else {
                ""
            };
            if !ct_ext.is_empty() && ct_ext != url_ext {
                cached_path = cache_dir.join(format!("{hash}{ct_ext}"));
                // если уже закэширован с новым ext - вернуть
                if let Ok(meta) = std::fs::metadata(&cached_path) {
                    if meta.len() > 0 {
                        if let Ok(modified) = meta.modified() {
                            if let Ok(elapsed) = modified.elapsed() {
                                if elapsed.as_secs() < 24 * 3600 {
                                    return Ok(cached_path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut reader = resp.into_reader();
    let mut tmp = cached_path.clone();
    tmp.set_extension("tmp");
    let mut file = File::create(&tmp).map_err(|e| e.to_string())?;
    std::io::copy(&mut reader, &mut file).map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())?;
    drop(file);
    let _ = std::fs::rename(&tmp, &cached_path);
    Ok(cached_path)
}

fn build_sink(
    handle: &OutputStreamHandle,
    path: &PathBuf,
    rate: f32,
) -> Result<(Sink, f64), String> {
    eprintln!("[native-audio] build_sink path={:?} rate={}", path, rate);
    let file = File::open(path).map_err(|e| {
        eprintln!("[native-audio] open failed: {}", e);
        e.to_string()
    })?;
    let decoder = Decoder::new(BufReader::new(file)).map_err(|e| {
        eprintln!("[native-audio] decoder failed: {}", e);
        e.to_string()
    })?;
    // duration estimate: total_duration() if available - guard NaN/Inf
    let raw_duration = decoder.total_duration().map(|d| d.as_secs_f64()).unwrap_or(0.0);
    let duration = if raw_duration.is_finite() && raw_duration >= 0.0 {
        raw_duration.min(86400.0 * 7.0)
    } else {
        0.0
    };
    eprintln!("[native-audio] decoder ok duration={}", duration);
    if !rate.is_finite() || rate <= 0.0 || rate > 16.0 {
        return Err(format!("invalid rate {rate}"));
    }
    let sink = Sink::try_new(handle).map_err(|e| {
        eprintln!("[native-audio] Sink::try_new failed: {}", e);
        e.to_string()
    })?;
    sink.set_speed(rate);
    sink.append(decoder);
    sink.pause(); // start paused, play() will resume
    eprintln!("[native-audio] sink created, empty={}", sink.empty());
    Ok((sink, duration))
}

// Commands

#[tauri::command]
pub fn initialize<R: Runtime>(app: AppHandle<R>) -> NativeAudioState {
    eprintln!("[native-audio] initialize");
    let arc = inner();
    let mut g = arc.lock();
    g.ensure_stream();
    if g.handle.is_none() {
        eprintln!("[native-audio] initialize: no audio device!");
        g.state.last_error = Some("no audio device".into());
    } else {
        eprintln!("[native-audio] initialize: device ok");
        g.state.last_error = None;
    }
    let s = g.snapshot();
    eprintln!("[native-audio] initialize -> {:?}", s);
    emit_state(&app, s.clone());
    s
}

#[tauri::command]
pub fn set_source<R: Runtime>(
    app: AppHandle<R>,
    src: String,
    id: Option<i64>,
    title: Option<String>,
    artist: Option<String>,
    artwork_url: Option<String>,
) -> Result<NativeAudioState, String> {
    eprintln!("[native-audio] set_source src={} id={:?}", src, id);
    let _ = (title, artist, artwork_url); // metadata not used on desktop yet (could integrate with muda)
    if src.trim().is_empty() {
        return Err("src is required".into());
    }
    // мгновенно стопаем старый трек чтобы не доигрывал во время буферизации нового (как на android/ios)
    let arc = inner();
    {
        let mut g = arc.lock();
        if let Some(old) = g.sink.take() {
            eprintln!("[native-audio] set_source: stopping previous sink");
            old.stop();
        }
        g.current_src = Some(src.clone());
        g.state.current_id = match id {
            Some(v) if v > 0 => Some(v),
            _ => None,
        };
        g.state.stable_time = 0.0;
        g.state.pending_seek = None;
        g.state.did_reach_end = false;
        g.state.last_error = None;
        g.state.desired_playing = false;
        g.duration = 0.0;
        // сразу эмитим idle/loading чтобы фронт показал буферизацию
        let s = g.snapshot();
        eprintln!("[native-audio] set_source: stopped old, now loading");
        emit_state(&app, s);
    }
    // теперь качаем/резолвим новый src без удержания lock (не блокируем play/pause)
    let path = resolve_src(&src, &app).map_err(|e| {
        eprintln!("[native-audio] resolve_src failed: {}", e);
        let mut g = arc.lock();
        g.state.last_error = Some(e.clone());
        let s = g.snapshot();
        emit_state(&app, s);
        e
    })?;
    eprintln!("[native-audio] resolve_src -> {:?}", path);
    let mut g = arc.lock();
    let rate = g.state.rate as f32;
    g.ensure_stream();
    let handle = g.handle.clone().ok_or("no audio device")?;

    match build_sink(&handle, &path, rate) {
        Ok((sink, duration)) => {
            eprintln!("[native-audio] set_source ok duration={}", duration);
            g.duration = duration;
            g.sink = Some(sink);
        }
        Err(e) => {
            eprintln!("[native-audio] set_source build_sink failed: {}", e);
            g.state.last_error = Some(e.clone());
            let s = g.snapshot();
            emit_state(&app, s.clone());
            return Err(e);
        }
    }
    let s = g.snapshot();
    eprintln!("[native-audio] set_source -> {:?}", s);
    emit_state(&app, s.clone());
    Ok(s)
}

#[tauri::command]
pub fn play<R: Runtime>(app: AppHandle<R>) -> Result<NativeAudioState, String> {
    eprintln!("[native-audio] play");
    let arc = inner();
    let mut g = arc.lock();
    if g.sink.is_none() {
        eprintln!("[native-audio] play: no source set");
        return Err("no source set".into());
    }
    eprintln!(
        "[native-audio] play: sink empty={} paused={} duration={} stable={}",
        g.sink.as_ref().map(|s| s.empty()).unwrap_or(true),
        g.sink.as_ref().map(|s| s.is_paused()).unwrap_or(true),
        g.duration,
        g.state.stable_time
    );
    if g.state.did_reach_end {
        // seek to 0
        if let Some(sink) = &g.sink {
            let _ = sink.try_seek(std::time::Duration::from_secs(0));
        }
        g.state.did_reach_end = false;
        g.state.stable_time = 0.0;
    }
    let pending = g.state.pending_seek.take();
    if let Some(sink) = g.sink.as_ref() {
        sink.play();
        if let Some(target) = pending {
            if let Ok(d) = safe_duration(target.max(0.0)) {
                let _ = sink.try_seek(d);
            }
            if target.is_finite() && target >= 0.0 {
                g.state.stable_time = target.min(86400.0 * 7.0);
            } else {
                g.state.stable_time = 0.0;
            }
        }
    } else if let Some(target) = pending {
        if target.is_finite() && target >= 0.0 {
            g.state.stable_time = target.min(86400.0 * 7.0);
        }
    }
    g.state.desired_playing = true;
    g.state.last_error = None;

    // poll ended in background - эмулируем timeupdate как на html5 (200ms)
    let app_clone = app.clone();
    let inner_clone = arc.clone();
    std::thread::spawn(move || {
        // simple ended detection: poll until empty
        let mut tick: u64 = 0;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(200));
            tick += 1;
            let (empty, _id, _time) = {
                let mut g = inner_clone.lock();
                let empty = g.sink.as_ref().map(|s| s.empty()).unwrap_or(true);
                // update stable_time from sink position approximation
                // rodio sink doesn't give position, so we increment manually when playing
                if !empty && g.state.desired_playing {
                    if !g.state.stable_time.is_finite() {
                        g.state.stable_time = 0.0;
                    }
                    g.state.stable_time = (g.state.stable_time + 0.2).min(86400.0 * 7.0);
                    if g.duration.is_finite()
                        && g.duration > 0.0
                        && g.state.stable_time >= g.duration
                    {
                        g.state.stable_time = g.duration;
                        g.state.did_reach_end = true;
                        g.state.desired_playing = false;
                    }
                }
                let s2 = g.snapshot();
                if tick % 5 == 0 {
                    eprintln!(
                        "[native-audio] tick {} -> time={:.1}/{:.1} playing={} empty={}",
                        tick, s2.current_time, s2.duration, s2.is_playing, empty
                    );
                }
                write_checkpoint(&app_clone, &mut g, &s2, s2.status == "ended");
                emit_state(&app_clone, s2.clone());
                (empty, g.state.current_id, g.state.stable_time)
            };
            if empty {
                let mut g = inner_clone.lock();
                if !g.state.did_reach_end && g.state.desired_playing {
                    g.state.did_reach_end = true;
                    g.state.desired_playing = false;
                    let s = g.snapshot();
                    write_checkpoint(&app_clone, &mut g, &s, true);
                    emit_state(&app_clone, s);
                }
                break;
            }
            // stop polling if paused/disposed
            {
                let g = inner_clone.lock();
                if !g.state.desired_playing {
                    break;
                }
            }
        }
    });

    let s = g.snapshot();
    eprintln!("[native-audio] play -> {:?}", s);
    // throttled persist handled in poll thread, but also emit
    emit_state(&app, s.clone());
    Ok(s)
}

#[tauri::command]
pub fn pause<R: Runtime>(app: AppHandle<R>) -> Result<NativeAudioState, String> {
    eprintln!("[native-audio] pause");
    let arc = inner();
    let mut g = arc.lock();
    if let Some(sink) = &g.sink {
        sink.pause();
    }
    g.state.desired_playing = false;
    g.state.pending_seek = None;
    let s = g.snapshot();
    write_checkpoint(&app, &mut g, &s, true);
    emit_state(&app, s.clone());
    Ok(s)
}

#[tauri::command]
pub fn seek_to<R: Runtime>(app: AppHandle<R>, position: f64) -> Result<NativeAudioState, String> {
    if !position.is_finite() {
        return Err("position is required".into());
    }
    let target_raw = position.max(0.0);
    // clamp target to safe range before any Duration conversion
    let target = if target_raw.is_finite() {
        target_raw.min(86400.0 * 7.0)
    } else {
        return Err("position is required".into());
    };
    let dur = safe_duration(target).map_err(|e| e.to_string())?;
    let arc = inner();
    let mut g = arc.lock();
    if g.sink.is_none() {
        return Err("no source set".into());
    }
    let should_resume = g.state.desired_playing;
    let seek_res = g.sink.as_ref().unwrap().try_seek(dur);
    if seek_res.is_err() {
        g.state.pending_seek = Some(target);
        g.state.stable_time = target;
    } else {
        g.state.stable_time = target;
        g.state.pending_seek = None;
    }
    if !should_resume {
        if let Some(sink) = g.sink.as_ref() {
            sink.pause();
        }
    }
    g.state.did_reach_end = false;
    let s = g.snapshot();
    write_checkpoint(&app, &mut g, &s, true);
    emit_state(&app, s.clone());
    Ok(s)
}

#[tauri::command]
pub fn set_rate<R: Runtime>(app: AppHandle<R>, rate: f64) -> Result<NativeAudioState, String> {
    if !rate.is_finite() || rate <= 0.0 {
        return Err("rate must be > 0".into());
    }
    let arc = inner();
    let mut g = arc.lock();
    g.state.rate = rate;
    if let Some(sink) = &g.sink {
        sink.set_speed(rate as f32);
    }
    let s = g.snapshot();
    emit_state(&app, s.clone());
    Ok(s)
}

#[tauri::command]
pub fn get_state<R: Runtime>(_app: AppHandle<R>) -> NativeAudioState {
    let arc = inner();
    let mut g = arc.lock();
    g.snapshot()
}

#[tauri::command]
pub fn get_progress_checkpoint<R: Runtime>(
    app: AppHandle<R>,
) -> Option<NativeAudioProgressCheckpoint> {
    read_checkpoint(&app)
}

#[tauri::command]
pub fn clear_progress_checkpoint<R: Runtime>(app: AppHandle<R>) {
    let path = checkpoint_path(&app);
    let _ = std::fs::remove_file(path);
    let arc = inner();
    let mut g = arc.lock();
    g.last_persist_ms = 0;
    g.last_persist_time = f64::NAN;
    g.last_persist_id = None;
}

#[tauri::command]
pub fn dispose<R: Runtime>(app: AppHandle<R>) {
    let arc = inner();
    let mut g = arc.lock();
    let s = g.snapshot();
    write_checkpoint(&app, &mut g, &s, true);
    if let Some(sink) = g.sink.take() {
        sink.stop();
    }
    g.duration = 0.0;
    g.current_src = None;
    g.state.reset();
    g.state.stable_time = 0.0;
    emit_state(&app, g.snapshot());
}

#[tauri::command]
pub fn register_listener<R: Runtime>(_app: AppHandle<R>) {}
#[tauri::command]
pub fn remove_listener<R: Runtime>(_app: AppHandle<R>) {}
