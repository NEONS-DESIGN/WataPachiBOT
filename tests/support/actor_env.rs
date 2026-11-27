//! GuildActor の試験の共通部品。偽実装と一時 DB の環境、待ちの補助、呼び出しの記録の読み取りを持つ。
//! - 試験の crate の直下で `fake_audio`、`fake_notifier`、`fake_resolver` を宣言してから、このファイルを読み込む
//! - `#[tokio::test]` は現在のスレッドで動くランタイム。試験側が譲るまでアクターは進まない
//! - 待ちは `wait_until` で条件を確かめながら、回数の上限付きで行う
//! - 時間を止めた試験では、試験側は tokio のタイマーを待たない (仮想時間を勝手に進めないため)
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::fake_audio::{AudioCall, FakeAudio};
use crate::fake_notifier::FakeNotifier;
use crate::fake_resolver::FakeResolver;
use async_trait::async_trait;
use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use watapachi_bot::audio::access::{AccessVerdict, VoiceAccess};
use watapachi_bot::audio::backend::{AudioEvent, PlayRequest, TrackId};
use watapachi_bot::config::Config;
use watapachi_bot::db::history::{self, HistoryEntry};
use watapachi_bot::db::playback::{self, PlaybackState};
use watapachi_bot::db::queue;
use watapachi_bot::db::{Database, DbError};
use watapachi_bot::domain::{
	ChannelId, GuildId, PlaylistMeta, ResolvedStream, TrackMeta, UserId, VideoId,
};
use watapachi_bot::guild::message::{CommandContext, GuildCommand, GuildMessage};
use watapachi_bot::guild::notifier::Notice;
use watapachi_bot::guild::registry::{ActorDeps, GuildHandle, GuildRegistry};

pub const GUILD: u64 = 1;
pub const TEXT: u64 = 10;
pub const VOICE: u64 = 20;
pub const USER: u64 = 30;
pub const PLAYLIST_URL: &str = "https://www.youtube.com/playlist?list=PLtest";

/// 条件を確かめる回数の上限。1 回ごとにタスクへ譲り、`POLL` だけ実時間で待つ (合計で約 5 秒)
pub const WAIT_ROUNDS: u32 = 5000;
pub const POLL: Duration = Duration::from_millis(1);
/// 1 回の待ちで譲る回数。アクターと、アクターが起動したタスクを一巡させるのに足りる数
pub const YIELDS: u32 = 16;

pub fn guild() -> GuildId {
	GuildId::new(GUILD)
}

pub fn text() -> ChannelId {
	ChannelId::new(TEXT)
}

pub fn voice() -> ChannelId {
	ChannelId::new(VOICE)
}

/// 同じ文字を 11 個並べた動画 ID。
pub fn vid(c: char) -> VideoId {
	VideoId(c.to_string().repeat(11))
}

pub fn watch_url(id: &VideoId) -> String {
	format!("https://www.youtube.com/watch?v={}", id.0)
}

pub fn title(id: &VideoId) -> String {
	format!("title-{}", id.0)
}

pub fn stream(id: &VideoId) -> ResolvedStream {
	ResolvedStream {
		video_id: id.clone(),
		title: title(id),
		duration_s: Some(10),
		url: "http://localhost/audio".to_string(),
		http_headers: BTreeMap::new(),
		filesize: 100,
		format_id: "251".to_string(),
		expires_at: None,
		available_at: None,
		is_live: false,
		protocol: "https".to_string(),
	}
}

pub fn meta(id: &VideoId) -> TrackMeta {
	TrackMeta {
		video_id: id.clone(),
		title: title(id),
		duration_s: Some(10),
		thumbnail: None,
	}
}

pub fn context(voice_channel: Option<ChannelId>) -> CommandContext {
	CommandContext {
		text_channel: text(),
		user: UserId::new(USER),
		voice_channel,
	}
}

pub fn play_message(query: &str, voice_channel: Option<ChannelId>) -> GuildMessage {
	GuildMessage::Command {
		command: GuildCommand::Play {
			query: query.to_string(),
		},
		context: context(voice_channel),
	}
}

#[derive(Default)]
struct AccessState {
	verdict: Option<AccessVerdict>,
	hang: bool,
	calls: Vec<(GuildId, ChannelId)>,
}

/// 試験用の参加の前の確認役。既定は `Allowed`。結果の指定、応答しない確認、呼び出しの記録ができる。
#[derive(Clone, Default)]
pub struct FakeAccess {
	state: Arc<Mutex<AccessState>>,
}

impl FakeAccess {
	/// 以後の確認の結果を指定する。
	pub fn set(&self, verdict: AccessVerdict) {
		self.state.lock().unwrap().verdict = Some(verdict);
	}

	/// 以後の確認を返さないようにする (上限時間の打ち切りの試験)。
	pub fn hang(&self) {
		self.state.lock().unwrap().hang = true;
	}

	/// 確認の呼び出しの記録 (ギルド、チャンネル)。
	pub fn calls(&self) -> Vec<(GuildId, ChannelId)> {
		self.state.lock().unwrap().calls.clone()
	}
}

#[async_trait]
impl VoiceAccess for FakeAccess {
	async fn check(&self, guild: GuildId, channel: ChannelId) -> AccessVerdict {
		let (verdict, hang) = {
			let mut state = self.state.lock().unwrap();
			state.calls.push((guild, channel));
			(state.verdict.clone(), state.hang)
		};
		if hang {
			std::future::pending::<()>().await;
		}
		verdict.unwrap_or(AccessVerdict::Allowed)
	}
}

/// 試験の環境。偽実装と一時 DB を持つ。
pub struct Env {
	pub registry: GuildRegistry,
	pub audio: FakeAudio,
	pub access: FakeAccess,
	pub resolver: FakeResolver,
	pub notifier: FakeNotifier,
	pub config: Arc<Config>,
	pub db: Database,
	pub db_path: PathBuf,
	_dir: TempDir,
}

impl Env {
	/// config.example.toml の既定値と、一時ディレクトリの DB で環境を作る。
	pub fn new() -> Self {
		let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		let config = Arc::new(Config::load(&root.join("config.example.toml")).unwrap());
		let dir = tempfile::tempdir().unwrap();
		let db_path = dir.path().join("test.db");
		Self::build(config, db_path, dir)
	}

	/// 全アクターを終了させる。期限は今から `shutdown.timeout_secs` 後。
	pub async fn shutdown_all(&self) {
		let limit = Duration::from_secs(self.config.shutdown.timeout_secs);
		self.registry
			.shutdown_all(tokio::time::Instant::now() + limit)
			.await;
	}

	/// 再起動を再現する。登録簿を捨て、DB を閉じてから同じファイルを開き直し、偽実装を作り直した環境を返す。
	/// - 先に `shutdown_all()` を済ませておく
	/// - 閉じる前に、書き込みスレッドに残った書き込み (再生位置の保存を含む) はすべて書かれる
	pub async fn reopen(self) -> Self {
		let Self {
			registry,
			config,
			db,
			db_path,
			_dir,
			..
		} = self;
		drop(registry);
		db.close().await.unwrap();
		Self::build(config, db_path, _dir)
	}

	/// 設定と DB のパスから環境を組み立てる。
	fn build(config: Arc<Config>, db_path: PathBuf, dir: TempDir) -> Self {
		let db = Database::open(&db_path, &config.db).unwrap();
		let audio = FakeAudio::default();
		let access = FakeAccess::default();
		let resolver = FakeResolver::default();
		let notifier = FakeNotifier::default();
		let registry = GuildRegistry::new(ActorDeps {
			audio: Arc::new(audio.clone()),
			access: Arc::new(access.clone()),
			resolver: Arc::new(resolver.clone()),
			notifier: Arc::new(notifier.clone()),
			db: db.handles.clone(),
			config: config.clone(),
		});
		Self {
			registry,
			audio,
			access,
			resolver,
			notifier,
			config,
			db,
			db_path,
			_dir: dir,
		}
	}

	pub fn handle(&self) -> GuildHandle {
		self.registry.get_or_spawn(guild())
	}

	/// ボイスチャンネルに居る利用者として `?p` を送る。
	pub fn play(&self, query: &str) {
		self.handle()
			.try_send(play_message(query, Some(voice())))
			.unwrap();
	}

	/// ボイスチャンネル `voice_channel` に居る利用者としてコマンドを送る。
	pub fn send_from(&self, command: GuildCommand, voice_channel: Option<ChannelId>) {
		self.handle()
			.try_send(GuildMessage::Command {
				command,
				context: context(voice_channel),
			})
			.unwrap();
	}

	/// ボイスチャンネルに居る利用者としてコマンドを送る。
	pub fn send(&self, command: GuildCommand) {
		self.send_from(command, Some(voice()));
	}

	/// ボイスチャンネルの聞き手の人数の変化を送る。
	pub fn listeners(&self, listeners: u32) {
		self.handle()
			.try_send(GuildMessage::VoiceMembersChanged { listeners })
			.unwrap();
	}

	/// 動画ごとに解決の結果 (成功) を指定する。
	pub fn set_videos(&self, ids: &[&VideoId]) {
		for id in ids {
			self.resolver.set_result(id, Ok(stream(id)));
		}
	}

	/// `PLAYLIST_URL` のプレイリストを指定する。各曲の解決も成功にする。
	pub fn set_playlist(&self, ids: &[&VideoId]) {
		self.set_videos(ids);
		self.resolver.set_playlist(
			PLAYLIST_URL,
			Ok(PlaylistMeta {
				title: Some("list".to_string()),
				entries: ids.iter().map(|id| meta(id)).collect(),
			}),
		);
	}

	/// 書き込みスレッドで処理を実行する (試験の前準備)。
	pub async fn write(&self, f: impl FnOnce(&Connection) -> Result<(), DbError> + Send + 'static) {
		self.db.handles.writer.run(f).await.unwrap();
	}

	/// DB を読み取り専用で開く。
	pub fn conn(&self) -> Connection {
		Connection::open_with_flags(&self.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
	}

	pub fn queue_ids(&self) -> Vec<VideoId> {
		queue::list(&self.conn(), guild(), 0, 100)
			.unwrap()
			.into_iter()
			.map(|q| q.item.video_id)
			.collect()
	}

	pub fn playback(&self) -> Vec<PlaybackState> {
		playback::load_all(&self.conn()).unwrap()
	}

	pub fn history(&self) -> Vec<HistoryEntry> {
		history::list(&self.conn(), guild(), 0, 100).unwrap()
	}

	pub fn notices(&self) -> Vec<(ChannelId, Notice)> {
		self.notifier.sent()
	}

	pub fn notice_keys(&self) -> Vec<&'static str> {
		self.notices().into_iter().map(|(_, n)| n.key).collect()
	}

	pub fn count_notice(&self, key: &str) -> usize {
		self.notice_keys().into_iter().filter(|k| *k == key).count()
	}

	pub fn has_notice(&self, key: &str) -> bool {
		self.count_notice(key) > 0
	}

	/// 指定のキーの通知のうち、最後のものの引数。
	pub fn notice_args(&self, key: &str) -> Option<Vec<(&'static str, String)>> {
		self.notices()
			.into_iter()
			.rev()
			.find(|(_, n)| n.key == key)
			.map(|(_, n)| n.args)
	}

	/// 指定のキーの通知のうち、最後のものの部品の並びの差し込み。
	pub fn notice_lists(&self, key: &str) -> Option<Vec<(&'static str, Vec<&'static str>)>> {
		self.notices()
			.into_iter()
			.rev()
			.find(|(_, n)| n.key == key)
			.map(|(_, n)| n.lists)
	}

	/// アクターがここまでのメッセージを処理し終えるまで待つ。
	/// - ボイスチャンネルに居ない `?p` を送り、その応答 (`error.not_in_voice`) を目印にする
	/// - 目印より前に起動されたタスクも一巡させる
	pub async fn barrier(&self) {
		let before = self.count_notice("error.not_in_voice");
		self.handle()
			.try_send(play_message("barrier", None))
			.unwrap();
		wait_until("目印の応答", || {
			self.count_notice("error.not_in_voice") > before
		})
		.await;
		settle().await;
	}
}

/// 他のタスクへ何度か譲る。tokio のタイマーは使わない。
pub async fn settle() {
	for _ in 0..YIELDS {
		tokio::task::yield_now().await;
	}
}

/// 条件が成り立つまで待つ。上限を超えたら失敗させる。
/// - 実時間で待つ間はランタイムのスレッドを止めるので、仮想時間は進まない
pub async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
	for _ in 0..WAIT_ROUNDS {
		if cond() {
			return;
		}
		settle().await;
		std::thread::sleep(POLL);
	}
	panic!("待ち切れません: {what}");
}

/// `play` の呼び出しの要求を順に返す。
pub fn plays(audio: &FakeAudio) -> Vec<PlayRequest> {
	audio
		.calls()
		.into_iter()
		.filter_map(|call| match call {
			AudioCall::Play(request) => Some(request),
			_ => None,
		})
		.collect()
}

/// `preload` の呼び出しの要求を順に返す。
pub fn preloads(audio: &FakeAudio) -> Vec<PlayRequest> {
	audio
		.calls()
		.into_iter()
		.filter_map(|call| match call {
			AudioCall::Preload(request) => Some(request),
			_ => None,
		})
		.collect()
}

/// 動画に割り当てられた最後の `TrackId`。偽実装は `play` と `preload` の順に 1 から払い出す。
pub fn track_for(audio: &FakeAudio, id: &VideoId) -> Option<TrackId> {
	let mut found = None;
	let mut next = 0;
	for call in audio.calls() {
		if let AudioCall::Play(request) | AudioCall::Preload(request) = call {
			next += 1;
			if request.video_id == *id {
				found = Some(TrackId(next));
			}
		}
	}
	found
}

/// 動画の再生が始まったか (`play`、または準備済みの曲の `start_preloaded`)。
pub fn started(audio: &FakeAudio, id: &VideoId) -> bool {
	let track = track_for(audio, id);
	audio.calls().into_iter().any(|call| match call {
		AudioCall::Play(request) => request.video_id == *id,
		AudioCall::StartPreloaded(t) => Some(t) == track,
		_ => false,
	})
}

pub fn disconnected(audio: &FakeAudio) -> bool {
	audio.calls().contains(&AudioCall::Disconnect)
}

/// 曲の終了を音声バックエンドから知らせる。
pub async fn end_track(env: &Env, id: &VideoId) {
	let track = track_for(&env.audio, id).unwrap();
	env.audio.emit(AudioEvent::TrackEnded { track }).await;
}

pub fn expected_request(env: &Env, id: &VideoId) -> PlayRequest {
	PlayRequest {
		video_id: id.clone(),
		stream: Some(stream(id)),
		start_at: Duration::ZERO,
		volume: env.config.defaults.volume,
		use_pipe: false,
		paused: false,
	}
}
