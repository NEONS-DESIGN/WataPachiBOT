//! Job Object の試験。実際に孫プロセスを作り、Job の破棄で全員が終了することを確かめる。
//! - KILL_ON_JOB_CLOSE でワーカーと子孫が Bot に紐付くことの検証を兼ねる
//! - 起動する python は support/python.rs で決める (PATH のランチャーを避けて実体を使う)

#[path = "support/python.rs"]
mod python;

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command;
use watapachi_bot::platform::job::Job;

/// 孫を作り、親と孫の PID をファイルへ書いて止まる Python のスクリプト。
const TREE_SCRIPT: &str = r#"
import os, subprocess, sys, time
grandchild = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)", sys.argv[1]])
with open(sys.argv[1] + ".tmp", "w") as file:
	file.write(f"{os.getpid()} {grandchild.pid}")
os.replace(sys.argv[1] + ".tmp", sys.argv[1])
time.sleep(600)
"#;

/// PID のプロセスが生きていて、そのコマンドラインに `marker` を含むかを確かめる。
///
/// - PID は終了後すぐ再利用されるため、コマンドラインで同じプロセスかを見分ける
///
/// # 例
/// `let alive = process_exists(1234, ".tmpAbCd").await;`
async fn process_exists(pid: u32, marker: &str) -> bool {
	let query = format!("(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine");
	let output = Command::new("powershell")
		.args(["-NoProfile", "-NonInteractive", "-Command", &query])
		.stdin(Stdio::null())
		.output()
		.await
		.expect("powershell を実行できること");
	String::from_utf8_lossy(&output.stdout).contains(marker)
}

/// ファイルが現れるまで待ち、PID の組を読む。
async fn read_pids(path: &Path) -> Vec<u32> {
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		if let Ok(text) = std::fs::read_to_string(path) {
			return text
				.split_whitespace()
				.map(|pid| pid.parse().expect("PID の数値"))
				.collect();
		}
		assert!(Instant::now() < deadline, "PID のファイルが現れない");
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
}

#[test]
fn dropping_job_kills_child_and_grandchild() {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.expect("ランタイムを作れること");
	runtime.block_on(async {
		let dir = tempfile::tempdir().expect("一時ディレクトリ");
		let pid_file = dir.path().join("pids");
		let marker = dir
			.path()
			.file_name()
			.expect("一時ディレクトリの名前")
			.to_string_lossy()
			.into_owned();
		let job = Job::new().expect("Job を作れること");
		let mut child = Command::new(python::test_python())
			.args(["-c", TREE_SCRIPT])
			.arg(&pid_file)
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.expect("python を起動できること");
		job.assign(&child).expect("Job に入れられること");

		let pids = read_pids(&pid_file).await;
		assert_eq!(pids.len(), 2);
		for pid in &pids {
			assert!(
				process_exists(*pid, &marker).await,
				"起動直後は {pid} が生きている"
			);
		}

		drop(job);

		tokio::time::timeout(Duration::from_secs(10), child.wait())
			.await
			.expect("直接の子が終了すること")
			.expect("終了の待機に成功すること");
		let deadline = Instant::now() + Duration::from_secs(10);
		for pid in &pids {
			while process_exists(*pid, &marker).await {
				assert!(Instant::now() < deadline, "{pid} が終了しない");
				tokio::time::sleep(Duration::from_millis(100)).await;
			}
		}
	});
}
