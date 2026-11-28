"""
WorkerPool の試験用ワーカー。

- 本物のワーカーと同じ JSON Lines で話し、resolve の URL の文字列で動作を変える
- 起動と死亡を --log の後ろに PID を付けたファイルへ追記する。試験はこれで再起動を数える
- ファイルをプロセスごとに分ける。Windows の追記は原子的でなく、共有すると行が消える
- --grandchild を付けると孫プロセスを作り、プロセスツリーの終了を確かめられる
- --exit-if-exists <path> を付けると、<path> があれば起動直後に終了し、無ければ作って動く。
  同じ起動方法の 2 台目だけを失敗させる
- --start-delay <秒> を付けると、start の行を書いた後、その秒数だけ標準入力を読まない (本物の準備の代わり)
- --request-budget-secs <秒> (プールが足す) を受け取り、記録ファイルへ budget の行を書く
- ping 以外の要求を受けるたびに、記録ファイルへ "received <PID> <URL>" の行を書く。試験はこれで
  ワーカーへ届いた要求を数える

URL と動作:
	ok                 成功を返す
	error              not_found の失敗を返す
	limited            rate_limited の失敗を返す
	slow-limited:<秒>  指定の秒数の後に rate_limited の失敗を返す
	hang               応答しない。記録ファイルへ hang の行を書く
	hang-once:<path>   <path> が無ければ作って応答しない (hang の行を書く)。あれば成功を返す
	die                応答せずに終了する
	die-once:<path>    <path> が無ければ作って終了する。あれば成功を返す
	partial-once:<path> <path> が無ければ作り、応答の途中まで (改行なし) を書いて終了する。あれば成功を返す
	garbage            JSON でない行を返す
	huge               10MB の 1 行を返す
	slow:<秒>          指定の秒数の後に成功を返す
	exit-after-reply   成功を返した直後に終了する
	freeze             成功を返した後、何も読まなくなる
	noisy              標準エラーへ 1MB と不正な UTF-8 を書いてから成功を返す
	env                環境変数 PYTHONUTF8 の値を返す

呼び出し例:
	python tests/fixtures/fake_worker.py --log worker.log --grandchild
"""

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any

HUGE_SIZE = 10 * 1024 * 1024
NOISE_SIZE = 1024 * 1024
FOREVER_SECS = 3600


def log_line(path: str | None, text: str) -> None:
	"""
	このプロセスの記録ファイルへ 1 行追記する。

	Args:
		path: 記録ファイルの接頭辞。None なら何もしない
		text: 改行を含まない本文
	Returns:
		なし
	Raises:
		OSError: 書き込みに失敗した場合
	Example:
		log_line("worker.log", "start 123 C:/python")
	"""
	if path is not None:
		with open(f"{path}.{os.getpid()}", "a", encoding="utf-8") as file:
			file.write(text + "\n")


def reply(payload: dict[str, Any]) -> None:
	"""
	応答を 1 行書いて flush する。

	Args:
		payload: 応答の辞書
	Returns:
		なし
	Raises:
		OSError: 書き込みに失敗した場合
	Example:
		reply({"id": 1, "ok": True, "result": {}})
	"""
	sys.stdout.buffer.write(json.dumps(payload).encode("utf-8") + b"\n")
	sys.stdout.buffer.flush()


def success(request_id: int, result: dict[str, Any]) -> None:
	"""
	成功の応答を書く。

	Args:
		request_id: 要求の id
		result: 結果の辞書
	Returns:
		なし
	Raises:
		OSError: 書き込みに失敗した場合
	Example:
		success(1, {"url": "ok"})
	"""
	reply({"id": request_id, "ok": True, "result": {"pid": os.getpid(), **result}})


def rate_limited(request_id: int) -> None:
	"""
	rate_limited の失敗の応答を書く。

	Args:
		request_id: 要求の id
	Returns:
		なし
	Raises:
		OSError: 書き込みに失敗した場合
	Example:
		rate_limited(1)
	"""
	reply({"id": request_id, "ok": False, "error": {"code": "rate_limited", "message": "limited"}})


def forever() -> None:
	"""応答せずに止まり続ける。Example: forever()"""
	time.sleep(FOREVER_SECS)


def handle(request: dict[str, Any], log: str | None) -> None:
	"""
	要求を 1 件処理する。

	Args:
		request: 解釈済みの要求
		log: 記録ファイル
	Returns:
		なし
	Raises:
		OSError: 書き込みに失敗した場合
	Example:
		handle({"id": 1, "op": "resolve", "url": "ok"}, None)
	"""
	request_id = request["id"]
	if request["op"] == "ping":
		success(request_id, {"prefix": sys.prefix})
		return
	url = request["url"]
	log_line(log, f"received {os.getpid()} {url}")
	if url == "ok":
		success(request_id, {"url": url})
	elif url == "error":
		reply({"id": request_id, "ok": False, "error": {"code": "not_found", "message": "none"}})
	elif url == "limited":
		rate_limited(request_id)
	elif url.startswith("slow-limited:"):
		time.sleep(float(url.split(":", 1)[1]))
		rate_limited(request_id)
	elif url == "hang":
		log_line(log, f"hang {os.getpid()}")
		forever()
	elif url.startswith("hang-once:"):
		marker = url.split(":", 1)[1]
		if not os.path.exists(marker):
			with open(marker, "w", encoding="utf-8"):
				pass
			log_line(log, f"hang {os.getpid()}")
			forever()
		success(request_id, {"url": url})
	elif url == "die":
		log_line(log, f"die {os.getpid()}")
		os._exit(1)
	elif url.startswith("die-once:"):
		marker = url.split(":", 1)[1]
		if not os.path.exists(marker):
			with open(marker, "w", encoding="utf-8"):
				pass
			log_line(log, f"die {os.getpid()}")
			os._exit(1)
		success(request_id, {"url": url})
	elif url.startswith("partial-once:"):
		marker = url.split(":", 1)[1]
		if not os.path.exists(marker):
			with open(marker, "w", encoding="utf-8"):
				pass
			log_line(log, f"die {os.getpid()}")
			sys.stdout.buffer.write(b'{"id": ' + str(request_id).encode() + b', "ok": tr')
			sys.stdout.buffer.flush()
			os._exit(1)
		success(request_id, {"url": url})
	elif url == "garbage":
		sys.stdout.buffer.write(b"this is not json\n")
		sys.stdout.buffer.flush()
	elif url == "huge":
		started = time.perf_counter()
		log_line(log, f"huge-begin {os.getpid()}")
		success(request_id, {"data": "x" * HUGE_SIZE})
		log_line(log, f"huge-written {os.getpid()} {time.perf_counter() - started:.3f}")
	elif url.startswith("slow:"):
		time.sleep(float(url.split(":", 1)[1]))
		success(request_id, {"url": url})
	elif url == "exit-after-reply":
		success(request_id, {"url": url})
		os._exit(0)
	elif url == "freeze":
		success(request_id, {"url": url})
		forever()
	elif url == "noisy":
		sys.stderr.buffer.write(b"\xff\xfe broken utf-8\n")
		sys.stderr.buffer.write(b"n" * NOISE_SIZE + b"\n")
		sys.stderr.buffer.flush()
		success(request_id, {"url": url})
	elif url == "env":
		success(request_id, {"pythonutf8": os.environ.get("PYTHONUTF8")})
	else:
		reply({"id": request_id, "ok": False, "error": {"code": "internal", "message": url}})


def main() -> None:
	"""標準入力が閉じるまで要求を処理する。Example: python fake_worker.py --log worker.log"""
	parser = argparse.ArgumentParser()
	parser.add_argument("--log", default=None)
	parser.add_argument("--grandchild", action="store_true")
	parser.add_argument("--exit-if-exists", default=None)
	parser.add_argument("--start-delay", type=float, default=0.0)
	parser.add_argument("--request-budget-secs", default=None)
	args = parser.parse_args()

	if args.exit_if_exists is not None:
		if os.path.exists(args.exit_if_exists):
			sys.exit(1)
		with open(args.exit_if_exists, "w", encoding="utf-8"):
			pass

	if args.grandchild:
		# 標準入出力を引き継がせない。引き継ぐと標準出力の EOF が孫の生存に左右される。
		# 記録ファイルを引数に付け、試験がコマンドラインで孫を見分けられるようにする
		grandchild = subprocess.Popen(
			[sys.executable, "-c", f"import time; time.sleep({FOREVER_SECS})", str(args.log)],
			stdin=subprocess.DEVNULL,
			stdout=subprocess.DEVNULL,
			stderr=subprocess.DEVNULL,
		)
		log_line(args.log, f"grandchild {grandchild.pid}")
	log_line(args.log, f"start {os.getpid()} {sys.prefix}")
	if args.request_budget_secs is not None:
		log_line(args.log, f"budget {os.getpid()} {args.request_budget_secs}")
	time.sleep(args.start_delay)

	for raw in sys.stdin.buffer:
		handle(json.loads(raw.decode("utf-8")), args.log)


if __name__ == "__main__":
	main()
