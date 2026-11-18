"""
PipeInput の試験用の偽 yt-dlp。

- `python fake_ytdlp.py [任意の引数...] -- <URL>` の形で起動される
- 最後から 2 番目の引数が `--` でなければ、標準エラーへ書いて終了コード 2 で終わる
  (URL がオプションとして読まれない形で渡されていることの確認用)
- URL の問い合わせの `file=<パス>` なら、そのファイルの中身を標準出力へ書いて終わる
- URL の問い合わせの `infinite=<PID を書くファイル>` なら、PID をそのファイルへ書き、標準出力へ無限に書き続ける
- 標準エラーへ 1 行書く (ログへ流れることの確認用)
"""

import os
import sys
from typing import List
from urllib.parse import parse_qs, urlsplit

# URL の前に置かれるはずの区切り
END_OF_OPTIONS = "--"
# 中身を書くファイルを指す問い合わせのキー
FILE_KEY = "file"
# 無限モードの PID を書くファイルを指す問い合わせのキー
INFINITE_KEY = "infinite"
# 引数の形が違うときの終了コード
BAD_ARGS = 2
# 標準出力へ 1 度に書くバイト数
BLOCK = b"\x00" * 4096


def main(argv: List[str]) -> int:
	"""
	偽の yt-dlp として動く。

	引数: argv は sys.argv[1:]。最後の要素が URL、その前が `--`。
	戻り値: 終了コード。引数の形が違えば BAD_ARGS。
	例外: 指定のファイルが無ければ OSError (終了コードは 1 になる)。
	呼び出し例: `main(["--", "http://fixture.invalid/?file=C%3A%5Ca.wav"])`
	"""
	sys.stderr.write("fake_ytdlp started\n")
	sys.stderr.flush()
	if len(argv) < 2 or argv[-2] != END_OF_OPTIONS:
		sys.stderr.write("the URL must follow --\n")
		return BAD_ARGS
	query = parse_qs(urlsplit(argv[-1]).query)
	out = sys.stdout.buffer
	if INFINITE_KEY in query:
		with open(query[INFINITE_KEY][0], "w") as f:
			f.write(str(os.getpid()))
		while True:
			out.write(BLOCK)
			out.flush()
	if FILE_KEY not in query:
		sys.stderr.write("no file in the URL\n")
		return BAD_ARGS
	with open(query[FILE_KEY][0], "rb") as f:
		out.write(f.read())
	out.flush()
	return 0


if __name__ == "__main__":
	sys.exit(main(sys.argv[1:]))
