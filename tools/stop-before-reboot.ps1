# OS の再起動の前に、WataPachiBOT に終了処理をさせるスクリプト。
# 本番機のタスク「自動再起動」の 2 番目の動作として、1 番目の `shutdown /r /t 60 /f` (60 秒後に再起動) の後に実行する。
# - 待ってから `bin\watapachi-bot.exe stop` を実行する (Bot の停止を遅らせ、止まっている時間を延ばさない)
# - さらに待ってもこのスクリプトが動いていれば、再起動が取り消された (shutdown /a) とみなし、タスク WataPachiBOT で Bot を起動し直す
# - 各段の結果を logs\stop-before-reboot.log に日時付きで追記する
# - 失敗しても例外で止まらない。再起動は 1 番目の動作で予約済みのため、このスクリプトの失敗では妨げない
# - Windows PowerShell 5.1 でも動くように書く
# 手で試すとき (待ちを短くする):
#   pwsh -NoProfile -ExecutionPolicy Bypass -File tools\stop-before-reboot.ps1 -DelaySeconds 0 -AbortWaitSeconds 3
param(
	# stop を送るまでの待ち時間 (秒)。再起動の予告 60 秒のうち、この時点で送る
	[int]$DelaySeconds = 30,
	# stop の後、再起動が取り消されたと判断するまでの待ち時間 (秒)。予告の残りより長くする
	[int]$AbortWaitSeconds = 60
)

# 外部コマンドの標準エラーや、止める必要の無いコマンドレットの失敗でスクリプトを止めない
$ErrorActionPreference = 'Continue'
# PowerShell 7.3 以降で、外部コマンドの 0 以外の終了コードを例外にしない (Windows PowerShell 5.1 では効果が無い)
$PSNativeCommandUseErrorActionPreference = $false

# 実行ファイルの場所 (リポジトリの直下からの相対パス)
$ExeRelativePath = 'bin\watapachi-bot.exe'
# ログの場所 (リポジトリの直下からの相対パス)
$LogRelativePath = 'logs\stop-before-reboot.log'
# 起動し直すときのタスクの名前 (tools\autostart.ps1 の $TaskName と同じ)
$TaskName = 'WataPachiBOT'

function Write-Log {
	<#
	.SYNOPSIS
	ログのファイルへ日時付きで 1 行を追記し、画面にも出す。
	.PARAMETER Message
	書く内容。
	.EXAMPLE
	Write-Log "stop を送ります"
	#>
	param([string]$Message)
	$line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
	Write-Host $line
	try {
		Add-Content -LiteralPath $script:LogPath -Value $line -Encoding UTF8 -ErrorAction Stop
	} catch {
		Write-Host "ログを書けません: $_" -ForegroundColor Red
	}
}

$root = Split-Path -Parent $PSScriptRoot
$LogPath = Join-Path $root $LogRelativePath
$exe = Join-Path $root $ExeRelativePath
try {
	$logDir = Split-Path -Parent $LogPath
	if (-not (Test-Path -LiteralPath $logDir)) {
		New-Item -ItemType Directory -Path $logDir -ErrorAction Stop | Out-Null
	}
} catch {
	Write-Host "ログのフォルダを作れません: $_" -ForegroundColor Red
}

Write-Log "再起動の前の停止を始めます。$DelaySeconds 秒後に stop を送ります"
Start-Sleep -Seconds $DelaySeconds

try {
	# exe の出力は、パイプへ書くとき UTF-8 になる。既定のコードページ (932 など) で読むと文字化けするため合わせる
	[Console]::OutputEncoding = [Text.Encoding]::UTF8
} catch {
	# コンソールの無いホストでは変えられない。ログの文字化けだけで停止には影響しないため続ける
}
try {
	Set-Location -LiteralPath $root -ErrorAction Stop
	$output = & $exe stop 2>&1 | ForEach-Object { "$_" }
	$code = $LASTEXITCODE
	Write-Log "stop の終了コード $code : $($output -join ' / ')"
} catch {
	Write-Log "stop を実行できません: $_"
}

Start-Sleep -Seconds $AbortWaitSeconds
Write-Log "$AbortWaitSeconds 秒待っても再起動しません。取り消されたとみなし、タスク $TaskName で起動し直します"
try {
	Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop
	Write-Log "タスク $TaskName を起動しました"
} catch {
	Write-Log "タスク $TaskName を起動できません: $_"
}
