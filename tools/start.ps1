# WataPachiBOT の起動スクリプト。タスクスケジューラのサインイン時のタスク (tools/autostart.ps1 で登録) から呼ぶ。
# - リポジトリの直下を作業ディレクトリにして bin\watapachi-bot.exe run を実行する
# - Bot が 0 以外の終了コードで終わったら (異常終了、起動時の通信の失敗など)、待ってから起動し直す
# - exe を起動できなかったとき (例外) も、異常終了と同じく待ってから起動し直す
# - 終了コード 0 (Ctrl+C などの通常の終了) では起動し直さずに終わる
# - 終了コードが「既に起動している」(別の場所の exe やサービスが動いている) なら、起動し直さずに終わる
# - 同じ exe が既に動いていれば何もせずに終わる (タスクの二重の実行に備える)
# - 外部コマンドの成否は終了コードで判定する。標準エラーへの出力では止めない
# - タスクは PowerShell 7 (pwsh) で実行する。pwsh が無い PC では Windows PowerShell 5.1 で登録されるため、5.1 でも動くように書く
# 手で起動するとき: pwsh -NoProfile -ExecutionPolicy Bypass -File tools\start.ps1

# 外部コマンドの標準エラーや、止める必要の無いコマンドレットの失敗でスクリプトを止めない。
# 止めたいコマンドレットには -ErrorAction Stop を個別に付ける
$ErrorActionPreference = 'Continue'
# PowerShell 7.3 以降で、外部コマンドの 0 以外の終了コードを例外にしない (Windows PowerShell 5.1 では効果が無い)
$PSNativeCommandUseErrorActionPreference = $false

# 実行ファイルの場所 (リポジトリの直下からの相対パス)。cargo build の出力を上書きから守るため target の外に置く
$ExeRelativePath = 'bin\watapachi-bot.exe'
# Bot が「既に起動している」で終えたときの終了コード (src/platform/single_instance.rs の EXIT_CODE_ALREADY_RUNNING と同じ値)
$ExitCodeAlreadyRunning = 3
# exe を起動できなかった (例外) ときに、終了コードの代わりに表示する値
$LaunchFailedCode = -1
# 異常終了の後、起動し直すまでの待ち時間 (秒)。続けて失敗するたびに次の値へ進み、最後の値で止まる
$RestartDelaysSeconds = @(10, 60, 300)
# この秒数以上動いてから異常終了したときは、待ち時間を最初の値に戻す
$StableRunSeconds = 600
# ウィンドウの題名
$WindowTitle = 'WataPachiBOT'

function Get-RunningBot {
	<#
	.SYNOPSIS
	指定した exe のパスで動いているプロセスを返す。
	.PARAMETER ExePath
	exe の絶対パス。大文字と小文字は区別しない。
	.OUTPUTS
	Win32_Process の配列 (無ければ空)。
	.EXAMPLE
	Get-RunningBot -ExePath 'C:\Bots\WataPachiBOT\bin\watapachi-bot.exe'
	#>
	param([string]$ExePath)
	Get-CimInstance Win32_Process -Filter "Name='watapachi-bot.exe'" -ErrorAction SilentlyContinue |
		Where-Object { $_.ExecutablePath -and [string]::Equals($_.ExecutablePath, $ExePath, [StringComparison]::OrdinalIgnoreCase) }
}

try {
	$root = Split-Path -Parent $PSScriptRoot
	Set-Location -LiteralPath $root -ErrorAction Stop
	$exe = Join-Path $root $ExeRelativePath
	try {
		$Host.UI.RawUI.WindowTitle = $WindowTitle
	} catch {
		# 窓の無いホストでは題名を変えられない。起動には影響しないため続ける
	}
	if (-not (Test-Path -LiteralPath $exe)) {
		Write-Host "実行ファイルがありません: $exe" -ForegroundColor Red
		exit 1
	}
	if (@(Get-RunningBot -ExePath $exe).Count) {
		Write-Host "WataPachiBOT は既に起動しています。" -ForegroundColor Yellow
		exit 0
	}
	$failures = 0
	while ($true) {
		$startedAt = Get-Date
		try {
			& $exe run
			$code = $LASTEXITCODE
		} catch {
			# exe が消えた、開けないなど。待ってから起動し直す (更新で置き換え中の場合がある)
			Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Bot を起動できません: $_" -ForegroundColor Red
			$code = $LaunchFailedCode
		}
		if ($code -eq 0) {
			break
		}
		if ($code -eq $ExitCodeAlreadyRunning) {
			Write-Host "別の WataPachiBOT が既に起動しているため、起動し直しません。" -ForegroundColor Yellow
			break
		}
		$ranSeconds = ((Get-Date) - $startedAt).TotalSeconds
		if ($ranSeconds -ge $StableRunSeconds) {
			$failures = 0
		}
		$delay = $RestartDelaysSeconds[[Math]::Min($failures, $RestartDelaysSeconds.Count - 1)]
		$failures++
		Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Bot が終了コード $code で終了しました。$delay 秒後に起動し直します。" -ForegroundColor Yellow
		Start-Sleep -Seconds $delay
		if (@(Get-RunningBot -ExePath $exe).Count) {
			Write-Host "待つ間に別の WataPachiBOT が起動したため、起動し直しません。" -ForegroundColor Yellow
			break
		}
	}
} catch {
	Write-Host "起動に失敗しました: $_" -ForegroundColor Red
	exit 1
}
