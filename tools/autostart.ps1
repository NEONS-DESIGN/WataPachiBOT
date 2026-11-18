# WataPachiBOT の自動起動 (タスクスケジューラ) の登録と解除。
# - 登録: 今の利用者のサインイン時に tools\start.ps1 を実行するタスクを作る (既にあれば置き換える)
#   ログオンの種類は Interactive (デスクトップに窓を出し、Firefox の Cookie を読めるようにする)。管理者の権限は付けない
#   優先度は 4 (通常) にする。既定の 7 は BELOW_NORMAL の優先度クラス、低い I/O 優先度とメモリ優先度で起動し、
#   子のワーカー (python、Deno) も低いクラスを引き継いで、CPU が混むとコマンドの応答と音声の処理が遅れる
#   (優先度は登録し直したときに効く。登録済みのタスクはこのスクリプトをもう一度実行して置き換える)
#   start.ps1 は PowerShell 7 (pwsh) で動かす。Store 版は利用者の WindowsApps にある実行の別名を登録する
#   (本体のパスは版ごとに変わるため使わない)。pwsh が無ければ Windows PowerShell 5.1 で登録する
#   (PowerShell を入れ替えた後も、このスクリプトをもう一度実行して登録し直す)
# - 解除: タスクを削除する。起動中の Bot は止めない (止めるときは Bot の窓で Ctrl+C か、窓を閉じる)
# 呼び出し方 (タスクの登録には管理者の端末が要る):
#   pwsh -NoProfile -ExecutionPolicy Bypass -File tools\autostart.ps1
#   pwsh -NoProfile -ExecutionPolicy Bypass -File tools\autostart.ps1 -Unregister
param(
	[switch]$Unregister
)

# タスクの名前
$TaskName = 'WataPachiBOT'
# 起動スクリプトの名前 (このスクリプトと同じフォルダ)
$StartScriptName = 'start.ps1'
# タスクの優先度 (0 が最高、10 が最低)。4 は NORMAL の優先度クラス、通常の I/O 優先度とメモリ優先度
$TaskPriority = 4
# Store 版の pwsh の実行の別名 (%LOCALAPPDATA% からの相対パス)
$StorePwshRelative = 'Microsoft\WindowsApps\pwsh.exe'
# PowerShell 7 の名前 (PATH から探す。MSI 版などの Store 版以外)
$PwshName = 'pwsh'
# pwsh が無いときに使う Windows PowerShell 5.1
$WindowsPowerShell = 'powershell.exe'

function Resolve-TaskShell {
	<#
	.SYNOPSIS
	タスクで start.ps1 を実行する PowerShell の実行ファイルを返す。
	.DESCRIPTION
	Store 版の pwsh の実行の別名 → PATH の pwsh → Windows PowerShell 5.1 の順に選ぶ。
	5.1 になったときは、その旨を表示する。
	.OUTPUTS
	実行ファイルのパス (5.1 のときは名前)。
	.EXAMPLE
	$shell = Resolve-TaskShell
	#>
	$store = Join-Path $env:LOCALAPPDATA $StorePwshRelative
	if (Test-Path -LiteralPath $store) {
		return $store
	}
	$pwsh = Get-Command $PwshName -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
	if ($pwsh) {
		return $pwsh.Source
	}
	Write-Host "pwsh が見つからないため、Windows PowerShell 5.1 で登録します。" -ForegroundColor Yellow
	return $WindowsPowerShell
}

try {
	if ($Unregister) {
		if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
			Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
			Write-Host "タスク $TaskName を削除しました。起動中の Bot は止まっていません。"
		} else {
			Write-Host "タスク $TaskName は登録されていません。"
		}
		exit 0
	}
	$root = Split-Path -Parent $PSScriptRoot
	$startScript = Join-Path $PSScriptRoot $StartScriptName
	if (-not (Test-Path -LiteralPath $startScript)) {
		throw "起動スクリプトがありません: $startScript"
	}
	# USERDOMAIN は SSH のセッションでは WORKGROUP になり、SID に解決できないため、今のプロセスの ID から取る
	$user = [Security.Principal.WindowsIdentity]::GetCurrent().Name
	$shell = Resolve-TaskShell
	$action = New-ScheduledTaskAction -Execute $shell `
		-Argument "-NoProfile -ExecutionPolicy Bypass -File `"$startScript`"" `
		-WorkingDirectory $root
	$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
	$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
	# 実行時間の上限を外す (既定の 72 時間で止められないように)。二重の実行は無視する
	$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew `
		-AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -Priority $TaskPriority
	Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal `
		-Settings $settings -Force -ErrorAction Stop | Out-Null
	Write-Host "タスク $TaskName を登録しました ($user のサインイン時に $shell で $startScript を実行、優先度 $TaskPriority)。"
} catch {
	Write-Host "失敗しました: $_" -ForegroundColor Red
	exit 1
}
