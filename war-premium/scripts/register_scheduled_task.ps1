# Register a Windows Scheduled Task to run the daily pipeline (requires elevated PowerShell).
# Usage (Administrator):
#   powershell -ExecutionPolicy Bypass -File scripts\register_scheduled_task.ps1
# Edit $RunTime and $WorkingDirectory if needed.

$TaskName = "WarPremiumDailyPipeline"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$ScriptPath = Join-Path $ProjectRoot "scripts\daily_pull.ps1"
$RunTime = "9:00PM"   # local time; adjust for your timezone vs US market close

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"" `
    -WorkingDirectory $ProjectRoot

$Trigger = New-ScheduledTaskTrigger -Daily -At $RunTime
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings `
    -Description "war-premium: Polymarket + spot + options + RND + analysis (daily)"

Write-Host "Registered task '$TaskName' to run daily at $RunTime. Working directory: $ProjectRoot"
