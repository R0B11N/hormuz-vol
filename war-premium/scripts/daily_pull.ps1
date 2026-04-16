# War-premium daily automation: full ingest + RND + analysis + figures.
# Usage:  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\daily_pull.ps1
#         powershell -File scripts\daily_pull.ps1 2026-04-16
# Task Scheduler: run daily after US close (e.g. 21:00) Mon-Fri; set "Start in" to repo root.

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$DateArg = if ($args.Count -gt 0) { $args[0] } else { (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd") }

Set-Location $ProjectRoot

$gdeltArgs = @()
if (-not $env:GOOGLE_APPLICATION_CREDENTIALS) {
    $gdeltArgs += "--skip-gdelt"
}

# Single entry point (logs to logs/pipeline_YYYY-MM-DD.log)
& py -m src.pipeline.daily --date $DateArg --project-root $ProjectRoot @gdeltArgs
exit $LASTEXITCODE
