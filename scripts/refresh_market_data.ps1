param(
    [string]$EndDate = "",
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    throw "Virtualenv Python not found: $Python. Create it with: python -m venv .venv"
}

if ($EnvFile) {
    $env:ML_FINANCE_ENV_FILE = $EnvFile
}

$Args = @()
if ($EndDate) {
    $Args += @("--end-date", $EndDate)
}

Push-Location $RepoRoot
try {
    & $Python scripts\refresh_fx_rates.py @Args
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    & $Python scripts\refresh_market_data.py @Args
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
