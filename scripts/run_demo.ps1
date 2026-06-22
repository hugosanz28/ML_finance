param(
    [switch]$SkipBootstrap,
    [int]$Port = 8501
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$DemoEnvFile = "demo/synthetic_config/.env.demo"

if (-not (Test-Path $Python)) {
    throw "Virtualenv Python not found: $Python. Create it with: python -m venv .venv"
}

$env:ML_FINANCE_ENV_FILE = $DemoEnvFile

Push-Location $RepoRoot
try {
    if (-not $SkipBootstrap) {
        & $Python scripts\bootstrap_demo.py
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    }

    & $Python -m streamlit run src\portfolio\dashboard.py --server.port $Port
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
