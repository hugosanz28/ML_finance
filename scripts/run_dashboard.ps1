param(
    [string]$EnvFile = "",
    [int]$Port = 8501
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

Push-Location $RepoRoot
try {
    & $Python -m streamlit run src\portfolio\dashboard.py --server.port $Port
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
