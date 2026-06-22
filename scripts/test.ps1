param(
    [string[]]$PytestArgs = @()
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    throw "Virtualenv Python not found: $Python. Create it with: python -m venv .venv"
}

Push-Location $RepoRoot
try {
    & $Python -m pytest @PytestArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
