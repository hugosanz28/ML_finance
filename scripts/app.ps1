<#
.SYNOPSIS
Abre ML Finance local, consulta su estado o gestiona copias privadas.
.EXAMPLE
.\scripts\app.ps1
.EXAMPLE
.\scripts\app.ps1 -Action Backup
#>
[CmdletBinding()]
param(
    [ValidateSet('Start', 'Stop', 'Status', 'Backup', 'Restore')]
    [string]$Action = 'Start',
    [string]$Archive,
    [string]$NodePath,
    [switch]$ConfirmRestore,
    [switch]$NoBrowser,
    [switch]$NoRefresh
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$python = Join-Path $repoRoot '.venv/Scripts/python.exe'
$dataRoot = Join-Path $repoRoot 'src/data/local'

function Get-Listener([int]$Port) {
    @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique)
}

function Test-OurProcess([int]$ProcessId, [string]$Kind) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId"
    if (-not $process) { return $false }
    $command = ([string]$process.CommandLine).Replace('\', '/')
    $rootText = $repoRoot.Replace('\', '/')
    if ($Kind -eq 'UI') {
        return ($process.Name -eq 'node.exe' -and $command.Contains($rootText) -and $command -match '/vite/')
    }
    if ($command -notmatch 'run_api\.py') { return $false }
    if ($command.Contains($rootText)) { return $true }
    # Windows venv Python delegates to its base interpreter; verify its parent too.
    $parent = Get-CimInstance Win32_Process -Filter "ProcessId = $($process.ParentProcessId)"
    return ($parent -and $parent.ExecutablePath -eq $python)
}

function Test-Ready([string]$Kind) {
    try {
        if ($Kind -eq 'API') {
            $health = Invoke-RestMethod 'http://127.0.0.1:8000/api/v1/health' -TimeoutSec 10
            return ($health.status -eq 'ok' -and $health.api_version -eq 'v1' -and
                $health.mode -eq 'operations' -and $health.workspace_mode -eq 'real')
        }
        $page = Invoke-WebRequest 'http://127.0.0.1:5173/' -UseBasicParsing -TimeoutSec 2
        return ($page.Content -match '<title>ML Finance')
    } catch { return $false }
}

function Get-ServiceState([string]$Kind, [int]$Port) {
    $listeners = @(Get-Listener $Port)
    if (-not $listeners.Count) { return 'stopped' }
    foreach ($listener in $listeners) {
        if (-not (Test-OurProcess $listener $Kind)) { return 'foreign' }
    }
    if (Test-Ready $Kind) { return 'ready' }
    return 'unready'
}

function Start-Hidden([string]$Executable, [string[]]$Arguments, [string]$Directory, [string]$LogName) {
    # Start-Process does not preserve array quoting; quote each path for the native process.
    $quoted = ($Arguments | ForEach-Object { '"' + $_ + '"' }) -join ' '
    Start-Process -FilePath $Executable -ArgumentList $quoted -WorkingDirectory $Directory -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $dataRoot "$LogName.stdout.log") `
        -RedirectStandardError (Join-Path $dataRoot "$LogName.stderr.log") -PassThru
}

function Wait-Ready([string]$Kind, [int]$Port) {
    $deadline = [DateTime]::UtcNow.AddSeconds(45)
    do {
        if ((Get-ServiceState $Kind $Port) -eq 'ready') { return }
        Start-Sleep -Milliseconds 300
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$Kind no está listo. Revisa los logs app-* en src/data/local/."
}

try {
    if ($env:OS -ne 'Windows_NT') { throw 'Este lanzador requiere Windows. Consulta los comandos manuales en README.md.' }
    if ($Action -in @('Backup', 'Restore')) {
        if (-not (Test-Path -LiteralPath $python)) { throw 'Falta .venv. Instala primero requirements.txt; consulta README.md.' }
        $arguments = @((Join-Path $PSScriptRoot 'local_backup.py'), $Action.ToLowerInvariant())
        if ($Action -eq 'Restore') {
            if (-not $Archive -or -not $ConfirmRestore) {
                throw 'Usa -Archive <copia.zip> -ConfirmRestore. Se sustituirán los datos locales y se guardará una copia previa.'
            }
            $arguments += @('--archive', [IO.Path]::GetFullPath($Archive), '--confirm')
        }
        & $python @arguments
        exit $LASTEXITCODE
    }

    # Serialise launch/stop requests for this checkout, independently of the worker lock.
    $hash = [Security.Cryptography.SHA256]::Create()
    $key = [BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($repoRoot))).Replace('-', '')
    $mutex = New-Object Threading.Mutex($false, "Local\MLFinance-$key")
    $locked = $false
    try {
        try { $locked = $mutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $locked = $true }
        if (-not $locked) { throw 'Ya hay un arranque o una parada en curso.' }
        $apiState = Get-ServiceState 'API' 8000
        $uiState = Get-ServiceState 'UI' 5173
        if ($Action -eq 'Status') {
            Write-Output "API: $apiState | UI: $uiState"
            Write-Output 'Web: http://127.0.0.1:5173/'
            exit 0
        }
        if ($apiState -eq 'foreign' -or $uiState -eq 'foreign') {
            throw 'Un puerto está ocupado por otro proceso o checkout. No se ha detenido ni reemplazado. Usa -Action Status.'
        }
        if ($Action -eq 'Stop') {
            if ($apiState -eq 'unready') { throw 'La API no permite comprobar los trabajos activos. Detén su terminal manualmente.' }
            if ($apiState -eq 'ready') {
                $jobs = Invoke-RestMethod 'http://127.0.0.1:8000/api/v1/jobs?limit=100' -TimeoutSec 5
                if (@($jobs.jobs | Where-Object { $_.state -in @('queued', 'running') }).Count) {
                    throw 'Hay trabajos pendientes o activos. Espera a que terminen antes de detener la aplicación.'
                }
            }
            foreach ($service in @(@('UI', 5173), @('API', 8000))) {
                foreach ($listener in @(Get-Listener $service[1])) {
                    if (Test-OurProcess $listener $service[0]) { Stop-Process -Id $listener }
                }
            }
            Write-Output 'Aplicación detenida. Puedes crear o restaurar una copia.'
            exit 0
        }
        # A first analytics request can temporarily delay health; wait without starting a duplicate.
        if ($apiState -eq 'unready') { Wait-Ready 'API' 8000; $apiState = 'ready' }
        if ($uiState -eq 'unready') { Wait-Ready 'UI' 5173; $uiState = 'ready' }
        if ($apiState -eq 'stopped' -and -not (Test-Path -LiteralPath $python)) {
            throw 'Falta .venv. Instala requirements.txt siguiendo README.md.'
        }
        $vite = Join-Path $repoRoot 'frontend/node_modules/vite/bin/vite.js'
        if ($uiState -eq 'stopped') {
            $node = $null
            $candidates = if ($NodePath) { @($NodePath) } else {
                @((Get-Command node.exe -ErrorAction SilentlyContinue).Source,
                  (Join-Path $env:USERPROFILE '.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe'))
            }
            foreach ($candidate in $candidates) {
                if ($candidate -and (Test-Path -LiteralPath $candidate)) {
                    $versionText = & $candidate --version
                    if ($LASTEXITCODE -eq 0 -and [version]($versionText.TrimStart('v')) -ge [version]'22.12.0') {
                        $node = $candidate
                        break
                    }
                }
            }
            if (-not $node) { throw 'Se requiere Node.js 22.12 o posterior. Puedes indicar -NodePath <node.exe>.' }
            if (-not (Test-Path -LiteralPath $vite)) { throw 'Faltan dependencias de UI. Ejecuta npm ci desde frontend/.' }
        }
        New-Item -ItemType Directory -Path $dataRoot -Force | Out-Null
        $started = @()
        try {
            if ($apiState -eq 'stopped') {
                $apiArguments = @((Join-Path $PSScriptRoot 'run_api.py'), '--env-file', (Join-Path $repoRoot '.env'), '--operations', 'real')
                if ($NoRefresh) { $apiArguments += '--no-refresh-on-start' }
                $started += Start-Hidden $python $apiArguments $repoRoot 'app-api'
                Wait-Ready 'API' 8000
            }
            if ($uiState -eq 'stopped') {
                $started += Start-Hidden $node @($vite) (Join-Path $repoRoot 'frontend') 'app-ui'
                Wait-Ready 'UI' 5173
            }
        } catch {
            # Leave logs and existing instances intact. Do not kill a worker doing a startup refresh.
            throw "No se completó el arranque: $($_.Exception.Message) Usa -Action Status para revisar los procesos iniciados."
        }
        Write-Output 'Aplicación lista: http://127.0.0.1:5173/ (datos reales). Puedes cerrar esta terminal.'
        if (-not $NoBrowser) { Start-Process 'http://127.0.0.1:5173/' }
    } finally {
        if ($locked) { $mutex.ReleaseMutex() }
        $mutex.Dispose()
        $hash.Dispose()
    }
} catch {
    Write-Error $_.Exception.Message
    exit 1
}
exit 0
