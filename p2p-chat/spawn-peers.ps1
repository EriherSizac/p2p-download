#Requires -Version 5.1
<#
.SYNOPSIS
  Lanza N instancias de p2p-chat en background con delay aleatorio 1-5s.
  La primera ejecucion pide UAC UNA sola vez para crear reglas de firewall
  permanentes. Usos posteriores no requieren privilegios de admin.

.PARAMETER Count     Numero de peers (default 10)
.PARAMETER BasePort  Primer TCP_PORT (default 41237; la instancia principal usa 41236)

.EXAMPLE
  .\spawn-peers.ps1
  .\spawn-peers.ps1 -Count 5 -BasePort 41240
#>
param(
  [int]$Count    = 10,
  [int]$BasePort = 41237,
  [switch]$FirewallOnly   # interno: solo crear reglas y salir
)

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
           ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

# ── 1. Detectar reglas faltantes (lectura sin admin) ─────────────────────────

function Get-MissingRules([int]$base, [int]$cnt) {
  $missing = [System.Collections.Generic.List[string]]::new()
  for ($i = 0; $i -lt $cnt; $i++) {
    $p = $base + $i
    if (-not (Get-NetFirewallRule -Name "p2p-chat-peer-$p" -EA SilentlyContinue)) {
      $missing.Add("tcp:$p")
    }
  }
  return ,$missing
}

$missing = Get-MissingRules -base $BasePort -cnt $Count

# ── 2. Crear reglas si faltan (una sola vez con UAC) ─────────────────────────

function New-ChatRules([int]$base, [int]$cnt) {
  Write-Host "=== Firewall (reglas permanentes) ===" -ForegroundColor Cyan
  for ($i = 0; $i -lt $cnt; $i++) {
    $p    = $base + $i
    $name = "p2p-chat-peer-$p"
    if (-not (Get-NetFirewallRule -Name $name -EA SilentlyContinue)) {
      New-NetFirewallRule -Name $name -DisplayName "p2p-chat :$p" `
        -Direction Inbound -Protocol TCP -LocalPort $p -Action Allow | Out-Null
      Write-Host "  [+] TCP :$p"
    } else {
      Write-Host "  [=] TCP :$p  (ya existe)"
    }
  }
  Write-Host "  Reglas guardadas — proximas ejecuciones no necesitan admin." -ForegroundColor Green
}

if ($missing.Count -gt 0) {
  if (-not $isAdmin) {
    Write-Host "[!] Primer uso — elevando para crear $($missing.Count) regla(s) de firewall..." `
      -ForegroundColor Yellow
    $elevArgs = "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" +
                " -Count $Count -BasePort $BasePort -FirewallOnly"
    Start-Process powershell -Verb RunAs -Wait -ArgumentList $elevArgs

    $missing = Get-MissingRules -base $BasePort -cnt $Count
    if ($missing.Count -gt 0) {
      Write-Host "[!] Reglas aun faltantes: $($missing -join ', ')" -ForegroundColor Red
      Write-Host "    Ejecuta el script como administrador manualmente." -ForegroundColor Red
      exit 1
    }
    Write-Host "[ok] Firewall listo. Continuando sin admin..." -ForegroundColor Green
  } else {
    New-ChatRules -base $BasePort -cnt $Count
  }
}

if ($FirewallOnly) { exit 0 }

# ── 3. Lanzar peers ───────────────────────────────────────────────────────────

$root      = Split-Path $PSCommandPath -Parent
$processes = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()

Write-Host ""
Write-Host "=== Lanzando $Count peers ===" -ForegroundColor Cyan

for ($i = 0; $i -lt $Count; $i++) {
  $port  = $BasePort + $i
  $delay = Get-Random -Minimum 1 -Maximum 6   # 1-5 s

  Write-Host "  peer $($i+1)/$Count  TCP_PORT=$port  (delay ${delay}s)" -NoNewline
  Start-Sleep -Seconds $delay

  $p = Start-Process "cmd.exe" `
    -ArgumentList "/c set TCP_PORT=$port && npx tsx src\main.ts" `
    -WorkingDirectory $root `
    -WindowStyle Hidden `
    -PassThru

  $processes.Add($p)
  Write-Host "  -> PID $($p.Id)"
}

Write-Host ""
Write-Host "$Count peers activos. Ctrl+C para matar todos." -ForegroundColor Green
Write-Host "(proximas ejecuciones no pediran admin)" -ForegroundColor DarkGray

# ── 4. Cleanup al salir ───────────────────────────────────────────────────────

function Stop-AllPeers {
  Write-Host ""
  Write-Host "=== Matando peers ===" -ForegroundColor Yellow
  foreach ($p in $processes) {
    if (-not $p.HasExited) {
      & taskkill /F /T /PID $p.Id | Out-Null
      Write-Host "  killed PID $($p.Id)"
    }
  }
}

try {
  while ($true) { Start-Sleep -Seconds 2 }
} finally {
  Stop-AllPeers
}
