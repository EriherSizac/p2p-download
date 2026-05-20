#Requires -Version 5.1
<#
.SYNOPSIS
  Lanza N instancias de p2p-chat en background con delay aleatorio 1-5s.
  Pide elevacion UAC una sola vez, crea reglas firewall para todos los
  puertos, y mata todos los hijos al salir (Ctrl+C o cierre de terminal).

.PARAMETER Count     Numero de peers (default 10)
.PARAMETER BasePort  Primer TCP_PORT (default 41237; la instancia principal usa 41236)

.EXAMPLE
  .\spawn-peers.ps1
  .\spawn-peers.ps1 -Count 5 -BasePort 41240
#>
param(
  [int]$Count    = 10,
  [int]$BasePort = 41237
)

# ── 1. Auto-elevacion ─────────────────────────────────────────────────────────
$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
$isAdmin   = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
  $args = "-ExecutionPolicy Bypass -File `"$PSCommandPath`" -Count $Count -BasePort $BasePort"
  Start-Process powershell -Verb RunAs -ArgumentList $args
  exit 0
}

$root = Split-Path $PSCommandPath -Parent

# ── 2. Firewall: una sola elevacion para todos los puertos ───────────────────
Write-Host "=== Firewall TCP $BasePort..$($BasePort + $Count - 1) ===" -ForegroundColor Cyan
$BasePort..($BasePort + $Count - 1) | ForEach-Object {
  $name = "p2p-chat-peer-$_"
  if (-not (Get-NetFirewallRule -Name $name -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -Name $name -DisplayName "p2p-chat :$_" `
      -Direction Inbound -Protocol TCP -LocalPort $_ -Action Allow | Out-Null
    Write-Host "  [+] :$_"
  } else {
    Write-Host "  [=] :$_ (ya existe)"
  }
}

# ── 3. Lanzar peers ───────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Lanzando $Count peers ===" -ForegroundColor Cyan

$processes = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()

for ($i = 0; $i -lt $Count; $i++) {
  $port  = $BasePort + $i
  $delay = Get-Random -Minimum 1 -Maximum 6   # 1-5 segundos

  Write-Host "  peer $($i+1)/$Count  TCP_PORT=$port  (delay ${delay}s)" -NoNewline
  Start-Sleep -Seconds $delay

  # cmd.exe: setea TCP_PORT + piped "echo n" cierra el prompt de firewall automaticamente
  $p = Start-Process "cmd.exe" `
    -ArgumentList "/c set TCP_PORT=$port && echo n | npx tsx src\main.ts" `
    -WorkingDirectory $root `
    -WindowStyle Hidden `
    -PassThru

  $processes.Add($p)
  Write-Host "  -> PID $($p.Id)"
}

Write-Host ""
Write-Host "$Count peers activos. Ctrl+C para matar todos." -ForegroundColor Green

# ── 4. Cleanup al salir (Ctrl+C o cierre de ventana) ─────────────────────────
function Stop-AllPeers {
  Write-Host ""
  Write-Host "=== Matando peers ===" -ForegroundColor Yellow
  foreach ($p in $processes) {
    if (-not $p.HasExited) {
      # /T mata el arbol completo: cmd.exe + tsx + node
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
