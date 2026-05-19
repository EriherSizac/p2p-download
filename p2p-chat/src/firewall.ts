/**
 * FIREWALL — gestión de regla inbound TCP (solo Windows).
 *
 * Problema: Windows bloquea por defecto conexiones TCP entrantes a procesos
 * sin regla explícita. Los peers nos descubren por UDP (que pasa, porque
 * abrimos el socket nosotros) pero su `connect()` TCP saliente choca con
 * nuestro firewall y muere con ETIMEDOUT.
 *
 * Solución: tras conocer el puerto TCP real, comprobamos si existe una
 * regla con nuestro nombre apuntando al mismo puerto. Si no, pedimos
 * confirmación y lanzamos una PowerShell ELEVADA (UAC) que la crea.
 *
 * Idempotente: borra reglas previas con el mismo `DisplayName` antes de
 * crear la nueva, así puertos efímeros no acumulan basura.
 */

import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { createLogger } from './logger.js';

const log = createLogger('firewall');
const execFileP = promisify(execFile);

export const RULE_NAME = 'p2p-chat-inbound';
export const RULE_NAME_UDP = 'p2p-chat-udp-node';

export function isSupported(): boolean {
  return process.platform === 'win32';
}

/**
 * Path del ejecutable node actual. Lo usamos para crear la regla UDP por
 * programa (no por puerto): WebRTC abre puertos UDP aleatorios para ICE,
 * imposible saberlos de antemano. Permitir UDP para el proceso node.exe es
 * la solución estándar en escritorio.
 */
function nodeExePath(): string {
  return process.execPath;
}

/** ¿Existe la regla UDP por programa apuntando a este node.exe? */
export async function udpRuleExists(): Promise<boolean> {
  if (!isSupported()) return true;
  const exe = nodeExePath().replace(/'/g, "''");
  const script = `$r = Get-NetFirewallRule -DisplayName '${RULE_NAME_UDP}' -ErrorAction SilentlyContinue; if ($null -eq $r) { '0'; exit }; $a = $r | Get-NetFirewallApplicationFilter; if ($a.Program -ieq '${exe}') { '1' } else { '0' }`;
  try {
    const { stdout } = await execFileP('powershell', ['-NoProfile', '-Command', script], {
      windowsHide: true,
    });
    return stdout.trim() === '1';
  } catch (err) {
    log.debug(`check UDP falló: ${(err as Error).message}`);
    return false;
  }
}

/** ¿Existe ya una regla con nuestro nombre + ese puerto? */
export async function ruleExistsForPort(port: number): Promise<boolean> {
  if (!isSupported()) return true;
  const script = `$r = Get-NetFirewallRule -DisplayName '${RULE_NAME}' -ErrorAction SilentlyContinue; if ($null -eq $r) { '0'; exit }; $p = $r | Get-NetFirewallPortFilter; if ($p.LocalPort -contains '${port}' -or $p.LocalPort -contains ${port}) { '1' } else { '0' }`;
  try {
    const { stdout } = await execFileP('powershell', ['-NoProfile', '-Command', script], {
      windowsHide: true,
    });
    return stdout.trim() === '1';
  } catch (err) {
    log.debug(`check falló: ${(err as Error).message}`);
    return false;
  }
}

/**
 * Lanza una PowerShell elevada que (re)crea la regla. Dispara UAC.
 * Resuelve true si el child exit code == 0. No mide si el usuario aceptó
 * el UAC; basta con que el subproceso termine sin error.
 */
export function requestFirewallRule(port: number, opts?: { udp?: boolean }): Promise<boolean> {
  if (!isSupported()) return Promise.resolve(true);

  // Comando que correrá la ventana elevada. Una sola elevación crea:
  //   1) Regla TCP inbound para nuestro puerto de chat (señalización).
  //   2) [opcional] Regla UDP inbound para node.exe (WebRTC/ICE), porque
  //      los puertos UDP de ICE son aleatorios y no se pueden fijar.
  const exe = nodeExePath().replace(/'/g, "''");
  const tcpPart =
    `Remove-NetFirewallRule -DisplayName '${RULE_NAME}' -ErrorAction SilentlyContinue; ` +
    `New-NetFirewallRule -DisplayName '${RULE_NAME}' -Direction Inbound -Protocol TCP ` +
    `-LocalPort ${port} -Action Allow | Out-Null`;
  const udpPart = opts?.udp
    ? `; Remove-NetFirewallRule -DisplayName '${RULE_NAME_UDP}' -ErrorAction SilentlyContinue; ` +
      `New-NetFirewallRule -DisplayName '${RULE_NAME_UDP}' -Direction Inbound -Protocol UDP ` +
      `-Program '${exe}' -Action Allow | Out-Null`
    : '';
  const elevated = tcpPart + udpPart;

  // Outer PowerShell pide elevación con `Start-Process -Verb RunAs`. -Wait
  // bloquea hasta que la consola elevada cierre, así sabemos cuándo
  // continuar. ArgumentList va como array para no pelear con escapes.
  const outer =
    `Start-Process powershell -Verb RunAs -Wait -WindowStyle Hidden ` +
    `-ArgumentList '-NoProfile','-Command','${elevated.replace(/'/g, "''")}'`;

  return new Promise((resolve) => {
    const child = spawn('powershell', ['-NoProfile', '-Command', outer], {
      stdio: 'ignore',
      windowsHide: true,
    });
    child.on('exit', (code) => {
      log.debug(`elevated firewall exit code=${code}`);
      resolve(code === 0);
    });
    child.on('error', (err) => {
      log.warn(`no se pudo lanzar elevación: ${err.message}`);
      resolve(false);
    });
  });
}
