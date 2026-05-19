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

export function isSupported(): boolean {
  return process.platform === 'win32';
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
export function requestFirewallRule(port: number): Promise<boolean> {
  if (!isSupported()) return Promise.resolve(true);

  // Comando que correrá la ventana elevada: borra reglas previas con el
  // mismo nombre y crea una nueva para el puerto actual.
  const elevated =
    `Remove-NetFirewallRule -DisplayName '${RULE_NAME}' -ErrorAction SilentlyContinue; ` +
    `New-NetFirewallRule -DisplayName '${RULE_NAME}' -Direction Inbound -Protocol TCP ` +
    `-LocalPort ${port} -Action Allow | Out-Null`;

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
