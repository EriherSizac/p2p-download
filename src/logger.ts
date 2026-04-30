/**
 * LOGGER
 * ------
 * Logger minimalista con niveles. No usa dependencias: cumple la regla del repo
 * de "solo módulos nativos". El nivel se controla con la env var `LOG_LEVEL`.
 *
 * 💡 Nota didáctica: en aplicaciones reales se usaría pino/winston, pero para
 * un ejercicio P2P es interesante ver lo poco que hace falta.
 */

const LEVELS = ['debug', 'info', 'warn', 'error'] as const;
export type LogLevel = (typeof LEVELS)[number];

const COLORS: Record<LogLevel, string> = {
  debug: '\x1b[90m', // gris
  info: '\x1b[36m',  // cian
  warn: '\x1b[33m',  // amarillo
  error: '\x1b[31m', // rojo
};
const RESET = '\x1b[0m';

const envLevel = (process.env['LOG_LEVEL'] ?? 'info').toLowerCase() as LogLevel;
const currentLevel: LogLevel = LEVELS.includes(envLevel) ? envLevel : 'info';
const minIdx = LEVELS.indexOf(currentLevel);

function emit(level: LogLevel, scope: string, args: unknown[]): void {
  if (LEVELS.indexOf(level) < minIdx) return;
  const ts = new Date().toISOString().slice(11, 23);
  const head = `${COLORS[level]}${ts} ${level.padEnd(5)} [${scope}]${RESET}`;
  // En errores y warnings usamos stderr; el resto a stdout.
  const stream = level === 'error' || level === 'warn' ? process.stderr : process.stdout;
  stream.write(head + ' ' + args.map(formatArg).join(' ') + '\n');
}

function formatArg(a: unknown): string {
  if (typeof a === 'string') return a;
  if (a instanceof Error) return a.stack ?? a.message;
  try {
    return JSON.stringify(a);
  } catch {
    return String(a);
  }
}

export function createLogger(scope: string) {
  return {
    debug: (...args: unknown[]) => emit('debug', scope, args),
    info: (...args: unknown[]) => emit('info', scope, args),
    warn: (...args: unknown[]) => emit('warn', scope, args),
    error: (...args: unknown[]) => emit('error', scope, args),
  };
}
