/**
 * INDEX DE ARCHIVOS COMPARTIDOS
 * -----------------------------
 * Escanea `SHARED_DIR` al arranque, calcula manifiestos y los persiste en
 * `DOWNLOAD_DIR/.manifests/`. Mantiene tablas:
 *   - fileId → { manifest, fullPath }
 *   - name   → fileId
 *
 * Se usa para responder LIST y MANIFEST de peers remotos, y como entrada para
 * el servidor de piezas cuando llega un REQUEST.
 *
 * 💡 Nota didáctica: cachear el manifiesto por (path, mtime, size) evita
 * rehashear archivos grandes en cada arranque. Si el archivo cambia, su
 * fileId cambia (es el hash de su contenido) — no hay manera de "actualizar"
 * un fileId; se trata como otro archivo.
 */

import fs from 'node:fs';
import path from 'node:path';
import { buildManifest, loadManifest, saveManifest } from './manifest.js';
import type { FileManifest, FileSummary } from './protocol.js';
import { createLogger } from './logger.js';

const log = createLogger('index');

interface IndexEntry {
  manifest: FileManifest;
  fullPath: string;
}

interface CacheRecord {
  /** fileId → datos para invalidar el cache. */
  size: number;
  mtimeMs: number;
  fileId: string;
}

export class FileIndex {
  private byId = new Map<string, IndexEntry>();
  private byName = new Map<string, string>(); // name → fileId
  /** fullPaths agregados explícitamente por CLI (fuera de SHARED_DIR). */
  private external = new Set<string>();
  private cacheFile: string;
  private cache: Record<string, CacheRecord> = {}; // fullPath → record

  constructor(
    private readonly sharedDir: string,
    private readonly manifestsDir: string,
  ) {
    this.cacheFile = path.join(this.manifestsDir, 'cache.json');
  }

  async init(): Promise<void> {
    await fs.promises.mkdir(this.sharedDir, { recursive: true });
    await fs.promises.mkdir(this.manifestsDir, { recursive: true });
    await this.loadCache();
    await this.refresh();
  }

  async refresh(): Promise<void> {
    const entries = await fs.promises.readdir(this.sharedDir, { withFileTypes: true });
    const seen = new Set<string>();
    for (const e of entries) {
      if (!e.isFile()) continue;
      const full = path.join(this.sharedDir, e.name);
      const stat = await fs.promises.stat(full);
      const cached = this.cache[full];
      let manifest: FileManifest | null = null;
      if (cached && cached.size === stat.size && cached.mtimeMs === stat.mtimeMs) {
        manifest = await loadManifest(this.manifestsDir, cached.fileId);
      }
      if (!manifest) {
        log.info(`hashing ${e.name} (${stat.size} bytes)…`);
        manifest = await buildManifest(full);
        await saveManifest(this.manifestsDir, manifest);
        this.cache[full] = { size: stat.size, mtimeMs: stat.mtimeMs, fileId: manifest.fileId };
      }
      this.byId.set(manifest.fileId, { manifest, fullPath: full });
      this.byName.set(manifest.name, manifest.fileId);
      seen.add(manifest.fileId);
    }
    // Quitar del index lo que ya no existe en SHARED_DIR. Conservamos
    // las entradas externas (añadidas por `share <ruta>`) y las que
    // vinieron de descargas completadas.
    for (const id of [...this.byId.keys()]) {
      if (seen.has(id)) continue;
      const e = this.byId.get(id)!;
      const fromShared = e.fullPath.startsWith(this.sharedDir + path.sep);
      if (fromShared && !this.external.has(e.fullPath)) {
        this.byName.delete(e.manifest.name);
        this.byId.delete(id);
      }
    }
    await this.saveCache();
    log.info(`index listo: ${this.byId.size} archivo(s) en ${this.sharedDir}`);
  }

  /** Añade al index un archivo recién descargado (sin re-hashear todo). */
  add(manifest: FileManifest, fullPath: string): void {
    this.byId.set(manifest.fileId, { manifest, fullPath });
    this.byName.set(manifest.name, manifest.fileId);
  }

  /**
   * Añade un archivo arbitrario por ruta (puede estar fuera de SHARED_DIR).
   * Calcula su manifiesto si no está cacheado. Lanza si la ruta no existe o
   * si ya hay otro archivo con el mismo `name` en el index.
   */
  async addPath(absPath: string): Promise<FileManifest> {
    const resolved = path.resolve(absPath);
    const stat = await fs.promises.stat(resolved);
    if (!stat.isFile()) throw new Error(`No es un archivo regular: ${resolved}`);

    const cached = this.cache[resolved];
    let manifest: FileManifest | null = null;
    if (cached && cached.size === stat.size && cached.mtimeMs === stat.mtimeMs) {
      manifest = await loadManifest(this.manifestsDir, cached.fileId);
    }
    if (!manifest) {
      log.info(`hashing ${path.basename(resolved)} (${stat.size} bytes)…`);
      manifest = await buildManifest(resolved);
      await saveManifest(this.manifestsDir, manifest);
      this.cache[resolved] = { size: stat.size, mtimeMs: stat.mtimeMs, fileId: manifest.fileId };
      await this.saveCache();
    }

    const existingByName = this.byName.get(manifest.name);
    if (existingByName && existingByName !== manifest.fileId) {
      throw new Error(
        `ya hay otro archivo en el catálogo con nombre "${manifest.name}". Renombra uno de los dos.`,
      );
    }

    this.byId.set(manifest.fileId, { manifest, fullPath: resolved });
    this.byName.set(manifest.name, manifest.fileId);
    if (!resolved.startsWith(this.sharedDir + path.sep)) {
      this.external.add(resolved);
    }
    return manifest;
  }

  /** Quita una entrada externa por nombre. No borra el archivo de disco. */
  removeExternal(name: string): boolean {
    const id = this.byName.get(name);
    if (!id) return false;
    const entry = this.byId.get(id);
    if (!entry || !this.external.has(entry.fullPath)) return false;
    this.byId.delete(id);
    this.byName.delete(name);
    this.external.delete(entry.fullPath);
    return true;
  }

  list(): FileSummary[] {
    return [...this.byId.values()].map((e) => ({
      fileId: e.manifest.fileId,
      name: e.manifest.name,
      size: e.manifest.size,
      pieceSize: e.manifest.pieceSize,
      numPieces: e.manifest.numPieces,
    }));
  }

  getByFileId(fileId: string): IndexEntry | undefined {
    return this.byId.get(fileId);
  }

  getByName(name: string): IndexEntry | undefined {
    const id = this.byName.get(name);
    return id ? this.byId.get(id) : undefined;
  }

  private async loadCache(): Promise<void> {
    try {
      const raw = await fs.promises.readFile(this.cacheFile, 'utf8');
      this.cache = JSON.parse(raw) as Record<string, CacheRecord>;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
        log.warn('cache index corrupta, descartando', err);
      }
      this.cache = {};
    }
  }

  private async saveCache(): Promise<void> {
    await fs.promises.writeFile(this.cacheFile, JSON.stringify(this.cache, null, 2));
  }
}
