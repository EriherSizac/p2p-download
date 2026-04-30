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
    // Quitar del index lo que ya no existe en disco.
    for (const id of [...this.byId.keys()]) {
      if (!seen.has(id)) {
        const e = this.byId.get(id)!;
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
