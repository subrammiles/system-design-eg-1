// Apply the PostgreSQL schema (idempotent).
import { readFile } from 'node:fs/promises';
import { createPg, retry } from '../src/shared/clients.js';

const pool = createPg();
await retry('postgres', () => pool.query('SELECT 1'));
await pool.query(await readFile(new URL('./schema.sql', import.meta.url), 'utf8'));
console.log('schema ready');
await pool.end();
