/**
 * FNV-1a (32-bit) hash of a string's UTF-8 bytes, as a number in [0, 2^32).
 *
 * Must match the native rusqlite `fnv1a_hash` scalar byte-for-byte: a parallel
 * scan partitions rows by `fnv1a_hash(pk) % totalSegments`, so the wasm and
 * native backends have to agree or a segmented scan reads the wrong rows. The
 * match is pinned by shared vectors in js/fnv1a.test.js and the
 * `fnv1a_hash_matches_known_vectors` test in src/storage.rs. A non-string
 * argument hashes as "" (the function only ever sees TEXT key columns).
 */
const ENCODER = new TextEncoder();

export function fnv1aHash(text) {
  const bytes = ENCODER.encode(typeof text === "string" ? text : "");
  let hash = 0x811c9dc5;
  for (const b of bytes) {
    hash ^= b;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}
