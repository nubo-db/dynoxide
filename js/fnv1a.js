/**
 * FNV-1a (32-bit) hash of a string's UTF-8 bytes, returned as a number in
 * [0, 2^32).
 *
 * This must match the native rusqlite `fnv1a_hash` scalar (src/storage.rs)
 * byte-for-byte: a parallel scan partitions rows by
 * `fnv1a_hash(pk) % totalSegments`, so the wasm bridge and the native backend
 * have to agree or a segmented scan would read the wrong rows. The agreement is
 * pinned by tests on both sides against shared vectors - js/fnv1a.test.js here
 * and `fnv1a_hash_matches_known_vectors` in src/storage.rs.
 *
 * A non-string argument hashes as the empty string. The native scalar instead
 * rejects a non-text argument, but the function is only ever applied to TEXT key
 * columns, so the two never diverge on a real input.
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
