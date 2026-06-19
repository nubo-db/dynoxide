import { test } from "node:test";
import assert from "node:assert/strict";

import { fnv1aHash } from "./fnv1a.js";

// FNV-1a parity contract. These are the same vectors the native scalar asserts
// in `fnv1a_hash_matches_known_vectors` (src/storage.rs). Both sides hashing the
// same inputs to the same values is what lets a parallel scan's segment
// assignment (`fnv1a_hash(pk) % totalSegments`) agree across the wasm bridge and
// the native backend - if these drift, a segmented scan reads the wrong rows.
test("fnv1a_hash matches the cross-backend known vectors", () => {
  const cases = [
    ["", 2166136261],
    ["a", 3826002220],
    ["u#1", 2199603432],
    ["artist#42", 2385694177],
    ["café", 2821410889],
    ["tenant#9007199254740993", 2022216178],
  ];
  for (const [input, expected] of cases) {
    assert.equal(fnv1aHash(input), expected, `fnv1a_hash(${JSON.stringify(input)})`);
  }
});

test("fnv1a_hash hashes a non-string argument as the empty string", () => {
  // The scalar is only ever applied to TEXT key columns, but the bridge passes
  // through whatever the engine hands it; a NULL or non-text value hashes as "".
  const empty = 2166136261;
  assert.equal(fnv1aHash(null), empty);
  assert.equal(fnv1aHash(undefined), empty);
  assert.equal(fnv1aHash(42), empty);
});
