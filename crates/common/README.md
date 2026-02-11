# dynamo-common

Shared types for the dynamo-kad project.

## What's Here

- **`NodeId`** — 160-bit identifier (both node IDs and keys live in the same space)
- **`Distance`** — XOR distance with big-endian `Ord` for correct ordering
- **`KadError`** — shared error enum
- **`random_id_in_bucket()`** — generates a random ID targeting a specific k-bucket (used for refresh)

## Key Properties

The XOR metric gives us:
- `d(a, a) = 0` (identity)
- `d(a, b) = d(b, a)` (symmetry)
- `d(a, c) = d(a, b) XOR d(b, c)` (XOR identity — stronger than triangle inequality)

Bucket index `i` stores nodes whose XOR distance has its highest set bit at position `i`. Bucket 159 = most distant (MSB differs), bucket 0 = closest (only LSB differs).
