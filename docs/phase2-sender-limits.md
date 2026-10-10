# Phase 2 sender resource and retry policy

The default sender count is one. Explicit sender parallelism is bounded by four,
available CPUs and runtime workers. Each sender shares sixteen slots between
staged and running deliveries, so at most 64 delivery tasks run in one process.
Each sender retains at most 128 payload-free dispatch hints and 128 coalesced
wake hints. Hint destinations cannot exceed the storage key limit (16 KiB).
These are hard inventory limits, not a measured process RSS guarantee.

A selected delivery has one `sender_timeout` deadline covering database reads,
composition, persistence and HTTP. Expiry yields a local uncertain outcome,
releases its guards and retains durable active rows and any immutable attempt
for recovery. It never invents a transport acknowledgement. Shutdown separately
uses its existing bounded drain and joins cancelled delivery tasks.

Appservice device/key metadata is complete or refused: 128 device IDs per user,
256 devices and 64 KiB of retained user/device IDs per transaction; one shared
4,096-row and 1 MiB encoded-input work budget for key metadata; at most sixteen
algorithms per device. This sender read does not prune one-time keys. Storage,
corruption and inventory-limit errors propagate instead of shortening output.

Appservice to-device serialization stops before an oversized payload allocation.
Every value reserves ten bytes for its eventual active identity. At most 900
recipients and 3 MiB of serialized input are retained before fanout admission.
To-device, device-list, ephemeral and badge-refresh producers preflight their
whole fanout against the existing 900-operation / 4 MiB encoded commit budget
before queue/counter mutation; they do not commit an accepted prefix on refusal.
Badge refresh also uses the checked complete 128-pusher inventory.

Immutable attempts retain at most 3 MiB of body split into 128 KiB chunks, a
256 KiB sealed header and 512 members. Decode refuses excessive CBOR member
length hints before allocating their inventory. Each chunk/header and complete
body digest is verified by the existing replay reader. A body that exceeds the
limit is split only between complete deliveries. An oversized singleton is
refused; a legacy accepted singleton remains owed and raises an explicit local
error. This is a fail-closed retention policy, not a promise to deliver invalid
historical payloads or a protocol for splitting one Matrix event.

Push retry metadata has one fixed thirteen-byte record per failed destination,
owned by its physical active admissions. It survives restart and is retired by
matching acknowledgement, cancellation or canonical erasure. Age does not erase
still-owed failure identity. A backward clock correction rebases a future stored
timestamp once under the active-row exclusion and persists that baseline. The
failure streak is preserved and one full configured retry window remains; later
reads do not renew it. Forward jumps can expire the hold, never its owed rows.

The focused producer, delivery, clock and sealed-manifest controls must pass on
the integrated source. Real provider failure/backpressure, deployed workload
memory and cost measurements remain separate release acceptance checks.
