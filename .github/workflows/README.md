## Overview

The release pipeline `Main` (main.yml) and its subroutines defined in the other yamls form a high-level
description for the underlying self-hosted build system in  `/docker`. In other words, this is a sort of
terminal, a "thin-client" with a display and a keyboard for our docker mainframe. We minimize
vendor-lockin and duplication with other services by limiting everything here to only what is
essential for driving the docker builder. See: [documentation](../../docs/development/testing/workflows.md)

The fork's `ci-native` job also runs `ci/native-gate.sh container-lifecycle`
on Linux/amd64 with the production release profile. `ci/container-features.txt`
records the default feature set with `systemd` and `io_uring` removed; the gate
refuses other platforms or feature drift. It runs the main/core/database/service/
router library suites and the real-process cancellation, fatal startup and
SIGKILL/recovery integrations, requiring 42 named lifecycle controls. The Unix
restart control executes the actual `exec` path in a disposable child process.
The same job, release dependency feature closure and Cargo target directory are
reused for lifecycle and record decoding; no second runner or provider credentials
are required. Cargo test enables the existing dev-dependency refusal/recovery hooks;
these are recorded in the receipt and are absent from the release image. The small receipt records source, scope and required
cases. This gate qualifies local RocksDB and loopback bridge behavior, and does
not claim a real-provider or staging result.
