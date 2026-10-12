# Backup and Recovery

Tuwunel offers two online ways to preserve a RocksDB database. The managed
backup system maintains a backup repository, verifies its contents, applies a
retention limit, and supports a built-in restore. Checkpoints are ordinary
RocksDB directories intended for operators who prefer to manage copying,
retention, and restoration themselves. A conventional offline copy remains the
simplest option when downtime is acceptable.

> [!WARNING]
> Online database backups and checkpoints do not include media. Back up the
> `media/` directory and every configured storage provider separately. A copy
> stored on the same disk as the live database also does not protect against
> disk loss.

## Choosing a method

| Method | Server downtime | Format | Retention and restore |
| --- | --- | --- | --- |
| Managed online backup | None while creating | RocksDB backup repository | Tuwunel manages retention, verification, and restore |
| Checkpoint | None while creating | Directly usable RocksDB directory | Operator manages copying, retention, and restore |
| Offline copy | Required | Copy of `database_path` | Operator manages retention and restore |

## Managed online backups

The managed system is the recommended choice when Tuwunel should own the
backup lifecycle. Configure a writable repository path and the number of
backups to retain:

```toml
database_backup_path = "/var/backups/tuwunel"
database_backups_to_keep = 7
```

Both settings are reloadable. The retention value must be at least one. After
each successful backup, RocksDB removes the oldest entries until the repository
is within the configured limit.

Create and inspect backups from the admin room:

```text
!admin server backup-database
!admin server list-backups
!admin server verify-backup
!admin server verify-backup 3
```

`verify-backup` selects the newest backup when its ID is omitted. Verification
confirms that every expected file is present with the recorded size. Restore
also verifies file checksums.

Backups can be removed independently of the configured retention limit. The
argument is the number of newest backups to keep, not a backup ID:

```text
!admin server delete-backups 3
```

A value of zero deletes every managed backup.

### Restoring a managed backup

The backup repository is not itself a directly usable database directory. Stop
Tuwunel, then start the binary once with `--restore-backup` to restore the most
recent backup into `database_path`:

```bash
tuwunel --restore-backup
```

Pass an ID from `list-backups` to select a specific backup:

```bash
tuwunel --restore-backup=3
```

Only this command-line argument can select a restore. Tuwunel refuses the
equivalent setting from configuration files, the environment, and `-O`, which
prevents an old setting from repeating a restore on a later restart.

> [!WARNING]
> Restore replaces the RocksDB files in `database_path`. Preserve the current
> database and media before proceeding so the operation can be reversed if the
> selected backup is not the intended one.

For a systemd installation, perform the one-time restore as the service user
while the normal service is stopped:

```bash
systemctl stop tuwunel
sudo -u tuwunel tuwunel --config /etc/tuwunel/tuwunel.toml --restore-backup \
	--maintenance --execute "server shutdown"
systemctl start tuwunel
```

Maintenance mode prevents the temporary process from serving clients. The
startup command shuts it down after the restore completes. With Docker or
Podman, append `--restore-backup` to a one-time container invocation that uses
the normal configuration and volumes, then recreate the regular container.

#### Manual recovery

Prefer the built-in restore whenever the Tuwunel binary can run. If it is not
available, a managed backup can be reconstructed manually:

1. Create a new, empty directory for the recovered database.
2. Copy the `.sst` files from
   `$DATABASE_BACKUP_PATH/shared_checksum` into it.
3. Rename each shared file from `######_sxxxxxxxxx.sst` to `######.sst`.
4. Copy every file from the selected numbered backup directory into the new
   directory.
5. Set `database_path` to the recovered directory, restore its ownership, and
   start Tuwunel.

For example, this shell loop performs the shared-file rename from inside the
new directory:

```bash
for file in *_s*.sst; do
	mv "$file" "$(printf '%s\n' "$file" | sed 's/_s.*/.sst/')"
done
```

## Checkpoints

A full checkpoint is a consistent, directly usable RocksDB directory. RocksDB
normally hard-links immutable SST files when the destination permits it, so a
checkpoint can be quick to create and initially consumes little additional
space. Copy it to independent storage if it must survive loss of the database
disk.

Create a checkpoint with the default settings:

```text
!admin server checkpoint-database
```

Without a path, Tuwunel creates
`<database_path>/checkpoint-<unix_timestamp>`. An explicit destination is often
more convenient for an external backup volume:

```text
!admin server checkpoint-database --path /var/backups/tuwunel/checkpoint-2026-08-22
```

The destination must be new. Tuwunel does not overwrite an existing directory.
The server does not track checkpoint directories or apply retention to them.

### Checkpoint options

`--log-size [bytes]` controls the write-ahead log threshold used for a full
database checkpoint. Omitting the option, writing it without a value, or
setting it to zero forces RocksDB to flush as needed before creating the
checkpoint. Keep this default unless retaining recent writes through copied WAL
files is a deliberate part of the backup design.

`--map <name>` exports one column family instead of the complete database.
`--column` is an alias for the same option. A map export always flushes that
column family, does not use `--log-size`, and is not a complete server backup.

### Deleting a checkpoint

A checkpoint has no registration or reference in Tuwunel. After it has expired
or been copied and verified elsewhere, remove its directory with normal
filesystem tools. Removing a checkpoint safely releases its hard links without
deleting the live database files.

> [!WARNING]
> Confirm the exact path before removing it. Never remove files from the live
> `database_path` individually. For a default checkpoint, the complete
> `checkpoint-<unix_timestamp>` child directory is the deletion boundary.

For example:

```bash
rm -r /var/lib/tuwunel/checkpoint-1755850800
```

### Restoring a checkpoint

Stop Tuwunel before opening or restoring a checkpoint. Either copy the complete
checkpoint into a new database directory and point `database_path` to it, or
replace the old database directory only after preserving it for rollback.
Restore directory ownership for the service account, restore media separately,
then start Tuwunel and verify the server before deleting the previous database
or source checkpoint.

### Scheduling checkpoints with a signal

On Unix systems, `admin_signal_execute` runs its configured admin commands when
Tuwunel receives `SIGUSR2`. Command strings omit the `!admin` prefix. This
configuration creates a timestamped checkpoint inside `database_path` for each
signal:

```toml
admin_signal_execute = ["server checkpoint-database"]
```

The setting is reloadable. After reloading the configuration or restarting the
server, test it once with systemd:

```bash
systemctl kill --kill-whom=main --signal=USR2 tuwunel.service
```

The command result is written to the server log. Successful signal delivery
only confirms that systemd sent the signal, so monitor the log and confirm that
the new checkpoint directory exists.

A root-owned cron file can request a checkpoint every day at 03:00 in the cron
daemon's local timezone:

```cron
# /etc/cron.d/tuwunel-checkpoint
SHELL=/bin/sh
PATH=/usr/bin:/bin

0 3 * * * root systemctl kill --kill-whom=main --signal=USR2 tuwunel.service
```

Adjust the unit name for the installation. Cron only triggers creation; it does
not copy, verify, or expire checkpoints. Add a separate retention process after
the checkpoints have reached their independent backup destination.

## Offline backups

For the simplest database copy, stop Tuwunel and copy the entire
`database_path` to independent storage. The copied RocksDB directory can be
restored without conversion. Copy the `media/` directory and any external media
storage providers as part of the same backup set.

## Upgrading durable federation deliveries

Before upgrading a database older than native schema 22, close new admissions
and let the previous writer drain its active outgoing deliveries. Those versions
did not persist the transaction body and identity needed to distinguish a retry
from a second delivery after a lost acknowledgement. The new writer refuses a
nonempty legacy active queue before migration writes. Do not delete or requeue
those rows to make the check pass. Keep an offline pre-upgrade backup until the
new writer has passed recovery checks; an older binary must not open the upgraded
schema.

Native schema 24 also records whether this server accepted a canonical event's
federation forwarding role. Databases created by earlier writers have no durable
historical forwarding receipts. Draining active deliveries does not reconstruct
those receipts: a received remote membership event may have been forwarded and
acknowledged already, or may never have acquired a resident forwarding role.

For an already-known event without a role record, a repeated membership handshake
therefore refuses without admitting another delivery or rewriting the event. It
reports the missing role and preserves the database. New events record their role
atomically and are unaffected by that historical ambiguity. A missing role on new
data is corruption and also refuses. Do not manufacture received/owned records,
change the schema version, or blindly replay historical events.

Automatic historical role reconstruction is not supported. Preserve the original
backup and seek an event-specific recovery decision based on independent delivery
evidence. If rollback is required, restore the complete pre-upgrade backup under
closed admission and use its matching previous writer; do not downgrade the live
upgraded database. This refusal is a data-preserving limitation, not a claim of
transparent legacy handshake compatibility.
