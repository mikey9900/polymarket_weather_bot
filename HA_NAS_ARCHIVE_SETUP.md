# Keep the bot on Home Assistant and archive to a NAS

The active SQLite tracker stays in `/share/weather_bot` on Home Assistant's
local storage. The archive job makes a consistent SQLite backup **on local
storage**, then writes a verified ZIP and JSON receipt to the NAS. SQLite's
[network filesystem guidance](https://www.sqlite.org/useovernet.html) is the
reason the live database does not move to the NAS. The backup uses SQLite's
[online backup API](https://www.sqlite.org/backup.html), so the add-on can keep
running while an archive is made.

## Connect the existing storage in Home Assistant OS

On October 6, 2026, `WeatherArchive` was connected in Home Assistant Network
Storage and `mountpoint /share/WeatherArchive` confirmed a real mount. The
Weather Bot add-on did not appear in the installed apps list, so it still needs
to be installed or updated before enabling archiving. The existing **Home
Storage Pool** app exposes a two-disk mergerfs pool through the SMB share
`\\storage.home.arpa\HomeStorage`. The storage app and bot run on the same HA
host. The pool is not a mirror, and a host outage affects both local data and
this archive; keep a separate backup for disaster recovery.

1. Open **Settings → System → Storage → Add network storage**.
2. Name the connection `WeatherArchive` (or another single name using letters,
   numbers, `_`, or `-`). Do **not** name it `weather_bot`, which is the live
   data directory.
3. Choose **Usage: Share** and **Protocol: Samba/Windows (CIFS)**. Enter server
   `192.168.1.83`, share `HomeStorage`, and the credentials for that SMB share.
   Connect it. `storage.home.arpa` did not resolve from this HA host, so use the
   verified LAN IP. These credentials go into Home Assistant's UI, not this
   repository.
4. Home Assistant will expose it to apps at `/share/WeatherArchive`. The bot
   add-on already maps `/share` read/write. These paths and usage types are
   documented in [Home Assistant's network storage guide](https://www.home-assistant.io/common-tasks/os/#network-storage).

The archive code refuses to write if `/share/WeatherArchive` is not an actual
mount, or if the configured share name is a path or symlink. It will not create
a local replacement directory when the NAS is disconnected. In the Home
Assistant Terminal, `mountpoint /share/WeatherArchive` should report that it
is mounted; `df -h /share/WeatherArchive` shows available space.

The bot repository is already present in Home Assistant's app store, where
**Polymarket Weather Bot** is listed as **Not installed**. After the updated
code is published, open **Settings → Apps → Install app** and install it from
the existing listing. Home Assistant's [app installation guide](https://www.home-assistant.io/addons/)
describes that screen.

An archive temporarily needs free space on Home Assistant's local SSD roughly
equal to the current tracker DB size, plus a safety margin. The observed HA
directory was about 3.1 GB on October 6, 2026 (1.1 GB DB and 2.1 GB WAL), so
leave at least about 1.2 GB free for the first snapshot. The code checks free
space and leaves local history intact if there is not enough.

## Configure the add-on

In the Polymarket Weather Bot add-on Configuration tab, set:

```yaml
storage_nas_share_name: WeatherArchive
storage_nas_archive_max_gb: 90
```

`storage_nas_share_name` is blank by default; leave it blank until the NAS is ready.
The example uses a 90 GiB software archive budget, leaving headroom for a new
snapshot before deleting old ones. The mounted storage currently has about
510 GB free, and no hard 100 GB share quota has been set. Other data on the
same storage pool can still consume that free space.
The full new ZIP is written before older ZIPs are removed, so temporary usage
can exceed 90 GiB by one snapshot. This setting is a rolling target, not a
filesystem quota. The bot checks free space for that new snapshot and pauses
local cleanup if the Share is too full.

This is **rolling retention**, not permanent history. Older ZIPs are removed
when the 90 GiB budget fills. A closed trade deleted from the local tracker
may exist only in those older ZIPs; after they roll off, that history is gone.
Save any records you need indefinitely to a separate, off-host backup. The
Home Storage Pool share is on the same HA host and is not an off-host backup.

Local storage cleanup checks on the configured schedule. When retention
pressure calls for pruning, it creates a fresh archive at most once per day
under `/share/WeatherArchive/weather_bot_archive/` before deleting local
history or exports. Each ZIP contains
`weatherbot.db` plus current scan JSON, analysis bundle ZIPs, reports,
research candidates, Codex run records, and retained preseed DB backups.
A matching JSON receipt records the ZIP and DB SHA-256 hashes and sizes.
After a verified new ZIP and receipt exist, the bot removes its oldest archive
ZIPs when the archive budget is exceeded. It never deletes other NAS files.

If the NAS is unavailable or a snapshot fails verification, the bot continues
scanning and paper trading from its local database. Local cleanup pauses when
a NAS is configured but unavailable, and the add-on reports the archive error.
The local size target is therefore a soft limit during a NAS outage. Check
free space on Home Assistant if the mount stays offline for an extended time.

## Verify and restore

After a cleanup run that needs to prune data, check the add-on logs for the archive result
and confirm a `weatherbot_*.zip` with a matching `.json` receipt appears in
`/share/WeatherArchive/weather_bot_archive/`. The receipt's
`snapshot_sha256` can be checked against the ZIP with `sha256sum` on a system
that can access the NAS. The status includes the archive timestamp and size.

To restore:

1. Stop the add-on and confirm it has exited. Verify the selected ZIP against
   its receipt, then extract its `weatherbot.db` to a staging location on local
   HA storage.
2. Move the existing `/share/weather_bot/weatherbot.db` and any matching
   `weatherbot.db-wal` and `weatherbot.db-shm` into a separate backup directory
   **as one set**. Do not leave either old sidecar beside the restored DB.
3. Move the staged `weatherbot.db` into `/share/weather_bot/weatherbot.db`.
   Confirm the live directory has no stale `weatherbot.db-wal` or
   `weatherbot.db-shm` before starting the add-on.
4. Start the add-on. Check its startup logs, dashboard, paper balance, and
   position history. Keep the original DB and sidecars until those checks pass.

Keep the live database on local HA storage; do not point
`WEATHER_SHARED_DATA_ROOT` at the NAS.
