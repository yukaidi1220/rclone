---
title: "Cloudreve"
description: "Rclone docs for the Cloudreve backend"
versionIntroduced: "v1.76"
---

# Cloudreve

[Cloudreve](https://github.com/cloudreve/Cloudreve) is a self-hosted file
hosting system: it keeps its own database of names, folders and permissions in
front of a storage policy, and the policy decides where the bytes actually
live. This backend talks to a Cloudreve site as one of its users, so rclone
sees the drive the account sees.

Cloudreve supports several storage policies - local disk, the object stores of
cloud providers, and a pool that chooses between them per upload. **This backend
only writes through the `s3` policy**, which is the one that hands rclone
presigned part URLs to push bytes with. Folders on any other policy can still be
listed and downloaded, but writing into them is refused with an error naming the
policy in use.

A `load_balance` folder is a pool of child policies, and the server picks one per
upload. rclone reads the pool from the folder listing, picks one of its `s3`
children at random and names that child in the upload session, so a pool of S3
buckets works and still spreads files across them. A pool whose children are all
non-S3 is refused, and so is an upload the server lands on a non-S3 policy
despite being asked otherwise.

A folder holds no policy of its own unless the site binds one: by default every
folder inherits the policy of the account's group. On a site that allows binding -
the Pro `PATCH /file/policy` call - binding a folder to a plain S3 policy makes
rclone write through that policy instead, which it picks up from the listing with
no configuration of its own.

To configure a site for this backend, make sure the folders rclone writes to are
on an S3 policy, or on a load_balance pool containing at least one S3 policy.

<!--intro-start-->

## Configuration

Here is an example of making a configuration for an account on
`https://drive.example.com`. Running `rclone config` selects `cloudreve` from
the list, then asks for the URL and the credentials.

This sets up a remote called `cloudreve remote`.

```
No remotes found - make a new one
n) New remote
...
name> cloudreve remote
Storage> cloudreve
url> https://drive.example.com
email> me@example.com
password> ********
access_token>
refresh_token>
location> rclone
```

`access_token` and `refresh_token` are asked in the normal flow, so an account
that cannot use a password - one created through SSO or OIDC, or one that only
signs in with a passkey - can be configured without digging through the advanced
options. Pressing Enter at `email` and `password` and filling the two tokens is
that path. `refresh_token` can be left blank when rclone issued the token itself,
because it then renews it on its own.

### Authentication

Cloudreve's API authorises requests with a bearer token only - the session
cookie the web interface uses is not read by the file endpoints - so rclone has
to hold a token itself. Two ways in:

- **`email` and `password`** - rclone signs in when the remote opens and renews
  the session before the token expires, or again if the server refuses it. The
  renewed pair is written back into the remote's config, because a refresh token
  is spent by being used: saving it is what keeps the next run working.
- **`access_token`** (with `refresh_token` to allow renewal) - for a token taken
  from somewhere else.

Before signing in, rclone asks the site how that account can prove itself, so the
failure modes say something useful instead of "wrong password":

| Situation | What rclone says |
|---|---|
| Site asks for a login captcha | cannot answer it - turn it off, or use `access_token` |
| Account has no local password (issued through SSO or OIDC) | no local password - set one, or use `access_token` |
| Account signs in with a passkey | cannot present a passkey - use `access_token` |
| Email not on this site | no account with that email |
| Credentials refused | the site rejected this email and password pair |

### Root

`location` is the folder rclone starts in, and paths on the command line are
relative to it: with `location = rclone`, `cloudreve-remote:photos` is the drive
path `rclone/photos`. `root_folder_id` addresses the root by the folder's ID
instead, and skips resolving a path at all, which helps when a folder name contains
characters the URI parser treats specially. The two name the root in different
ways, so setting both is refused rather than guessed at.

<!--intro-end-->

## Transfers

### Comparison and data integrity

The Cloudreve API returns no content hash, so rclone cannot verify a transfer
with checksums and compares objects by size and modified time. `--checksum` has
nothing to compare and `rclone check` can only report size and mod time - use
`rclone check --download` to compare the bytes themselves.

Mod times are held to the second: the time stored is the `last_modified` of the
upload, and there is no API to change a file's time afterwards, so `rclone touch`
fails. rclone sends the source's mod time when it uploads, so a copy preserves it.
Note that the server stamps a moved or renamed entry with the time of that
operation, which is why rclone does not move or copy files server-side at all -
see [server-side operations](#server-side-operations).

### Streaming, empty files and names

An upload session is created with the size of the file in it, so rclone must know
the size up front: piped input is refused.

A zero byte file goes through the same upload session as any other file: the
server hands out one part URL even for size 0, and that is the only path that
carries `last_modified`, so an empty file keeps its modified time. The file
creation endpoint is not used for this - it has no time field and would stamp the
server's own clock.

The server answers `40052 Invalid object name` for names containing
`\ " * : < > ? |` and for a name of 256 bytes or more, so rclone stores those as
their fullwidth equivalents and maps them back when listing. rclone's standard
encoding also applies, which is what the drive cannot hold at all: `/` becomes the
fullwidth solidus because it separates path levels, `.` and `..` as whole names
become fullwidth dots, and NUL, the other control characters and DEL become the
visible symbol characters `␀`, `␁` … `␡`. Bytes that are not valid UTF-8 are
escaped too. Spaces, `#`, `%` and a dot inside a longer name are stored exactly as
they arrive, since nothing has to give way for them. `--cloudreve-encoding` changes
which characters are escaped; see
[the encoding section](/docs/#encoding-multi-encoder).

### Folders

`rclone mkdir`, an upload and a server-side move or copy make the whole folder
chain their destination names, one server call per level: the server creates a
single folder per request and reports `40016` when a parent is missing, so rclone
walks down from the first folder that is not there. Writing to a destination
folder that does not exist therefore creates it, as it does on FTP and the local
backend.

### Chunked uploads

The server decides how a file is cut into parts and hands rclone one presigned
URL per part, so there is no chunk size to choose here: the part layout is
whatever the storage policy says. `upload_concurrency` (4 by default) caps how
many parts rclone keeps in flight, and never more than the number of URLs the
session returned. A policy that says the file is not to be cut up gets one part
holding the whole file. The part URLs carry their own credentials, so rclone
sends no session token to the object store.

Each part is buffered and retried on its own: a dropped connection costs that part
again, not the file, and the buffer is rewound before the retry. Because the parts
are read from the source in order, up to `upload_concurrency` part buffers are held
at a time. On the measured site the S3 policy hands out a 150 MiB chunk, so a
160 MiB file becomes two parts (150 MiB + 10 MiB) and roughly 150 MiB of buffer at
the default concurrency; a file smaller than the chunk is one part. Raise
`--transfers` and the total goes up by that many files. If a file cannot be
completed, rclone abandons the session so the server releases the lock on the name -
see [interrupted uploads](#interrupted-uploads).

### Retries

A site's reply, a dropped connection or a page from whatever proxy answers in front
of it is retried through rclone's pacer, which waits 2 seconds at most between
attempts unless the server itself says how long: a `Retry-After` is honoured as
given, whether it comes with a `429` or with an HTML rate-limit page. A business
error - wrong password, name locked, object missing - is an answer rather than a
fault and is reported at once, because repeating it cannot change the outcome.

### Parallel downloads

Reads ask the object store for byte ranges, so rclone's
`--multi-thread-streams` (4 by default) applies to files fetched from this backend:
a file at or above `--multi-thread-cutoff` (256 Mi by default) is downloaded in
parallel streams, each with its own `Range` request. Should the store answer a
ranged request with the whole file, rclone says so and falls back to a single
stream.

The write side does not use that machinery: rclone only multi-threads a transfer
into a destination that supports writing at an offset, which this backend does not,
so uploads are split by the server's part list instead - see
[chunked uploads](#chunked-uploads).

### Deleting

`rclone delete` and `rclone purge` ask the server to delete, which by default
**soft-deletes**: the entry leaves the listing but goes to the trash bin, still
occupying quota, until the account group's retention period collects it. Set
`skip_soft_delete` to remove the object immediately and irreversibly - this is
what releases the space at once, and is what `rclone sync --delete` usually wants
on a quota-limited site.

### Server-side operations

Only a **directory move** is done by the server. The server's move endpoint takes a
destination **folder** and keeps the entry's own name, so moving a folder to a new
name is a move plus a rename, and a rename within one folder is only a rename.

Files are never moved or copied server-side: on this site a move, a rename and a
copy each stamp the entry with the time of the operation, and there is no API to
give a file its modified time back - so rclone transfers the bytes instead, which
does set the time. The measured difference on a live site:

| Operation | Modified time afterwards |
|---|---|
| upload | the source's time, to the second |
| move a file between folders | the time of the move |
| rename a file | the time of the rename |
| server-side copy of a file | the time of the copy |
| move a folder | unchanged for every file inside it |

That last row is why the folder move stays server-side: nothing inside it loses its
time, and rclone does not compare directory times when deciding what to transfer.

`rclone link` returns the **normal download URL** of a file, the same one reads go
through (`POST /file/url`, with the site's `download_host` rewrite applied), minted
for that call rather than reused from the read cache. No share and no stored direct
link is created: it is a signed URL, so anyone holding it can download until the
site's own expiry passes, and it cannot be revoked early. `--expire` has no effect
for the same reason, and `--unlink` is ignored, as rclone asks for a backend that
has no stored link to remove. A folder has no such URL - the site only hands out
entity URLs for files - so asking for one is reported as unsupported.

### Interrupted uploads

An upload session locks the file it writes. If rclone is killed mid upload, that
lock stays until the session times out - `upload_session_timeout` on the server, a
day by default - and the server refuses the name with `40073 Lock conflict`. The
day is an upper bound rather than a timer: the site has no sweep task for this and
collects expired locks when the next lock operation touches them, and the lock table
lives in the master's memory, so restarting it drops every lock. Two things follow
from the kill, and rclone handles both:

- The half-written entry is listed as a file of **0 bytes** and carries the session
  that holds it in its metadata, under `sys:upload_session_id`. rclone hides these
  placeholders from listings and answers `object not found` when one is named
  directly: an entry nobody can open is not a file, and showing it would make a
  re-run compare its transfer against zero bytes.
- Writing over a locked name is refused by default. The server's `40073` answer names
  the lock's token, its path and the account holding it, but nothing that says
  whether the upload behind it is still running: a conflict carries no timestamp, and
  the unfinished entry the lock holds exposes only its session id, with no expiry.
  The one trace of age is the newest version of that file, dated at the start of the
  upload that is writing it - an overwrite leaves the file's own `created_at` alone,
  so that is not the timestamp to read. rclone will not break a lock to write: it
  reports the name as locked and says who holds it. `drop_stale_locks_after` is the
  opt-in: set it above the time your longest upload takes and a name whose running
  upload is older than that is released and written. It stays off (0) by default,
  because taking a younger lock means both sessions are live and whichever callback
  reaches the server first files the name. At 0 nothing times a lock out: the server
  has no sweep, so a name left locked by a killed upload stays locked until it is
  released by hand (`DELETE /api/v4/file/lock` with the tokens the conflict named)
  or the site restarts. That is the price of never cutting off a live upload.
  Deleting is the exception, and it does not wait: asking to remove an entry locked by
  a half-written upload means wanting it gone, and leaving it locked is the deadlock
  where a folder cannot be purged because one file inside it was interrupted. Either
  way a lock is only broken when this account owns it, it was taken by an upload, and
  its path is one this request names; anything else is left alone and the reason
  appears in the error. Set `drop_stale_locks` off to get the server's answer untouched.

What that costs: breaking a lock discards the parts of the upload that held it, so
that file has to be sent again. Two runs of the **same account** writing the **same
name** at once has no correct answer - both sessions can be live, whichever callback
arrives first files the name, the other is refused as a lost lock and its bytes are
discarded - so rclone no longer creates that situation: it will not take over a name
another upload holds, in this process or at any age below the threshold you set.
Keep concurrent runs to different names.

A second shape of `40073` belongs here. The server also answers `Lock expired` with
no holders listed, and that is what a callback hears when the file **was** filed and
filing it released the name - which happens when a reply is lost in transit and the
request is sent again. rclone then reads the name back, and if a file is there with
the size and modified time this upload carried, it reports the upload complete rather
than sending it a second time.

A failed upload that rclone itself abandons releases the lock immediately, and it
does so even after the caller has been cancelled - Ctrl-C still sends the abandon.

To clear a lock by hand - which is what rclone does when deleting the entry it
locks, or when `drop_stale_locks_after` says the lock is old enough to take over -
the token comes from the conflict answer and is released with:

```
DELETE /api/v4/file/lock
{"tokens": ["<token>"]}
```

Abandoning the session instead - `DELETE /api/v4/file/upload` with `{"id":
"<sys:upload_session_id>", "uri": "cloudreve://my/some/folder/name.txt"}` - also
removes the placeholder and aborts the parts in the object store. The field is `id`;
sending the session id as `session_id` is answered `ID cannot be empty`.

## Limitations

- Only the `s3` policy can be written, directly or as a child of a
  `load_balance` pool. Other policies are read-only.
- Moving or renaming a **file** is not done server-side, because the server would
  restamp its modified time; only a directory move is. `--server-side-move` and
  `--server-side-copy` have nothing to act on for files here.
- No content hash, so transfers cannot be checksum-verified.
- The mime type goes with the upload but the API never reports it back, so rclone
  cannot read what a file was given and works out a type from the name instead, as
  any other client of the site does.
- Login captcha blocks the `email`/`password` sign-in route.
- Names are told apart by case on both sites measured: `c1.txt` and `C1.TXT` were
  created side by side in one folder, both listed, and each read back its own
  bytes, so the backend is not declared case-insensitive and rclone treats them as
  two objects. A site whose database folds case rejects or
  folds the second name at the server, not here.
- Streaming uploads are not supported: the size must be known before uploading.
- Some sites cannot take `#`, `&` or `+` in a name at all: the upload session is
  created, the parts are accepted, and the finalising callback then answers with
  the server's generic error. Measured on one site, while two others stored the
  same names without trouble - so it is that site's object store, not a name rclone
  mangles. Such a name is reported as a failed transfer; renaming the file works.
- Names holding `\ " * : < > ? |` are refused by the server, so rclone stores them
  with fullwidth substitutes and maps them back when listing. The bytes on the
  drive are spelled differently in the web UI, but the same name reads the same
  everywhere rclone runs. A name of 256 bytes or more is refused outright.
- Deleted files stay in the trash bin and keep occupying quota unless
  `skip_soft_delete` is set.
- A placeholder left by an interrupted upload is hidden from listings, so the file
  it names is invisible until that upload is finished, abandoned or timed out - see
  [interrupted uploads](#interrupted-uploads). Two rclone runs of one account
  writing one name at once cannot both succeed for the same reason.

## Backend options

Run `rclone help backend cloudreve` for the options this backend defines and their
defaults, and see [rclone flags](/docs/#flags) for the global ones. Those worth
knowing about:

- `location` and `root_folder_id` both say where the remote starts, by path and by
  folder ID; setting both is refused. `location` need not exist yet - rclone makes
  it when it first writes there - and the ID for `root_folder_id` is reported as
  `folder_id` by the [policy command](#policy), for a folder you can already reach.
- `skip_soft_delete` decides whether a deleted file goes to the site's trash bin or
  is gone at once - see [deleting](#deleting).
- `upload_concurrency` bounds how many parts of one file are in flight - see
  [chunked uploads](#chunked-uploads).
- `user_agent` names the User-Agent this remote sends, winning over the global
  `--user-agent`. It applies to the storage requests as well as the API ones,
  since a site behind a proxy that filters clients needs both to look alike.
- `drop_stale_locks` (on by default) releases an upload lock this account left
  behind so the name can be written again - see
  [interrupted uploads](#interrupted-uploads).

### Quota

`rclone about` reports the account's quota: the total is the limit of the
account's group and the used figure is what the account holds, **files in the
trash bin included** - they keep counting until the site's retention period
collects them, so `--cloudreve-skip-soft-delete` is also what frees the space at
once. What the site calls `storage_pack_total` is not reported: nothing in the
answer says whether it is already inside the total, and adding it could overstate
the space.

### User agent

Measured with `--dump-headers`: with `user_agent` set on the remote and a competing
global `--user-agent` on the command line, the requests carried the remote's value;
a remote without the option carried the global one; with neither rclone sends its
own agent. The value is set on the HTTP client rather than per request, so the
presigned part uploads into the object store carry it as well - which is the case
that matters, since a proxy that filters by client sees both kinds of request.

## Backend commands

`rclone backend policy remote:folder` reports the storage policy that uploads into
that folder land on, exactly as the site resolves it, and whether rclone could
write there. A `load_balance` policy is a pool: the answer lists its children, and
those IDs are what `rclone backend set_policy remote:folder <id>` binds the folder
to. The answer also carries `folder_id`, the site's own ID for that folder, which is
what `root_folder_id` wants. The folder always comes from the remote, never as an argument, so the two
commands cannot be pointed at different places; both also run against a served
backend through the rc command `backend/command`.

Binding changes where **new** files go and leaves what is already there alone. The
site has no "inherit again" value, so undoing a binding means naming the policy the
`policy` command reported beforehand. See [folders](#folders) for how a folder's
policy is resolved.

`rclone backend downloadurl remote:folder file.ext` returns the normal download URL
of one path - the same URL reads use (honoring `download_host`), not a share link -
so scripts and rc callers can fetch links without shelling out to a second binary.
The URL carries the server's own expiry; re-request it when downloads start
failing. Via the rc command `backend/command`, name the path in `arg` relative to
the remote root (the handler reads `fs`, `command`, `opt` and `arg` only):

    rclone rc backend/command command=downloadurl fs=remote: -a some/folder/file.ext
