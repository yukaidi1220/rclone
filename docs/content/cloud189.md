---
title: "Cloud189"
description: "Rclone docs for the China Telecom Cloud Drive (天翼云盘)"
versionIntroduced: "v1.76"
---

# Cloud189

[Cloud189](https://cloud.189.cn/) is the personal cloud storage service sold by
China Telecom (天翼云盘). It offers a personal space (个人云) and family spaces
(家庭云), with web, mobile and desktop clients; the desktop client is called
ecloud.

Rclone talks to the same HTTPS API as the ecloud 7.2.4.0 PC client. Every number,
error code and shape described below was taken from captures of that client and
from requests made against live accounts on 2026-10-22; nothing here is inferred
from a third-party driver.

You configure the backend with a 32 character **access token**, which is the
long-lived credential the client obtains when it exchanges a login for a session.

## Configuration

Here is an example of making a cloud189 remote called `remote`. First run:

```console
rclone config
```

Then follow through the interactive setup:

```text
No remotes found, make a new one?
n) New remote
s) Set configuration password
q) Quit config
n/s/q> n

Enter name for new remote.
name> remote

Option Storage.
Type of storage to configure.
Choose a number from below, or type in your own value.
[snip]
XX / China Telecom Cloud Drive (天翼云盘)
   \ (cloud189)
[snip]
storage> cloud189

Option access_token.
The 189 access token.
Enter a value.
access_token> ****

Keep this "remote" remote?
y) Yes this is OK
e) Edit this remote
d) Delete this remote
y/e/d> y
```

To use a family space instead of the personal one, set `space = family`. With
exactly one family drive the backend finds its id on its own; with several it
refuses to guess and asks for `family_id`.

### Configuration reference

`access_token` and `space` are the two the interactive setup asks for. Everything
else below is an advanced option, offered only with `rclone config --advanced`
and always settable with `rclone config set <remote> <key> <value>`:

- `--cloud189-space`: `personal` (个人云) or `family` (家庭云). One remote uses
  one space; a server-side copy between them is the one operation that crosses
  over - see [Server-side operations](#server-side-operations).
- `--cloud189-family_id`: the numeric id of the family space, for example
  `900000001`. Required when the account belongs to more than one family drive.
- `--cloud189-root_folder_id`: the directory to treat as the root. Leave blank
  for the top level of the space. A family drive that has never held anything
  cannot name its own root, so its first folder is created without a parent
  (that is how the official client makes its transfer folder); this option is
  the escape hatch for the paths that still need the number, which are the
  destructive ones.
- `--cloud189-no_refresh`: never exchange the access token again after the
  remote is opened. By default a request the server refuses with
  `InvalidSessionKey` is retried once on a freshly exchanged session. Measured
  2026-10-22 and 2026-10-25, that exchange is safe to share: it hands out a new
  session key (the secret usually changes too) and the key it replaces keeps
  answering, so the official client and other runs are not signed out - six
  exchanges in a row left six keys alive at the same time. Set this to make
  rclone fail loudly instead of touching the login at all.
- `--cloud189-hard_delete`: delete permanently. The drive has no delete that
  skips the recycle bin, so rclone deletes normally and then clears exactly the
  entries it just put there. See
  [Hard delete and the recycle bin](#hard-delete-and-the-recycle-bin).
- `--cloud189-chunk_size`: the base of the part-size ladder, default `10Mi`. The
  server insists on one specific part size per file size and answers anything
  else with `InvalidPartSize` - see
  [The part-size ladder](#the-part-size-ladder). Values below `10Mi` were
  measured to be refused outright (`1`, `2`, `4`, `5`, `8`, `16`, `32` and `64Mi`
  are all `InvalidPartSize` for a small file), and so is `1048577`, one byte more
  than the step.
- `--cloud189-upload_concurrency`: how many parts of one file are in flight at a
  time, default `4`. The server accepts parts out of order within a session and
  assembles them by part index. The account publishes a concurrent-transfer
  privilege (`1` on a plain account, `5` on a platinum one, read from
  `getUserPrivilegesV2`) but nothing measured shows it enforced against this
  number, so raising it is a local choice about memory and connections.
- `--cloud189-max_file_size`: refuse files larger than this. The default `0`
  applies no per-file limit. The account also publishes `transFileSize`, but it
  is not a limit that bites: on the accounts measured it carries the same number
  as the day's allowance (`transDayFlow`), and the allowance is what actually
  refuses a file. Both are int64 max on an upgraded account, which is why no
  limit is hardcoded anywhere, and why the backend does not turn that number
  into a per-file cap the drive does not enforce.
- `--cloud189-no_quota_check`: skip the pre-checks and let the server refuse
  instead - see [Quota and space pre-checks](#quota-and-space-pre-checks).
- `--cloud189-fatal_if_no_space`: make an exhausted day allowance or a full drive
  end the whole run instead of only that file. Neither condition can clear itself
  while a run is going, so both refusals stop the retries of the file that hit
  them; `--retries` would still re-run the transfer list. With this set the run
  stops at once, which leaves the rest of the list untouched rather than failed -
  check what is missing afterwards. A file refused for being over a single-file
  ceiling is not one of those two conditions and never ends the run, whatever is
  set here.
- `--cloud189-upload_zone`: send all upload traffic to this origin instead of the
  one the server names, for example `https://upload.example.net`. Both the
  session host and the storage host inside every presigned part URL are
  rewritten, the path and query of each URL are not. Use it to route uploads
  through a proxy you run. The session key and the file contents both go to that
  host, so only point it at a server you control, and it must present a TLS
  certificate rclone accepts.
- `--cloud189-disable-http2`: use HTTP/1.1 keep-alive instead of HTTP/2, so that
  concurrent part uploads spread over parallel TCP connections rather than
  sharing one. Measured live 2026-10-23: with HTTP/2 left on, one read of a
  21 MiB file died mid-transfer with `stream error: INTERNAL_ERROR; received
  from peer`, once in a run of some four hundred requests. The throughput
  argument alone is reason enough to set it on a long run.
- `--cloud189-encoding`: the default is `Standard` plus `EncodeDoubleQuote`,
  `EncodeAsterisk`, `EncodeColon`, `EncodeLtGt`, `EncodeQuestion`,
  `EncodeBackSlash`, `EncodePipe`, `EncodeSingleQuote` and `EncodeInvalidUtf8`,
  and it should normally be left alone - see
  [File names](#file-names).

## Credentials and sessions

The access token is the credential you keep. Each run exchanges it with
`getSessionForPC.action` for a session key and a session secret; the secret signs
requests and encrypts the parameter block, and the key names the session. The
same response carries a second key, `familySessionKey`, which is what signs every
`/family/...` request and which ends in `_family` in every capture. Its
`familySessionSecret` has equalled the personal secret in every capture, but the
field is read separately, so a server that changes that does not silently break
family traffic.

The exchange does not revoke the session it replaces: six exchanges in a row, one
after another, left all six keys answering at once (2026-10-25), so a run, the
official client and other runs can be open at the same time. How long an old key
keeps answering, and whether an account holds any cap on how many can coexist,
has not been measured.

When the server rejects a session mid-run, rclone exchanges it and rebuilds the
one request that failed. The request reports which key it was signed with, so
the copy threads that all notice the same dead session share a single login
rather than each performing one.

### Several remotes of one account

Remotes that carry the same access token share one session in the process: a
personal remote and a family remote of one account are two configuration
sections, not two logins, and neither needs its own copy of the key. The exchange
answers with both keys at once, so a login made by any one of them serves the
rest; when one of them finds its key refused it exchanges, and the others pick up
the new key with their next request. The access token itself is never rewritten.

There is one family session key per account, because the response carries one
field for it, so several family drives on one account share it. An answer that
does not name a family session leaves the one in use alone rather than clearing
it for the other remotes. All of this holds on the refresh path as well as at
start-up, and `no_refresh` is honoured on both: a refused session is then
reported as such and the login is not touched.

### What reaches the logs

The account's login name (`loginName`, which the session answer carries) is
written to the debug log with the phone number masked except for its last two
digits; an email address is written as it is. Session keys are never printed at
all - not by the backend and not by the live probes - because a session key signs
requests; the probes report a key's length, whether it changed, and whether the
family key still carries its suffix. `--dump` is the one place this backend
cannot redact for you: it prints what the transport carries, including the
`sessionkey` and `signature` headers, so don't share those dumps.

## File names

The set of characters the default encoding escapes is exactly the set this
server damages, measured by uploading and reading back 54 candidate names plus 12
fullwidth substitutes:

- `"` `*` `:` `<` `>` `?` `\` `|` are **deleted silently** from the name, and a
  lone `'` becomes `\'`. There is no error, and the original cannot be fetched,
  so rclone replaces each one with its fullwidth lookalike, which the server
  stores verbatim.
- Everything else is stored as sent, on purpose: leading `.`, `~`, tabs,
  newlines, `& = % # + , ; @ ( ) [ ] { } $ ! ^`, names of 300 ASCII characters or
  200 Chinese ones, and `..` (which is a legal file name here). Escaping any of
  those would invent a restriction the server does not have and would make rclone
  rename files the official client stores without complaint.
- Leading and trailing **spaces** are the exception, and newer than the first
  table: the drive strips them from file and directory names alike. Uploading
  ` film.mkv` stores `film.mkv`, and the rename that installs the object under
  the wanted name is then refused as already existing, so such a name could not
  be uploaded at all. Both boundaries are therefore escaped. A space inside a
  name is stored as sent and is not escaped. Measured 2026-10-23 with the
  integration suite and then directly on the drive with a leading-space file, a
  trailing-space file, and two directories.
- `EncodeInvalidUtf8` is required rather than cosmetic: names travel inside an
  encrypted form string, where Go would replace invalid bytes with U+FFFD and
  make the name irreversible.

Two things are refused locally, before a byte of the source is read:

- A name containing a character above the BMP (an emoji, most CJK extension
  ideographs) is rejected with a no-retry error. The drive answers
  `403 EmojiArgumentError` - but only at commit time, after the whole file has
  been transferred and charged to the day's allowance.
- Length is deliberately not checked. There is no measured limit to enforce, and
  inventing one would block names the official client accepts.

Names differing only in letter case were not probed, so rclone treats this drive
as case-sensitive.

## Modified time and hashes

Every file on this drive carries an MD5, and listings return it, so `rclone
check --checksum` and size-only comparisons both work, and the MD5 of a file
never has to be recomputed from the download.

Modification times are read-only. The drive stores `lastOpTime` and `createDate`
as wall-clock text with a one second resolution, in the account's zone (UTC+8),
and there is no API to set either one: the upload session is opened with a name,
a size and digests, and no time. rclone therefore reports
`fs.ModTimeNotSupported` for this remote, which means:

- `sync` and `copy` compare by size, or by MD5 with `--checksum`, and never by
  mtime;
- asking rclone to set a time answers `fs.ErrorCantSetModTime` together with the
  reason, which is what lets `rclone touch` and the copy internals say
  "unsupported" rather than "the transfer failed";
- `rclone bisync --compare modtime` is refused with a clear message rather than
  producing a wrong answer, and bisync's default size-and-hash comparison works;
- `lastOpTime` moves whenever the drive touches the object, so a server-side copy,
  rename or move refreshes the time a listing shows. rclone does not re-read the
  time after such an operation - nothing it can do with a remote that cannot be
  told a time depends on it - so `lsf --time` after a move shows the time the
  object had before it moved until the next listing.

## Uploads

The drive will not accept a stream. The first move of an upload is to declare the
file size, the whole-file MD5, the MD5 of every part and an aggregate of the part
digests, and only then does it hand out presigned URLs. So rclone must know every
digest before the session opens, which means the source is read once for hashing
rather than twice for hashing and transferring. A source of unknown size
(`rclone cat | rclone copy` style pipes) is refused with a no-retry error.

A file no larger than one part is read into memory whole. Anything larger is
first staged to a temporary file, made in the system temp directory (the same
convention the yun139 backend in this fork uses; `TMPDIR` decides where it lands)
and then read from at the offsets each part needs. Multi-threaded copies go
through `OpenChunkWriter`, which reserves the same kind of file up front and
writes each arriving part at its offset, so `--multi-thread-streams` works on this
backend.

If the source changes size while it is being staged, or carries bytes beyond the
size rclone was told about, the upload is refused rather than sent with digests
that describe different content.

### The part-size ladder

The server demands one specific part size per file size. Measured 2026-10-22 by
declaring sizes without transferring them, with the default base of `10Mi`:

| file size | part size the server accepts | parts |
|---|---|---|
| up to about 9.75 GiB | 10 MiB | up to 999 |
| about 9.75 GiB to 19.5 GiB | 20 MiB | up to 999 |
| above that | a multiple of 10 MiB, at least 50 MiB | up to about 1999 |

A `20Mi` part size is refused for a `20Mi` file and for a `2Gi` one, and accepted
for an `11Gi` one. What the server really checks is the part count; the size is
how that is spelled on the wire, and `chunkPlan` implements the rule so the count
always lands inside it.

### Part digests, and what the server does with them

The declared per-part digests travel as `partInfo=1-<base64>,2-<base64>,...` and
the server echoes each one back inside the `Content-MD5` header of that part's
presigned URL, which rclone replays unchanged, along with the `Authorization` and
the `x-amz-limit: rate=<KB/s>` header the same response carries. That rate header
has equalled the account's `transSpeed` privilege in the captures (`12800` on an
ordinary account), so the throttle is the storage layer's, not rclone's choice.

Measured on 2026-10-22 with deliberately wrong part digests: **the storage layer
does not verify them**. A part whose bytes do not match the digest it declared is
accepted, and the file commits. So the digests are the client's bookkeeping, not
a server-side integrity check, and rclone keeps its own: it computes the whole
file's MD5 from the staged bytes and compares that against the `fileMd5` the
commit reports. A mismatch is an error, and the object is not left under the name
it was meant to have.

### Retries, and which flag governs them

There are three separate retry levels, and they are governed by different flags:

- one part: `--low-level-retries` attempts, with a backoff that starts at 500 ms
  and doubles to 5 s, jittered per part. A part refused with HTTP 400 can be sent
  again on the same presigned URL and the upload then commits with the right
  digest, so the session is not poisoned by one bad part.
- a part that has used up its attempts: the error is marked so that the *whole
  file* is retried under `--retries`, not the part under another round of
  `--low-level-retries`.
- the source itself: it is read once, in full, before the session is opened, so
  there is no mid-file re-read to retry. A part that fails is re-sent from the
  staged copy, never from the source.

Errors the drive will keep refusing - a name it cannot store, a session that
`--no-refresh` forbids refreshing, a full drive or spent allowance - are all
marked no-retry, so none of them burns `--retries` worth of whole-file uploads.

### Rapid transfer

If the account already stores content with the same digest, `initMultiUpload`
answers `fileDataExists: 1` and rclone commits immediately without sending any
bytes. This costs a full MD5 of the source, which this backend reads anyway
before opening the session, so it is free. Bytes that do go up are counted locally
between quota refreshes, so the next file's pre-check sees what this process has
already sent.

## Server-side operations

`Copy`, `Move`, `DirMove`, `Purge` and deletions are batch tasks:
`createBatchTask.action` followed by `checkBatchTask.action` until the task
reaches state 4. Nothing crosses the wire except ids.

A finished task's counters are the trigger, not the verdict. `res_code` stays `0`
and HTTP stays `200` even when the server did nothing at all: measured 2026-10-22, a
family delete came back with `failedCount: 1`, no `errorCode` and no message of any
kind, and the objects were still on the drive. So `failedCount`, `skipCount` and
`successedCount` decide whether a task is *suspect*, and a task that reports fewer
successes than objects were sent, or skipped anything, is not accepted at its word.
What settles a suspect **delete** is the drive's own evidence, in this order:
`successedFileIdList`, which names the objects the task handled (captured 2026-10-22
for MOVE, DELETE and CLEAR_RECYCLE; a successful COPY does not answer it), and then a
listing of each parent directory, read to its end - the drive's own `count`, which
measured 2026-10-28 is the number of rows in the directory rather than on the page,
has to agree with the rows actually seen before an absence may be read as a deletion.
`skipCount` is the one counter whose meaning has not been measured: a name conflict
does not use it (captured: `failedCount 1, skipCount 0, errorCode FileAlreadyExists`),
so a skip is reported as "the drive declined these and did not say what became of
them" rather than being explained.

The `fileName` a `COPY` or `MOVE` entry carries is **ignored**: the object lands in
the target folder under its **source's** name (measured live 2026-10-24, with the
command and the readback both kept: a copy of `A.txt` asked to arrive as
`sub/B.txt` came back as `sub/A.txt`, and a move of `deep/M.txt` asked to arrive as
`sub3/N.txt` came back as `sub3/M.txt`). So a server-side copy or move that also
renames is one task plus one rename by id, and the name that has to be free in the
destination before the task starts is the source's spelling.

`COPY` reports no id for the object it made (unlike `MOVE` and `DELETE`, which do),
so the new object is read back from the destination listing by the name it arrived
under, excluding the source's own id. Handing back a row that is the source would
make every later operation on the "copy" address the original.

A copy inside the directory that already holds its source cannot be done at all:
the copy would have to take a name its own source is holding, and the drive refuses
that instead of duplicating the object (measured as `COPY` failing with
`FileAlreadyExists`, and on 2026-10-23 as a task stopped in the state that waits
for a client to answer). `Copy` returns `fs.ErrorCantCopy` for it, which lets rclone
transfer the bytes. A move into the directory that already holds the object is
equally not a task, and for a worse reason: addressed at its own directory, the task
has been measured to destroy the object (2026-10-23, on a file). So both `Move` and
`DirMove` do that case as a plain rename and start no task - for a directory the
subtree rides along with it.

The object a directory move leaves behind is checked before anything is deleted for
it: the destination must hold the moved directory under the name the task left it
in, and the drive keeps its listings a second or two behind, so that readback waits
the way a created directory's id does. Nothing that was already on the drive is
deleted for a move that cannot be seen.

### When the name is already taken

The drive never overwrites and never auto-renames. A task that lands on a name
already in use stops in state 2 and waits for the client to answer, which this
backend does not do (the conflict endpoints are captured but not implemented). So
rclone clears the name first, addressing objects by id throughout:

1. rename whatever holds the name to a unique `rclone-park-<unixnano>-<hex>` name,
2. rename the object this run made into the name it wanted,
3. delete the parked object.

For an upload all three steps are rclone's own renames. For a server-side copy or
move the arrival takes step 2's place: the task runs while the name it will land on
is clear, and step 2 then renames the arrival into the name rclone asked for.

Replacing a file (`Update`) parks the old object **before** the new bytes go up, so
the upload arrives wearing the name it was asked for and no rename follows it. The
drive will not overwrite: an upload onto a name still in use is stored beside it
under a name the server invents, `movie(20261024123855).bin` (measured live
2026-10-24). Freeing the name first avoids that invented name, and it also avoids
hitting one name with a park, a rename and a delete inside a single second - which
is measured to leave the drive's listing seconds behind, long enough that rclone
could not read back a file the swap had in fact replaced. One case must not be
deleted: because the drive deduplicates by digest, content identical to the object
just parked can come back as **that object's own id**. It is the original, so it is
renamed back into place and never dropped.

A replacement large enough for `--multi-thread-streams` (256 MiB by default) is
not an `Update`: the parts are staged first and the drive only sees one upload at
the end, so that path cannot free the name before the bytes exist. It parks,
renames and deletes on the same name inside one second, which is the shape
measured to leave the listing behind - so a large replacement still pays the lag
that a small one avoids.

Which parked object is deleted and which is put back is decided by one question:
was it the object rclone was asked to replace? When the operation keeps the name,
the object standing where the arrival has to land is exactly that one, so it goes.
When the operation also renames, that object merely happened to own the name the
drive lands arrivals under; it is given its name back and stays on the drive. Only
the object holding the name rclone actually wanted is replaced.

Handing that name back is not always one rename. Once the task has run it has put
this run's object on the arrival's name, so a restore would collide, and the drive
does not overwrite. The moved object is set aside first: both stay on the drive, the
object that was there before the run keeps its name, and the log says which park
name the moved bytes are left under.

The window in which a name holds nothing is normally one rename. Two paths are
wider and mean it. A replacement (`Update`) frees the name before the new bytes go
up, so the name is empty for the whole transfer - which is the price of the drive
not overwriting an upload in place, and the reason a killed run can leave the only
copy of a file named `rclone-park-…`. And if step 2 fails, the parked object is
given its name back before anything else - on a context detached from the caller's,
since the commonest way to fail part-way through a swap is the user stopping the
run, and an object left named `rclone-park-…` reads to the next `rclone sync` as a
file to delete. For an upload the object this run made is then deleted; a task that
failed made nothing, so there is nothing to take back. The one case with no clean
answer is a `COPY` whose task reported success but whose object cannot be read back
out of the destination: it stays on the drive, because an object with no known id
cannot be addressed, and the error says so.

Nothing that was on the drive before the run is deleted on the strength of a task's
counters. Before a move or a replacement removes the object it displaced, the
destination is read back and has to be holding this run's object - by id, not by
name, and not by what the commit or the task reported.

A directory move is refused when the name it has to take is held by a file, before
anything is parked; the directory cache only refuses a destination that is
already a directory.

A directory is never moved aside for a file, and a file is never replaced by a
name that a directory holds: the delete that follows a successful task is
recursive, so clearing a name by parking a subtree would put that subtree one
failed step from removal. Both cases are refused locally.

An `rclone-park-…` object left on the drive means a swap was interrupted. The
restore runs on a context detached from the caller's, so stopping a run with
Ctrl-C and a request timeout both still hand the name back; what survives neither
is the process being killed outright. The parked object is then the version that
held that name before the run, sitting beside the one that holds it now, and
`rclone sync` counts it as an extra file - so look at it before letting a sync
delete it.

### Directories and deletion

`DirMove` is a single `MOVE` task on the directory: measured with a directory of
18 files, it moved the whole subtree in one task that reported one object done.
A family remote whose root id could not be read refuses `Purge` and `Rmdir`
outright: the id is learnt from a listing, that listing came back empty once on a
drive that still held 24 GiB, and a purge aimed at the alias behind it deleted
what the alias named. Nothing destructive is sent until `family_id` or
`root_folder_id` names the drive. Creating a folder is the exception: a
top-level one is made without naming a parent, which is the only shape the
server accepts on a drive that has never held anything, so a migration can
start there.

`Purge` of a directory is likewise a single recursive `DELETE`, so a deep tree
costs one request rather than one per entry. `Purge` of a remote's own root
empties it in one multi-object task; the drive's top level itself is emptied,
never deleted, and the same is true of the directory a remote was opened at.
That anchor is the directory the remote resolved at open time, not the space's
top level: a purge of a subdirectory remote used to name the top level's
contents, which is how a family drive came to be emptied from inside the test
suite.

A single-object `DELETE` task sometimes ends with one object failed and no reason
given at all - `Couldn't delete: ... the server gave no reason` - and the object is
still on the drive. Measured 2026-10-28 on a 40-object fixture: a pass sees 9 to 12
of its 40 delete tasks refused this way, and the same targets sent again usually
finish (47 of 63 refused tasks cleared on the first repeat). After both repeats a
pass ends with nothing refused or one object left refused - three passes measured.
Nothing about the refusal depends on how many transfers are in flight -
`--transfers` 15, 5 and 3 produced the same counts - and waiting for the drive to
work it out does not help: with the repeat switched off and a single pass, 17
refused objects were still listed ninety seconds later. So the backend asks the
drive what it holds and sends a refused delete back up to twice, the listing having
the final word each time. A refusal that did give a reason (a full drive, a spent
day) is not repeated, because that answer does not change inside a run. Five passes
run with this tolerance left 1, 1, 0, 0 and 0 objects refused at the end of a pass,
and rclone's next pass cleared each of them, so all five exited zero. What a last
pass still cannot delete is reported, and `--retries` counts it: rclone exits
non-zero when the last pass fails, however many passes before it recovered, so a run
that must delete a lot needs enough retries rather than trusting this backend to
have absorbed everything.

`Rmdir` refuses a directory that still has children - the drive cannot remove one
without taking them - and refuses the drive's top level. The root of a remote
opened below the top level is removable when empty, which is what the test
suite's teardown and `rclone rmdir remote:sub` mean. A deleted directory is dropped
from rclone's id cache, so a later `mkdir` of the same name creates it again
instead of writing into an id the drive no longer has.

### Which directions are supported

Server-side copy works inside one space, from the personal space to a family
space, and from a family space to the personal one. The task for a
family-to-family copy between two different families was never captured, and
neither was a cross-space `MOVE`; those are refused, and rclone moves the bytes
instead. Copying between two remotes with different access tokens is refused the
same way.

## Quota and space pre-checks

On the first upload of a run - and again at the open of the remote, where a failed
read is retried rather than left unloaded - the backend reads
`getUserPrivilegesV2` (the day's transfer allowance, what of it is used, the
single-file ceiling) and `getUserInfo` (capacity and free space), and refuses,
before reading the source or writing a temp file, what the server is known to
refuse:

- a file that would take the run over the day's allowance,
- a file larger than `--cloud189-max_file_size`, when that is set,
- a file larger than the free space on the drive.

Those figures are re-read when a refusal is about to be made, so an allowance
spent elsewhere is noticed. That re-read is made at most once a minute for a
remote, because a run that has genuinely used up its day would otherwise pay two
metadata requests to every remaining file just to be refused again; between
re-reads the local count keeps moving, so what a file is refused on is at worst a
minute behind. Bytes that go up are added to a local count so the next file sees
them. An endpoint that answers with a refusal in `res_code` is treated as a
failure, not as an account with no limits - a silently empty privilege list would
otherwise look like an unlimited account.

The server's own refusals are `UserDayFlowOverLimited` and
`InsufficientStorageSpace`. Neither can clear itself during a run, so both stop
the retries of the file that hit them; `--cloud189-fatal_if_no_space` turns them
into a run-ending error. A file over `--cloud189-max_file_size` is not one of
those two: it is refused without another attempt and the rest of the run
carries on whatever `fatal_if_no_space` says. `--cloud189-no_quota_check` skips the
pre-checks and leaves the server to refuse: that saves the two requests per run,
`getUserPrivilegesV2` and `getUserInfo`, not the transfer.

`rclone about` reports total, free and used space and logs the day's allowance,
what has been used of it and the single-file ceiling. Free space is read from the
field called `available`, which is negative on an over-quota account; rclone
clamps it to zero for `about` but the gates use the real number.

## Hard delete and the recycle bin

A deleted file goes to the recycle bin and stays there for the account's
retention window - the account publishes it as the `recycleDay` privilege, and
that number is part of what a membership buys: `10` days on an ordinary account
became `60` on the same account after it was upgraded (measured 2026-10-22).
rclone does not read that number, because it cannot act on it; it is here so the
wait is predictable. While a file sits in the bin it still counts against the
quota, so space is only freed by a `CLEAR_RECYCLE` task, and
`--cloud189-hard_delete` is the option that sends it: rclone deletes as usual,
then clears exactly the ids it put there. What is cleared cannot be restored.

Restoring from the recycle bin is not implemented: the drive can do it, for files
and directories alike, but rclone has no operation that maps onto it.

## Downloads

Reading a file takes two steps: `getFileDownloadUrl.action` mints a direct link
for one object id, and the content is then read from the storage host that link
names. rclone caches the link per object, for 3 minutes on a personal remote and
8 hours on a family one, because the link's own life is fixed by the server and
not by the client: captured 2026-10-22, a personal link's expiry sits about 5
minutes after it is issued and a family link's about 10 hours after, so a cached
link is still valid when it is handed out.

The link comes back HTML-escaped - the family endpoint in particular writes
`&amp;` inside its own query string - and is unescaped before use. Requested as
written, the storage layer refuses it.

The URL's query is a signature; rclone strips it out of any message or log line
that would otherwise echo it back. That covers the whole redirect chain, not just
the link that was asked for: a transport failure after the redirect quotes the
address it was sending to, which is the storage URL with a signature of its own.

`rclone link` hands out the address the bytes actually come from, not the
`download.cloud.189.cn` endpoint. That endpoint only redirects - one 302 lands on
a presigned storage URL (measured 2026-10-27, a ctyun OBS host) - and the storage
host is what a direct-link rule keys on, so following the one hop costs a
single-byte request and gives the caller something it can decide on before asking.
`--expire` has no effect: `getFileDownloadUrl.action` takes no expiry parameter, so
the server picks the window. `--unlink` has nothing to revoke, since the link is
minted per request and stored nowhere.

The storage grant behind that address is shorter than the redirecting form: on the
family space it is 5 minutes against about 8 hours. A family link passed on from
`rclone link` therefore lives minutes where the redirecting form would have lived
hours, and rclone reports the shorter window when it hands one out. A client that
needs the longer life should take the redirecting form and follow the redirect
itself, which also renews the grant on every hit.

The storage host is also measurably unreliable over long transfers: on 2026-10-24
the standard suite's 21 MiB read failed with `unexpected EOF` on three runs in a
row, while the same object read back whole minutes later and a ranged request
answered at once. So a body that stops before the last byte the drive announced
is continued: the rest is asked for from the byte already delivered, on a fresh
link, up to four times per read. An object cannot change under a reader, because
it is addressed by id, so the whole download is still one object's bytes - a
dropped connection costs time rather than the file.

## How to reach the service

The endpoints are `https://api.cloud.189.cn` (the session call, listing, tasks,
account, download links) and `https://upload.cloud.189.cn` (the upload session and
the presigned part URLs). The part URLs and download links name a storage host of
the server's choosing, and rclone follows them there. Nothing else is contacted,
unless `--cloud189-upload-zone` is set.

## Sharing one account

The day's allowance is one budget for one account: rclone, the official client and
any other program draw from the same `transDayFlow`, and rclone cannot see what
the others sent, which is why the figures are re-read before a refusal.

Deleting and re-uploading the same content is not free either: with
`--cloud189-hard_delete` off, deleted files keep holding their space until the bin
is cleared, so a run that replaces large files can be refused for space the
account appears to have.

## Security notes

- The access token is the long-lived credential, and it is what to treat as a
  password. rclone marks the option sensitive, which keeps the value off the
  terminal echo while it is typed and replaces it with `XXX` in `--dump` and debug
  output. It does not change what is stored: `rclone config create …
  access_token=…` writes the token to the config file exactly as given (measured
  against this backend's own binary), so obscure it with `rclone obscure` or
  protect the file with a config password if that matters on the machine.
- Request signing is HMAC-SHA1 over the session key, operate, request path, date
  and encrypted parameters, and the parameter block is AES-128-ECB under the
  session secret. Both are what the service does, not choices rclone can change;
  the practical protection is TLS to the endpoints named above.
- Presigned part URLs and direct download URLs carry live signatures in their
  query. rclone rewrites them out of errors and logs.
- `--cloud189-upload-zone` sends the session key and the file contents to a host
  of your choosing, which is a deliberate trust decision, not a routing hint.
- `--dump headers` and `--dump requests` are not clean on this remote, because
  rclone scrubs only `Authorization` and `X-Auth-Token` from dumped traffic. The
  live session travels under names neither of those covers - `sessionkey` and
  `signature` on every API call - and the session exchange puts the
  `accessToken` itself in the request URL. A dump taken against a cloud189 remote
  is therefore credential material: avoid it on shared terminals, and treat any
  log that has one in it as a secret.
- Ordinary failures are not. Go's transport errors quote the whole URL they were
  sent to, and on this drive a URL is never only a URL: the exchange URL carries
  the access token, and a presigned part or download URL carries a live signature
  for that file. A dial timeout or a reset connection on any of the three would
  otherwise write a credential into the log, so those messages are scrubbed while
  the error underneath stays intact for rclone's retry rules. The same applies to
  what the storage layer puts in the *body* of a refusal: it echoes back the
  request it refused, query included, and that text is redacted before it reaches
  an error message.

## Limits of this backend

- The name-conflict endpoints are captured but not implemented, so a task that
  stops in state 2 is reported as an error rather than answered.
- Aggregate throughput has not been measured on this backend, so this doc makes no
  claim about speed. The server does publish a throttle - the `transSpeed`
  privilege, echoed as the `x-amz-limit` header on every part upload - but the
  measured accounts' other limits (`transConcurrent`, `transQos`) turned out not
  to be enforced against rclone's requests, so one unmeasured number cannot be
  read as a ceiling either.
- Case-only differences in names were not probed - see [File names](#file-names).
- The standard integration suite runs against this backend in a directory of its
  own (`fstest/test_all/config.yaml`). It refuses empty files and cannot set
  modification times, which the suite skips rather than fails.
- One run left an `rclone-park-…` object behind although its delete task had
  reported success (2026-10-25, for an object parked two seconds earlier), so
  such an object can survive a replace. It is visible in listings and safe to
  remove by hand.
- A zero-byte file cannot be uploaded: whether a multipart session accepts one
  has never been measured, and the protocol's first move is to declare a size, so
  rclone says it cannot rather than guessing. The sibling drives in this fork
  answer the same way for the same reason.
