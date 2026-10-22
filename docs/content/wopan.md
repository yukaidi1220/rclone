---
title: "Wopan"
description: "Rclone docs for the China Unicom Cloud Drive (联通云盘)"
versionIntroduced: "v1.76"
---

# Wopan

[Wopan](https://pan.wo.cn/) is the personal cloud storage service offered by
China Unicom (中国联通). It offers personal and family cloud spaces with web,
mobile and desktop clients.

Rclone accesses Wopan through its mobile "wohome" API, the same protocol used
by [OpenList](https://github.com/OpenListTeam/OpenList). You need a
`refresh_token` to configure the backend.

## Configuration

Here is an example of making a wopan remote called `remote`. First run:

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
XX / China Unicom Cloud Drive (联通云盘)
   \ (wopan)
[snip]
storage> wopan

Option refresh_token.
Refresh token. Obtain from an existing OpenList wopan storage config, or by
capturing the app login flow.
Enter a value.
refresh_token> ****

Keep this "remote" remote?
y) Yes this is OK
e) Edit this remote
d) Delete this remote
y/e/d> y
```

### Configuration reference

The advanced options are:

- `--wopan-access_token`: the access token, refreshed automatically from the
  `refresh_token`. Do not set it manually unless you want a fully static
  token.
- `--wopan-family_id`: the ID of a family (家庭云) space. Leave blank to use
  the personal space.
- `--wopan-root_folder_id`: the folder ID to use as the root. Leave blank for
  the top level of the space.
- `--wopan-no_refresh`: never call the token refresh endpoint. **Set this when
  the same account is also in use by another program such as OpenList** - see
  [Sharing an account with OpenList](#sharing-an-account-with-openlist) below.
- `--wopan-hard_delete`: delete files permanently instead of moving them to
  the recycle bin. Deleting files from the recycle bin still works, so quota
  is only released immediately with this flag. Note that permanent deletion
  works by purging the file from the recycle bin: when the bin holds more
  entries than the server is willing to page out, files beyond that page can
  only be moved into the bin (rclone logs a warning) - empty the recycle bin
  from the app to restore full hard delete.
- `--wopan-upload_zone`: upload endpoint override, e.g.
  `https://tjupload.pan.wo.cn`. Leave blank to use the zone the server
  assigns per account (recommended). When set, all upload traffic - file
  contents and the access token included - goes through the given host, so
  only point it at a server you trust, such as your own reverse proxy. The
  URL must present a valid TLS certificate.
- `--wopan-upload_cutoff`: files above this size are uploaded in chunks of
  `--wopan-chunk_size` instead of a single request. Defaults to `64Mi`.
  Smaller files are sent as one request, where the server's ETag is the
  content MD5 immediately at upload time, while chunked files never carry a
  content MD5 at upload time - see
  [Chunked uploads](#chunked-uploads) below.
- `--wopan-chunk_size`: chunk size for chunked uploads. Defaults to `8Mi`
  (the minimum is `5Mi`; smaller chunks have triggered a server error at
  upload completion). The last chunk absorbs the remainder and may reach
  roughly twice this size, matching the official client.
- `--wopan-upload_concurrency`: how many chunks of the same file are uploaded
  concurrently. Defaults to `4` (the minimum is `1`). The server accepts
  out-of-order parts within one upload session and assembles them by part
  index - see [Chunked uploads](#chunked-uploads) below.
- `--wopan-no_member_check`: skip the membership probe, and with it the notice that
  probe makes possible. rclone never blocks a file on the reported size anyway -
  see [Membership and the single-file upload
  size](#membership-and-the-single-file-upload-size) below - so this only removes
  the request and those log lines. Defaults to `false`.
- `--wopan-quick_transfer`: ask whether the server already stores content with the
  same SHA-256 before uploading, and if it does, create the new directory entry
  without sending any bytes. Off by default because the answer costs a full hash
  of each source - see [Quick transfer](#quick-transfer) below.
- `--wopan-fatal_if_no_space`: make the exhausted-quota answer (`RSP_CODE 2001`)
  cancel the whole run instead of only that file. Freeing space is a manual act
  outside the run, so the no-retry marking on that answer stops the low level
  retries but leaves `--retries` to re-run the transfer list: a real run on 2026-10-21
  spent 32 minutes failing 126 file paths about ten times each. Defaults to
  `false`. Cancelling early means the rest of the list is never attempted, so
  check what is missing afterwards rather than reading the abort as a full pass.
- `--wopan-encoding`: the encoding for the backend. The default is `Standard`
  plus `EncodeQuestion`, `EncodeAsterisk`, `EncodeLtGt` and `EncodeInvalidUtf8`
  and should normally be left alone. The server rejects `?`, `*`, `<` and `>`
  in a file name, so the encoder escapes them to their fullwidth equivalents,
  which the server stores verbatim. The other characters those flags would
  cover in a Windows-style encoding (`:`, `"` and `|`) are stored verbatim and
  are deliberately left unescaped, as are trailing spaces, dots and tabs.

## Modified time and hashes

Wopan stores the modification time of files with a precision of 1 second.
The modification time is set at upload time and preserved by server side
copies and moves.

Wopan reports an MD5 hash, taken from the ETag of the object. It is not free and it
is not everywhere: reading one costs a request to sign the download URL plus a HEAD
of that URL, and **objects above `--wopan-upload_cutoff` have no content MD5 at all**
(their ETag is the multipart kind), where `Object.Hash` reports "no hash". rclone
therefore marks the hash as slow and fetches it only when a comparison needs it.

That last point changes what `--checksum` means here: when the destination has no
hash, rclone gives up on the hash and reports the two objects as equal, so for files
above `upload_cutoff` `--checksum` is *weaker* than the default size-plus-mtime
check, and no content verification is available at all through rclone's hash path -
the only way to verify such a file is to read its bytes back. **Using `--checksum`
is the recommended way to run sync and copy against wopan for files below
`upload_cutoff`**; without it,
rclone compares sizes and modification times.

Files of 8 MiB or more are stored in server-side shards, however they were
uploaded, and the ETag depends on how many parts the upload had:

- **Single-part uploads** (rclone uploads up to `--wopan-upload_cutoff`)
  carry the true content MD5 in the ETag, ready within seconds of
  completion. The download link can transiently fail for a few minutes
  after a large upload; rclone tolerates that window instead of reporting
  a missing hash.
- **Multi-part uploads** - rclone uploads above the cutoff, and any file
  uploaded by other clients as true multipart - keep an S3-style composite
  ETag (`md5-N`) forever. Its MD5 part is a digest of the part digests,
  never the content MD5.

rclone reports a composite ETag as "no hash available", so `--checksum`
**silently degrades to size-only comparison** for those files - this is by
design so that verification does not delete valid objects.

## Chunked uploads

Files no larger than `--wopan-upload_cutoff` are sent as a single upload
request. Larger files go through the service's chunked upload protocol:

1. rclone splits the file locally into parts of `--wopan-chunk_size` with
   the official client's floor-division rule; the last part absorbs any
   leftover bytes and can reach roughly twice `chunk_size`. No upload plan
   is fetched from the server.
2. All parts travel through one upload session (a single `uniqueId`), posted
   by `--wopan-upload_concurrency` workers. Each part is read from its own range of
   the source and buffered before it is posted; parts are posted concurrently, so
   arrival order is shuffled; the server reassembles the file by part index, and
   out-of-order arrival has been verified against the live service. The buffering
   stays even though parts can be re-posted, because it also protects a source
   whose `Open` does not honour the requested byte range.
3. A part whose POST fails at the transport level is re-posted within the same
   session - only that part, up to `--low-level-retries` attempts for it, with a
   short wait between attempts. A re-post has to produce the same bytes again from
   the start of the part; if the source cannot be rewound there, nothing is
   re-posted and the original failure is reported.
4. The finished file ID comes back with whichever part's response completes the
   set, not with the highest part index. Measured 2026-10-21: a session that is
   missing a part answers the final part's POST with success and no file ID, and
   the part that later fills the gap returns the ID itself. Once a session has
   produced an ID every part of it is stored, so no part is re-posted after that -
   measured on the same day, a re-post after completion is answered with success
   and ignored, and never reports the ID a second time.

Memory usage is about `chunk_size * upload_concurrency` per transferring
file (and `--transfers` files may upload at once); each buffer is sized for
the largest part, so it may reach twice `chunk_size`. A part that exhausts its
own re-posts fails the file, and the error is marked so the copy engine does not
re-upload the whole file in this pass - `--retries` still decides whether the
command runs the list again, and a re-run starts a fresh session with a new id.
That marking is what makes the layering readable: one part's POST count is set by
`--low-level-retries`, and the number of times a file is uploaded is set by
`--retries`. Before this, a single broken part cost the entire upload: cutting one
part's request mid-body on a 96 MiB file made rclone start over from part 1 in a
new session, which is 96 MiB of extra uplink for 8 MiB of lost bytes.
A 4xx rejection, and the single-request path below `--wopan-upload_cutoff`, are
treated as deterministic and are never re-posted.

Chunked uploads leave the file in the server's shard storage with a
composite ETag, which is never a content MD5 - the caveats in
[Modified time and hashes](#modified-time-and-hashes) apply: `--checksum`
treats these files as having no hash and compares by size only.

### Tuning

- Lower `--wopan-upload_cutoff` if you want MD5 available at upload time for
  more files (single-request uploads carry it immediately).
- Raise `--wopan-upload_concurrency` for large files if the network path
  allows more than one stream to help - behind a reverse proxy or on a
  high-latency link this can improve throughput; on a saturated access link
  it changes little.
- Raise `--wopan-chunk_size` to reduce request count for very large files,
  at the cost of more memory per part.

All three options work both as command line flags (`--wopan-chunk_size 16Mi`)
and as config file keys (`chunk_size = 16Mi` under the remote's section),
like every other rclone backend option. Invalid values (`chunk_size` below
`5Mi`, `upload_concurrency` below `1`) fail fast when the remote is created.

### Multi-thread copy

The backend implements rclone's `OpenChunkWriter` interface, so when copying
**from** a ranged-download source (S3, HTTP, another local, ...) to wopan,
files at or above `--multi-thread-cutoff` (default `256Mi`) use rclone's
multi-thread copy engine instead of a single download stream: the source is
downloaded in parallel ranged streams and each chunk is handed directly to a
wopan upload part of one upload session, so the file is never buffered on
local disk.

The wopan side controls the part layout: chunk boundaries follow
`--wopan-chunk_size` (each engine chunk is one part, `partIndex` counting up
to `totalPart`) and the server reassembles by part index, so shuffled
arrival order is fine. The stream count comes from `--multi-thread-streams`
(default `4`), raised to `--wopan-upload_concurrency` when that is higher.
`--multi-thread-chunk_size` does not apply here because the destination
supplies the chunk size.

A re-post of a part rewinds that chunk's reader to the start of the chunk, which
both the buffered reader and a local source's ranged reader position correctly.
A reader that refuses to seek is not re-posted; its failure is reported as is.

If the source server ignores a ranged request (rclone detects this and
fails the chunk with `fs.ErrorRangeIgnored`), rclone logs
`multi-thread copy: ...: downloading in a single stream` and re-runs the
file through the normal single-stream path, so a misbehaving source or CDN
degrades to the slower transfer instead of corrupting data.

## Membership and the single-file upload size

wopan reports how large a single uploaded file may be for the account. rclone asks
for that number once per run through the desktop client's own benefit endpoint
(`displayMembershipBenefit`) and reads `uploadFileSize` from the answer - `1024`,
in MiB, i.e. 1 GiB for a non-member account at the time of writing. The value
belongs to the account rather than to a space, so the family space shares it, and
a second remote of the same account does not ask again.

A successful probe logs one line per account, and `-vv` adds the whole benefit
block as it arrived:

```console
NOTICE: wopan top: wopan: membership "非会员" (vipLevel=0): reported single-file upload size 1Gi - files over it are warned about, not skipped
```

The probe asks with the desktop client's own header set, measured from the traffic.
One part of that cannot be honoured by default: rclone's transport rewrites
`User-Agent` on every request, so what actually goes out is `rclone/<version>`.
That does not change the answer - the captured user agent, `rclone/<version>` and
no user agent at all returned byte-identical bodies when measured - but to send the
client's string anyway set `--wopan-user_agent` to it.

**This is a number to read, not a limit rclone enforces.** Measured 2026-10-20 on
that same non-member account, files of 1 GiB + 1 byte and of 1.2 GiB were uploaded
through rclone's chunked path and read back at full size. Anything past
`upload_cutoff` (64 MiB by default) goes through that path, so blocking on the
reported number would skip files the server accepts - a silently smaller sync.
rclone therefore names each such file in the log and uploads it anyway:

```console
NOTICE: wopan top: wopan: big.bin is 1288490188 bytes, over the single-file upload size the account reports (1073741824 bytes, membership "非会员"); uploading anyway - the chunked upload path is not held to that number
```

The notice appears at each of the three upload entry points (`Put`, `Update`,
`OpenChunkWriter`) before any request is spent, so a file the official app would
refuse is still visible here rather than surfacing later as a server rejection. A
server-side `Copy` moves no bytes and says nothing.

Two things to know about that number. `-vv` prints the benefit block as the service
returned it, and that block carries account identifiers - do not paste it into a
public issue. And the block reports the *service's* view: the official client raises
the figure to 51200 MiB locally for some tiers, so a member account's display value
need not be what the endpoint said.

The probe itself can never block a transfer. If the endpoint is unreachable,
answers a shape rclone cannot read, or reports no usable `uploadFileSize`, rclone
logs the reason at `-vv` and says nothing about sizes. An expired or missing token
is one of those shapes: the endpoint answers `HTTP 200` with
`meta.code 10002404 用户未登录` rather than an authentication error, and rclone does
not rotate the token pair to learn a display value. `--wopan-no_member_check` skips
that request, and the notice with it.

## Quick transfer

`--wopan-quick_transfer` asks the drive whether it already stores content with the
same SHA-256 before uploading a file, and if it does, the file appears in the
destination without a single byte being transferred. This is the official client's
"秒传": rclone sends the hash to the openapi service the client itself uploads to
(`POST https://b.smartont.net/openapi/transfer/quickTransfer`, one `access-token`
header, the same token the dispatcher uses) and the server registers a new entry
pointing at content it already holds.

Measured 2026-10-20 on the personal space of a test account, both directions:

```console
DEBUG : older-than-time.bin: wopan: rapid upload miss (sha256 00e2a676fd4f36c7a83d73baf617e6cd2fb2e5968e4cb91f221a7eacd8509b47 not stored), uploading normally
NOTICE: qt-conv-Y.bin: wopan: rapid upload hit - 3Mi already stored under this SHA-256, no bytes uploaded; the service stamps the entry with the current time and wopan cannot set a modification time afterwards, so a later copy or sync re-registers it unless --size-only is used
```

The hit is a real file: `Transferred: 0 B`, the listing shows the full size, the
MD5 read back equals the normally-uploaded copy's, and downloading it gives bytes
whose SHA-256 matches the source. A miss costs one request and transfers normally.

**The one thing to know before turning it on is the modification time.** The entry
is stamped with the moment it was registered; the timestamp sent in the request is
ignored by the service, and wopan has no way to set a modification time afterwards
at all (`SetModTime` is not supported). rclone's default comparison is size plus
modtime, so a second `copy` or `sync` of the same content sees a mismatch and
registers the file again - no bytes move, and the run reports
`Copied (replaced existing)`. Use `--size-only`: in five measured runs it skipped
the second copy every time, and it asks no hash of either side. `--checksum` works
differently by size. Files above `--wopan-upload_cutoff` have no stored content MD5
at all, so rclone gives up on the hash comparison and falls back to comparing sizes
(`Dst hash empty - aborting Src hash check`) - it converges, but not because of the
hash. Smaller files do have one, and reading it costs a `GetDownloadUrlV2` plus a HEAD
per file; in six measured runs of such a file that pair failed twice with
`wsarecv: An existing connection was forcibly closed`, and when the destination hash
cannot be read rclone transfers rather than skips. Whether the replacement
changes the stored object id and where the old entry goes is not measured. The
object rclone returns from the transfer itself reports the source's timestamp, while
the drive reports the registration time, so any later listing of that file
disagrees with it.

The other cost is the hash. rclone needs SHA-256 of the source to ask the question,
which for a local file means a second full read and for a remote source can mean
downloading it - so the option is off by default, and the probe is skipped for files
under 3 MiB, where a request is not worth it. (That floor is what the official
client is reported to use; it is not measured here.)

Nothing about this feature can lose data. A miss, a timeout, an HTTP error, an
unreadable response, an unexpected `meta.code`, a source whose SHA-256 rclone cannot
obtain, a source with no modification time and every family-space transfer all fall
through to the ordinary upload. If the first answer never arrives at all - the
connection drops, a `500`, a body rclone cannot parse - rclone asks once more
before giving up, because that is the one case in which the service may already have
registered the content, and re-asking is cheap: three offers of the same
`(directoryId, fileName, SHA-256)` measured on 2026-10-20 leave exactly one entry,
with no renamed copy and no second one. A clean miss is never re-asked; a miss is
the normal answer for new content. The one case that stops the transfer is a hit
that comes back without a usable object id: the service has registered content
rclone cannot name, and uploading would store a second copy of it.

What a hit costs on the source side depends on where the source is. A local file is
not re-read: the writer rclone hands the multi-thread engine for a hit reads
nothing, and for a local source the engine streams rather than buffers. For a remote
source the engine still walks the file once after the hit, in `--wopan-chunk_size`
pieces - buffered, because buffering is also what protects a source whose `Open`
ignores a byte range. So the transfer is cheap on the network to wopan, not
necessarily cheap on the source.

Family spaces are not offered at all. The listing side of that space is known - the
client asks `QueryAllFiles` with `spaceType "1"` and a `familyId`, exactly as rclone
does, and its entries have no `sha256` field at all (the schema also carries
`familyId`, `campusId` and `loadInfo` instead). What `quickTransfer` wants for a
family directory has never been captured: a recording of an upload there only
contains panservice traffic, because the offer goes to the openapi host. TODO:
capture one with `*.pan.wo.cn` and `*.smartont.net` unfiltered, during an upload to a
family folder, then teach the probe the family shape.

The offer carries the access token in an `access-token` header, to
`b.smartont.net` - the same openapi operator the official client uploads bytes to
(`du.smartont.net` answers on the same address), not a third-party service. With
`--wopan-quick_transfer` on, `--dump headers` prints it: treat those logs as
credential material.

## Public links

`rclone link` returns a direct download URL for a single file:

```console
rclone link wopan:path/to/file.bin
```

The URL is the tokenised direct-download link returned by `GetDownloadUrlV2`,
so anyone holding it can fetch the file until it expires. wopan fixes that
lifetime on the server at 120 minutes (measured 2026-09-30, and not extended by
access), and the API takes no expiry parameter, so `--expire` has no effect -
rclone logs a warning and returns the server-issued link as-is - and `--unlink`
is meaningless because there is no stored share to revoke. Directories cannot be
linked: `rclone link` returns `fs.ErrorCantShareDirectories`.

For longer-lived or revocable sharing, use the wopan app or web client.

## Rejected directory names

The service refuses some directory names: the sensitive-word filter answers
`RSP_CODE 4444`, an illegal character answers `1009`. rclone reports both as a
non-retryable error for the file, so no flag makes them pass.

The refusal is per directory, but rclone resolves a tree one file at a time and
the directory cache stores only successes. That used to scale badly: in a
measured run on 2026-10-21 one refused directory with 123 files beneath it
produced 733 `failed to make directory` errors - a parent listing plus a
`CreateDirectory` per file, times two transfer phases, times three `--retries`
rounds.

The backend now remembers refusals, keyed by parent directory id and name, so a
refused directory costs one probe instead of one pair of requests per file.
Every refusal - `1009`, `4444` and any other business code alike - is trusted
for 60 seconds and re-probed at most 3 times, after which the memory goes quiet
for 30 minutes.

Nothing is held for the life of the process, not even the explicit name
rejections. The memory is consulted on the read side too: listing a directory,
`rclone rmdir`, `rclone purge` and the destination check inside a server-side
directory move all ask the same question, and a held entry sends no request. A
verdict that was ever wrong would therefore hide a directory that really exists
without ever finding out, and `rclone rcd` and `rclone mount` run for days.
Measurement also says permanence bought nothing: the savings come from the cap,
and one reopen costs 2 requests per bad directory per half hour. And the codes
are not as tidy as it looks - one refused directory answered `4444` for some of
its files and `9999` for others, and which one a file got did not depend on the
file, the phase or the round.

- A network failure is never remembered, because it says nothing about the name.
  Neither is a failure of the lookup that follows a "name taken" answer: that
  one is a listing, so it does not spend the probe budget.
- `rclone purge`, `rclone rmdir` and a directory move drop the whole memory,
  because the tree they describe has changed.

The error lines do not decrease - rclone reports the failure per file whether
or not a request went out; the requests are what fall. A name you free in the
app during a long run is picked up again within about half an hour.

## Restrictions

- **Empty files are not supported.** The service rejects 0-byte files, so
  rclone returns `fs.ErrorCantUploadEmptyFiles` when asked to upload one.
  This affects `rclone touch` on a non-existent file too.
- **The account reports a single-file upload size; rclone does not enforce it.**
  rclone logs it once per run and names any file above it, but the chunked upload
  path is not held to that number (measured: 1.2 GiB uploaded on an account
  reporting 1 GiB) - see [Membership and the single-file upload
  size](#membership-and-the-single-file-upload-size).
- **File names are limited to 100 characters (runes).** Longer names return
  `fs.ErrorFileNameTooLong` rather than being silently truncated. Note that
  the app and web clients themselves truncate long names.
- **Emoji and other non-BMP characters are not supported.** The service
  rejects them outright; rclone reports the offending character instead of
  retrying. A handful of storable-looking special symbols and 4-byte names
  pass that check but are then rejected by the server with a bare HTTP 500;
  rclone surfaces this as a non-retryable upload error for the file instead
  of burning the retry ladder on it.
- **Names are case-insensitive.** Two names differing only in case refer to
  the same file.
- **Modification times cannot be changed after upload.** `rclone touch` on an
  existing file fails with a "failed to touch" error; sync's mtime-only
  updates (e.g. `--update`) and `--refresh-times` are handled gracefully with
  an informational message instead.
- **`rclone cleanup` is not supported**, and `--fast-list` has no effect
  either: the backend always lists directory by directory. (`rclone link` *is*
  supported - see [Public links](#public-links).)
- **Server side copy and move work only within the same remote.** Copying
  between a personal space remote and a family space remote transfers the
  data through rclone, even if it is the same account.
- **Naming a freshly uploaded file in `moveto`/`move` can fail oddly.** The
  listing index is eventually consistent (see below): while the new file is
  not yet listing-visible, `moveto` may report `directory not found` or fall
  back to a directory move, which rclone then refuses as a directory moved
  into itself. Wait a few seconds and repeat the command - the retry applies
  cleanly.
- **Renaming onto an existing name fails with `file name already in use`**;
  the server never overwrites on rename. A rename that
  differs only in case (`Case.txt` → `case.txt`) is accepted and then
  silently does nothing. This also governs concurrent updates of the same
  file: the loser's restore cannot win back a name the winner has already
  taken, so it reports an error naming the `.rclone-old-` backup it left
  behind (see "Transfers and temporary files") and the next run repairs it.
- **Names are stored verbatim, including characters other services reject.**
  A fullwidth question mark (`？`, U+FF1F) and an ASCII `?` are two different
  names and can coexist in one directory, and trailing spaces, dots, tabs
  and `%` characters survive a round trip untouched.
- **Server side copy onto an existing name auto-renames.** The copy API
  cannot choose the destination name: the copy lands under the source's name,
  and if that name is taken in the target directory the server renames the
  copy to `name(1).ext` instead of failing or overwriting. A same-directory
  copy always collides with the source itself, so rclone does not even attempt
  the server side operation there and transfers the file instead. A
  cross-directory copy onto an existing name leaves both the original and the
  `name(1).ext` copy behind, and the target directory can refuse listings and
  downloads with a "system exception" error for a while afterwards; the next
  sync run picks up the orphan.
- **Newly uploaded or copied files may take a while to appear in listings.**
  The service's directory index is eventually consistent: a file that has
  just finished uploading (or a server side copy that has just completed) can
  take anywhere from a few seconds to over a minute to show up when listing
  the parent directory. rclone retries these listings, but a sync run
  immediately after a large upload may need a second pass. Deletions have the
  same lag, so a purge immediately followed by a second purge can succeed
  twice (the server idempotently accepts deleting a directory id it has not
  yet made visible as gone) instead of reporting the directory as missing.

### Do not delete directories with another client while rclone runs

If a directory is deleted with the Wopan app, web client or another rclone
remote while an rclone command is running, listing that directory id returns
an **empty list** rather than an error. In a pull direction sync
(`rclone sync remote: local:`) that empty listing looks like "directory is
now empty" and **deletes the corresponding local files**. Always delete
directories through the same rclone remote, or make sure no rclone command
is using the directory while another client deletes it.

## Transfers and temporary files

Updating an existing file is lossless: the new content is uploaded under a
temporary name (`name.rclone-tmp-XXXXXXXX`), the old file is then renamed to
`name.rclone-old-XXXXXXXX`, and only once that has succeeded is the new file
renamed onto the real name. The old file is therefore never deleted before its
replacement is complete, and a failed or interrupted upload leaves the file
readable under its own name.

If the final rename fails, rclone checks which file actually holds the real name:
if the new content got there after all (the rename may have taken effect even
though its response was lost) the update counts as successful and the backup is
dropped; otherwise the old file is renamed back. If the old file cannot be
restored either, the error names the `.rclone-old-` backup that still holds the
previous content.

If rclone is killed mid-update, a temporary or backup file may remain on the
server. Neither overwrites anything, but both consume quota.

A `.rclone-old-` file must not be deleted blindly: if the process was killed
between the two renames, the old content lives only under the backup name, and
deleting it loses the file. Check that the real name exists first, and recover a
missing file by renaming the backup back to the real name.

Leftover `.rclone-tmp-` files are safe to remove once the real name is present,
since the new content is re-uploaded on the next attempt:

```console
rclone delete remote:path --include "*rclone-tmp-*" --rmdirs
```

(The `--rmdirs` is optional; it removes directories that became empty.)

## Sharing an account with OpenList

The token refresh endpoint rotates the `refresh_token`: each refresh token
can only be used once. If rclone and another program such as OpenList both
refresh tokens for the same account, whichever refreshes second invalidates
the first one's credentials.

To share an account safely:

1. Let one side own the token refresh, and use the `no_refresh` option on
   the other. With `no_refresh` set, rclone never calls the refresh endpoint
   and needs a valid `access_token` in the config instead of a
   `refresh_token`.
2. Multiple rclone remotes of the same account (in the same rclone process)
   share one token state and refresh in lockstep, so you can mix personal
   and family remotes freely; only *third party* programs need the
   coordination above.

## Security notes

The upload endpoint takes the `access_token` as a URL query parameter (a
protocol constraint of the wopan upload channel), so `--dump requests`
writes it into the log. Avoid `--dump requests` on shared terminals, and
treat captured logs as credential material.

`--dump headers` is not clean either, measured 2026-10-20: rclone redacts
`Authorization` and `X-Auth-Token`, and wopan sends its token under names
neither of those - the membership probe repeats it in `accesstoken`,
`Access-Token` and `X-YP-Access-Token`, so one `rclone lsf --dump headers`
against a wopan remote printed the token three times in plaintext. Instant
upload sends the same value in `access-token`, which the redaction does not
cover either.
