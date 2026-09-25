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

Wopan reports an MD5 hash, taken from the ETag of the object. Fetching the
hash requires a HEAD request per file, so rclone marks it as slow: it is only
used when you ask for it explicitly with `--checksum`. **Using `--checksum`
is the recommended way to run sync and copy against wopan**; without it,
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
   by `--wopan-upload_concurrency` workers. The source is read sequentially
   and parts are posted concurrently, so arrival order is shuffled; the
   server reassembles the file by part index, and out-of-order arrival has
   been verified against the live service.
3. When the final part is accepted the server assembles the file and returns
   the finished file ID.

Memory usage is about `chunk_size * upload_concurrency` per transferring
file (and `--transfers` files may upload at once); each buffer is sized for
the largest part, so it may reach twice `chunk_size`. A failure on any part
aborts the whole upload and rclone surfaces the error for that file.
Abandoned parts remain on the server side - invisible and not counted
against quota - and the retry starts a new upload session with a fresh id.
A transient server 5xx keeps the file retryable, so `--retries` re-runs the
whole upload; a 4xx rejection is treated as deterministic and not retried.

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

If the source server ignores a ranged request (rclone detects this and
fails the chunk with `fs.ErrorRangeIgnored`), rclone logs
`multi-thread copy: ...: downloading in a single stream` and re-runs the
file through the normal single-stream path, so a misbehaving source or CDN
degrades to the slower transfer instead of corrupting data.

## Restrictions

- **Empty files are not supported.** The service rejects 0-byte files, so
  rclone returns `fs.ErrorCantUploadEmptyFiles` when asked to upload one.
  This affects `rclone touch` on a non-existent file too.
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
- **Public links are not supported** (`rclone link` returns an error), nor is
  `rclone cleanup` on the backend. `--fast-list` has no effect either: the
  backend always lists directory by directory.
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
