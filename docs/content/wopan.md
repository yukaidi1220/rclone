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
  is only released immediately with this flag.
- `--wopan-encoding`: the encoding for the backend. The default is `Standard`
  plus `EncodeInvalidUtf8` and should normally be left alone. The server
  stores file names verbatim (including trailing spaces, dots and tabs), so
  no additional escaping flags are needed.

## Modified time and hashes

Wopan stores the modification time of files with a precision of 1 second.
The modification time is set at upload time and preserved by server side
copies and moves.

Wopan reports an MD5 hash, taken from the ETag of the object. Fetching the
hash requires a HEAD request per file, so rclone marks it as slow: it is only
used when you ask for it explicitly with `--checksum`. **Using `--checksum`
is the recommended way to run sync and copy against wopan**; without it,
rclone compares sizes and modification times.

Files uploaded by other clients at 16 MiB or larger are stored as multipart
objects. Their ETag is a multipart digest, not a content MD5, and rclone
cannot tell the difference between a real MD5 and a multipart digest for
files it did not upload itself. For those files `--checksum` **silently
degrades to size-only comparison** - this is by design so that verification
does not delete valid objects.

## Restrictions

- **Empty files are not supported.** The service rejects 0-byte files, so
  rclone returns `fs.ErrorCantUploadEmptyFiles` when asked to upload one.
  This affects `rclone touch` on a non-existent file too.
- **File names are limited to 100 characters (runes).** Longer names return
  `fs.ErrorFileNameTooLong` rather than being silently truncated. Note that
  the app and web clients themselves truncate long names.
- **Emoji and other non-BMP characters are not supported.** The service
  rejects them outright; rclone reports the offending character instead of
  retrying.
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
- **Renaming onto an existing name fails with `file name already in use`**;
  the server never overwrites on rename. When two concurrent updates race,
  the loser reports the error and the next run repairs it. A rename that
  differs only in case (`Case.txt` → `case.txt`) is accepted and then
  silently does nothing.
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

Updating an existing file uploads the new content under a temporary name
(`name.rclone-tmp-XXXXXXXX`) and then renames it over the old one. If rclone
is interrupted between the two steps, the temporary file may remain on the
server. It never overwrites anything, but it does consume quota. Leftover
temporary files can be removed with:

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
