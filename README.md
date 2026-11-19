# Rust Object Store for Linux
An [`object_store`] implementation specifically for modern Linux.

This is an alternative to the builtin [`LocalFileSystem`], which is suffering from trying to fit multiple operating systems into a single, high-level implementation:

- [PR#630 -- add xattr-based attribute support](https://github.com/apache/arrow-rs-object-store/pull/630)
- [PR#643 -- add fsync for durability](https://github.com/apache/arrow-rs-object-store/pull/643)
- [Issue#661 -- reliable file writes: explicit close error checking and opt-in sync](https://github.com/apache/arrow-rs-object-store/issues/661)
- [PR#676 -- explicit close with error checking](https://github.com/apache/arrow-rs-object-store/pull/676)
- [PR#758 -- fsync create-mode rename source delete](https://github.com/apache/arrow-rs-object-store/pull/758)
- [Issue#767 -- implement fsync for delete operations](https://github.com/apache/arrow-rs-object-store/issues/767)
- [PR#866 -- honor fsync for local deletes](https://github.com/apache/arrow-rs-object-store/pull/866)
- [Issue#888 -- can return before a new directory's entry is durable](https://github.com/apache/arrow-rs-object-store/issues/888)
- [Issue#889 -- a failed multipart complete leaks the staging file, and a staging file can be removed after being renamed](https://github.com/apache/arrow-rs-object-store/issues/889)
- [PR#890 -- make concurrent directory creation durable with fsync](https://github.com/apache/arrow-rs-object-store/pull/890)
- [PR#891 -- remove the staging file when publishing it fails](https://github.com/apache/arrow-rs-object-store/pull/891)
- [PR#892 -- fsync directories a failed durable creation left unsynced](https://github.com/apache/arrow-rs-object-store/pull/892)

## Constraints
This should work similarly to the existing local object store:

- paths map to files in the file system
- `/` characters split directories
- unicode characters are percent-encoded
- files store natively, i.e no special file format or attributes are required
- multiple readers & writers are allowed

## Priorities
Given the above constraints, we usually implement things in this priority order:

1. **correctness:** The implementation MUST be correct, even when a system crashes.
2. **functionality:** We SHALL implement all features that the most object store implementations support.
3. **performance:** If possible we SHOULD make it as fast and memory+disk-efficient as possible.

## Limitations & Workarounds
Given how limited even the Linux file system API is, we can only do so much. This lists current limitations and workarounds.

### Atomic File Creation
Modern Linux offers [`O_TMPFILE`] which allows you to create an "unnamed" file in a directory (i.e. it's stored there but it doesn't have a name yet). You can then write the file content and attributes, [`fsync`] it, and finally publish it with [`linkat`]. The issue is that [`linkat`] cannot replace existing files.

The workaround is to use a unique, temporary filename and use [`renameat2`] to move the file to its final destination. This however exposes the temporary staging files to readers and we need to filter them out in all APIs.

Also see:

- [UAPI Group Kernel Feature Ideas -- Linking of `O_TMPFILE` files with replacement](https://uapi-group.org/kernel-features/#linking-of-o_tmpfile-files-with-replacement)

### Case-folding / Case-insensitive File Systems
Just don't. We will not support that madness.

Sadly there isn't a good way to detect "is this file system or directory subject to case-folding" from userspace. So you've been warned!

### Compare-And-Swap
Object store offers a PUT method that has atomic compare-and-swap semantic. There is no way to replace-override with a file descriptor. Hence, we need to use directory-based locking to ensure the file doesn't change while we're at it.

Locking is implemented using [`flock`], which is an "advisory" lock, so other writers need to play along.

Also see:

- [UAPI Group Kernel Feature Ideas -- Unlinking via two file descriptors](https://uapi-group.org/kernel-features/#unlinking-via-two-file-descriptors) (similar but not the same, might be required though if we ever need conditional DELETE)
- [Kernel Bug 93441 -- Add `AT_EMPTY_PATH` ability to `unlinkat()` and `renameat()` for direct use of fds](https://bugzilla.kernel.org/show_bug.cgi?id=93441)
- [UAPI Group Kernel Feature Ideas -- Issue 58 -- Race-free file-based compare-and-swap](https://github.com/uapi-group/kernel-features/issues/58)

### Directories
Directories conflict in keyspace with files. So a file `a/b` and a file `a` cannot exist at the same time. To simplify the implementation and improve performance, we NEVER delete directories.

Under circumstances directories might be created even if the resulting operation fails (e.g. during COPY or RENAME). This is required for a deterministic, deadlock-free [`flock`] ordering.

### Durable Reads
For readers there is no way to know if a file is durable or not. A writer can do this:

1. open staging file
2. prepare file content and attributes
3. [`fsync`] file
4. link file to directory
5. [`fsync`] directory

However, the writer may crash between step 4 and 5. For a reader to really only read durable data (i.e. that would be there even if the machine just crashes hard now and reboots) we would need to do this:

1. open file
2. get inode number using [`fstat`]
3. [`fsync`] dir
4. open file again
5. get inode number again using [`fstat`]
6. compare inode numbers

That's A LOT of syscalls for the reader and a lot of potential sync-contention on the directory. So we don't do that (and violate our first priority, yikes).

### ETag
Conditional updates in [`object_store`] use ETags (or versions) to determine identity. Considering our constraints, we should be able to determine an ETag for every ordinary file. On Linux that leaves us with these attributes:

- **inode number:** This is often used but note that inode numbers might recycle.
- **modification time:** This works as long as your system clock is working properly, you don't have too much system load, and timestamps are not "normalized" (e.g. by [Nix](https://releases.nixos.org/nix/nix-2.35.1/manual/store/file-system-object/os-file-system.html#metadata-normalization)).
- **size:** This often collides, but offers additional entropy.

None of these are perfect, but combining them is deemed "good enough".

Also see:

- [Caddy Issue 5548 -- Consider dropping ETag, Last-Modified-Time generation for files where epoch <= mtime <= epoch+1](https://github.com/caddyserver/caddy/issues/5548)

### Opening Regular Files
_EDIT: Seems like this is possible since Linux 7.2 with [`OPENAT2_REGULAR`]._

It would be nice if we can ensure that [`openat2`] can only open files (and not directories or other weird things like FIFOs). A flag like `O_REGULAR` which would be similar to [`O_DIRECTORY`]. In the meantime we use [`fstat`] for every file read (incl. GET, COPY, RENAME) to make sure a file is actually a file.

Also see:

- [UAPI Group Kernel Feature Ideas -- Ability to only open regular files](https://uapi-group.org/kernel-features/#ability-to-only-open-regular-files)

### Page Cache
Reading & writing files under Linux usually goes through the page cache, which is also where your application memory lives. This is OK if you expect that your application will reuse the same file content again, but its just garbage if you don't -- which is true for many database-like workloads that will likely manage their own in-memory cache.

The traditional way of dealing with this is direct-IO. Having written implementations that use that path before, I can attest that it is a massive headache to work with that interface and I really don't want to pollute code with that mess. It would also require more syscalls, e.g. to figure out all the direct-IO alignment and size constraints (see [`statx`]).

The modern alternative is [`RWF_DONTCACHE`]. That's not supported on `tmpfs` though, which makes testing harder. So we'll likely need some kind of feature detection via "try once and disable if it doesn't work".

## License
Licensed under either of these:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

### Contributing
Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion in the work, as defined
in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.


[`flock`]: https://man.archlinux.org/man/flock.2
[`fstat`]: https://man.archlinux.org/man/core/man-pages/stat.2.en
[`fsync`]: https://man.archlinux.org/man/fsync.2
[`linkat`]: https://man.archlinux.org/man/linkat.2
[`LocalFileSystem`]: https://docs.rs/object_store/latest/object_store/local/struct.LocalFileSystem.html
[`O_DIRECTORY`]: https://man.archlinux.org/man/openat.2.en#O_DIRECTORY
[`O_TMPFILE`]: https://man.archlinux.org/man/openat.2#O_TMPFILE
[`OPENAT2_REGULAR`]: https://man.archlinux.org/man/openat2.2#OPENAT2_REGULAR
[`object_store`]: https://github.com/apache/arrow-rs-object-store/
[`openat2`]: https://man.archlinux.org/man/openat2.2
[`renameat2`]: https://man.archlinux.org/man/renameat2.2
[`RWF_DONTCACHE`]: https://man.archlinux.org/man/preadv2.2#RWF_DONTCACHE
[`statx`]: https://man.archlinux.org/man/statx.2
