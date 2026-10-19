# Release Notes

Release notes for the open source Lore project. Releases before v0.8.4 predate this file; see the
[GitHub releases](https://github.com/EpicGames/lore/releases) page for the published record.

## Nightly

### Breaking changes

- `[environment.endpoint] user_url` now names a user directory serving `lore.user.v1.UserService`. Clients resolve
  names there with `UserGet` and `UserFind`, and no longer send it the `UrcAuthApi` `GetUserInfo` and `GetUserId`
  calls. A server that advertises no `user_url` is unchanged: clients ask `auth_url` over `UrcAuthApi` as before
- A client and the Lore service it relays commands to must come from the same build. Commands, their events and their
  status cross as bitcode under message protocol version 2, where they crossed as JSON, and bitcode carries no field
  names to tell two builds apart. A client and a service on either side of this change refuse each other's messages, so
  restart the service after upgrading
- Reads trust the local store and check what they fetch by the trust of its source
  ([ADR-00023](developing/decisions/00023-data-verification-and-trust.md)): no local read hashes its payload, and every
  payload fetched from a remote is checked against its address on receipt unless the command trusts the remote or a C
  API reader takes it as stored (`fragments`).
  `lore-storage`: `ReadOptions` loses `verify` and `trust_local` with `with_verify`, `no_verify` and `trusting_local`,
  and gains `trust_remote` with `trusting_remote`; `decompress_and_verify` is replaced by `expand_payload`. `read_raw`
  loses its `verified_by_caller` argument, `fetch_remote` takes `ReadOptions` for its `priority`, `trust_remote` and
  `decompress`, and `check_fetched` is removed and `fetch_payload` made private. `Loaded::content` is optional,
  `Flown::take` returns a `Result`, `Flown::Unverified` and `Flown::Rejected` are removed, and a `fetch_single_flight`
  fetch answers `Loaded`. `file_matches` takes the reader's `trust_remote`. `lore-revision`: `cache_counted` loses its
  `verify` argument and `RepositoryContext` its `trust_local` setting
- C API: `skip_verify` on `lore_storage_open_args_t` is now an alias of `trust_remote` for its handle, so a handle that
  sets it takes payloads fetched from its remote unhashed. Local reads no longer hash either way
- `lore-storage`: `ReadOptions` loses the unused `with_local`, `no_cache`, `with_direct_write` and
  `no_max_content_size`, and `with_remote`, which restated the default; `load_raw_local` is removed. `read_into` reports
  a slice shorter than its content as `Oversized`, as `read_into_buffer` does
- `lore-storage`: `StoreGetData` gains `content`, the content a store expanded where it checked a payload on receipt, so
  a struct literal names it. `read::Fetched` is `read::Loaded`, which `read_raw` also returns, and `read_raw` forwards a
  store's error with its variant rather than as internal
- C API: `lore_branch_merge_start_args_t` appends `link_paths` and `link_messages`, taking the struct from 80 to 112
  bytes. Rebuild against the new `lore.h`; a zero-initialized struct keeps today's behavior
- The server refuses to start when `[server.auth] resource_id_template`, or `resource_template` or `scope_template` in
  `[server.auth.oidc]`, has no `{id}`. Such a template names the same resource for every partition, so one grant
  matched every partition. The error names the setting
- `lore-storage`: `local::fan_out::commit_if_initial_level` returns the `std::io::Result` of its level marker write
- `lore-storage`: `local::fan_out::commit_if_initial_level` takes the group's `pending_level`,
  `local::fan_out::remove_stale_new_files` is `remove_stale_files`, and
  `local::immutable_store::SerializeFailureGuard::new` takes only the bucket's dirty mark. `lore-io`:
  `IoDriver::write_file_segments_atomic` takes its paths as `impl Into<PathBuf>`, so that a caller hands over the paths
  it builds rather than having them copied, and removes its temporary file when the write, sync or rename fails
- `lore-storage`: a local immutable store's `put` refuses a fragment whose flags or sizes contradict each other or its
  payload, such as store-managed obliteration flags, two compressors, or a fragment list that is not a whole number of
  references, and stores a flag bit it does not know. A put without its payload to an obliterated association is refused
  as `PayloadNotFound`; one with its payload replaces the obliteration
- `lore-storage`: `ImmutableStoreSettings` and `MutableStoreSettings` gain `reuse_state_across_holds`, so a struct
  literal names it, and `StoreLock::new` takes it. A store with it unset, the default, reloads at the first use after
  its lock was released. `MutableStore` gains `try_join_claim`, as `ImmutableStore` has. `LocalImmutableStore` and
  `LocalMutableStore` gain `new_claimed`, and their modules `create_claimed`, which return the store with the claim it
  was opened under. `lore-revision`: `create_client_immutable_store`, `create_immutable_store_at_path`,
  `create_client_mutable_store` and `create_mutable_store_at_path` return the store with that claim, `None` for a store
  from the cache, for the caller to keep until its own claim on the store has joined it; `claim_opened_stores` opens the
  mutable store beside an opened immutable store and claims both that way. `SharedStoreRegistryEntry::create_stores` is
  removed, and `create_null_repository_context` claims the stores for the context's lifetime

### Features

- `--trust-remote` (C API: `trust_remote` on `lore_global_args_t`) declares the remote a trusted source, for a machine
  within the remote's own infrastructure where performance comes first, such as a build server streaming from a server
  in its own backend. A payload fetched from a trusted remote has only its sizes checked against its length, is never
  hashed, and is decompressed only where its content is read. The field sits in the struct's tail padding, so its size
  and its other fields' offsets are unchanged. A caller must zero the whole struct, padding included: any non-zero byte
  there trusts the remote, so a caller built against the earlier header that sets the fields one by one may skip the
  check
- `lore-storage`: `read::check_payload_sizes`, the size check every payload takes, `read::check_receipt`, the check on
  receipt by the remote's trust, `read::heal_on_receipt`, the heal after a check that fails, `read::Loaded::take` and
  `defragment::MAX_FRAGMENT_TREE_DEPTH` are public
- Server: added opt-in HTTP health-only mode for deployments that do not serve repository HTTP routes.

- The user identity is read from the token, by the same rule on the client and the server: the `identity` claim, or
  its namespaced alias `https://lore.org/claims/identity`, where the issuer mints one, and `sub` otherwise. An issuer
  whose `sub` is not a good identity maps the value it wants into `identity`, such as Entra's `oid` or a username. A
  token whose deciding claim is empty or not a string is refused. The `[server.auth] identity_claim` setting and the
  environment response's advertisement of it are gone, since the setting could not reach a token the client reads
  without the server, such as one passed with `--identity-token` or used under `--offline`

- `lore auth login` against a server that advertises an OIDC issuer logs in through the OAuth 2.0 device authorization
  grant (RFC 8628). The login asks the provider for the server's default scopes and `offline_access`, opens the
  verification URL in the browser, or prints it with `--no-browser`, shows the code the provider's page may ask the user
  to confirm, polls the token endpoint at the cadence the provider sets, and stores the access token and the refresh
  token it issues. An ID token the provider returns names the user for display and is neither stored nor sent. After
  the login, partition operations send the access token itself. When the server advertises `resource_template` or
  `scope_template`, they send a token the provider issues for that partition through the RFC 8693 token exchange,
  which names the partition in that one parameter. A token exchange issuer on another host than the login issuer gets
  the login token only if the token names that host: in its `root_domains` claim, or, without that claim, in its
  `aud`. Repository metadata set and get and repository delete send the partition's token as well. The refresh and
  client credentials grants are not implemented yet
- `lore auth login --token-type <TYPE> --token <TOKEN>` against a server that advertises an OIDC issuer logs in
  through the RFC 8693 token exchange at the issuer's token endpoint, with the token as `subject_token` and the type
  passed verbatim as `subject_token_type`. The issuer decides which types it accepts and how they are spelled.
- `lore branch merge` and `lore branch merge start` accept `--link-message <PATH> <MESSAGE>`, as `lore commit` does,
  giving the linked repository at `PATH` its own message on the revision a merge auto-commits. A path that is not a
  link fails the merge before it starts
- `lore-io`: `IoFile::from_std` wraps a `std::fs::File` the caller already holds, so an `IoDriver` runs positional
  reads, writes and syncs on that handle without reopening it by path. On Windows the file must be opened with
  `FILE_FLAG_OVERLAPPED`, and the `iocp` backend binds it to the driver's completion port. The call is `unsafe`: on
  `iocp` the file takes I/O through that driver alone from then on
- `lore-server`: every gRPC request records a third latency histogram, `rpc.server.handler.duration`, counted from the
  server reading the request body to its end rather than from the request headers arriving. The two it joins,
  `rpc.server.duration` and `http.server.request.duration`, keep counting from the headers, so the gap between them
  holds the time the client took to send its request and any work done before the body is read, such as authorization,
  and a client slow to send skews only those two. A bidirectional stream, whose response is ready before its request has
  been read to its end, records the same value in all three
- `lore-server`: `BranchPush` records how hard its fragment verification drives the immutable store, once per push and
  including pushes that fail or are cancelled part-way. Histograms under `lore.branch_push` give the per-address
  fragment lookups the collection spawns (`fragment_lookups` and `peak_fragment_lookups_in_flight`), the addresses it
  collects (`collected_addresses`), and the existence query batches sent at once (`concurrent_query_batches`). Debug
  logs report the same counts per push, with whether the push returned or was dropped, and per collection step
- `lore`: `lore service status` can be used to get the current status of the currently available Lore service, if any.
- C API: `lore_storage_get_resolved_item_t` gains `fragments`, which delivers the item's content one leaf at a time as
  `LORE_EVENT_STORAGE_GET_FRAGMENT`, each event carrying the leaf's `lore_fragment_t` and its payload as stored, in
  place of `LORE_EVENT_STORAGE_GET_DATA`. No leaf is expanded or checked against its hash, so the reader decodes and
  verifies what it takes. A reader that wants content compressed the way the store holds it takes the stored payloads
  instead of the store expanding them and the reader compressing them again. The field sits in the struct's padding,
  so its size and its other fields' offsets are unchanged, and a zero-initialized item reads as before
- `lore clone --offline` prepares the clone for working offline. A clone needs the remote, so the flag does not keep it
  from connecting. With it, or with `--cache`, a clone that writes its files caches every file metadata block of the
  cloned revision, which staging and committing write to, so both work offline after it, and fails if it cannot. A
  bare clone, or a clone without either flag, reads them from the remote when a command first needs them
- Add a Bazel remote execution server on Lore's storage API under `contrib/bazel/`: a Remote Execution API v2 endpoint
  and scheduler whose action cache and content-addressable storage are Lore partitions, executors that move content
  between their input roots and a shared loreserver by path, and a script that builds, runs and stops them. A
  standalone workspace, built from within `contrib/bazel`
- Opt in to keeping stores in memory between calls with the environment variable `LORE_STORE_RETAIN=1` (or `true`). A
  process then keeps its stores after every repository call, and after `lore_storage_close`, for the keep-alive window,
  `store_keep_alive_seconds` (10 seconds by default), without their locks, whether or not the call sets
  `store_keep_alive`. A call in the window reuses them, and a store reloads only if its `epoch` file shows another
  process changed it. A process with it set cannot safely share a store directory with a process of an earlier build,
  which writes without advancing the epoch, so the retained state is served stale. Unset, the default, a store reloads
  at its first use after its lock was released, and `store_keep_alive` keeps the stores locked for the window. Either
  way a call loads each store at most once

### Fixes & Improvements

- `lore-storage`: a write dropped while it advanced a local store's epoch no longer makes a kept store reload for
  nothing or fails the next write with "No such file or directory"
- `lore-storage`: a claim for work spawned to outlive an operation keeps the store's lock for that work and covers no
  other operation: the guard `try_join_claim` returns, the claim `claim_for_task` takes through the new
  `ImmutableStore::hold_for_task` where the operation could not join, and a closing handle's claims for its flush. As
  spans, they covered every operation outside a span on the store, each of which then ran with no lock once the work
  ended
- A local immutable store's first flush of a group at the full bucket level, which every group of a server store is
  at, writes a bucket emptied since an obliterate, a size eviction or a compaction wrote its file. The flush left the
  file, which kept the removed entries; the next open loaded them back, pointing into packfile ranges compaction had
  since reused, and a read served another payload's bytes for them without error
- Opening a mutable store whose `version` file cannot be read fails rather than taking the store for one without a
  version. Taken for one, the open stamped the store `Initial` under a write claim, and every later open stamped it
  again and advanced the epoch, so every other process retaining the store reloaded it at each use. A store already
  stamped that way recovers at its next command with a connection to the remote: the migration that `Initial` asks
  for skips bucket files already in the current layout and writes a version later opens recognise. Until then, a
  command without a connection fails with "Unable to upgrade local store to current client, need a connection to
  server"
- `lore-storage`: a flush of a local store no longer reports success while a bucket write it overlapped, which failed
  or was dropped part way, left entries unwritten
- `lore-storage`: a flush or an obliterate of a local store dropped part way, as a command that stops waiting for it
  drops it, left the bucket writes it had started running with nothing keeping the store claimed, so another process,
  or a store opened on the directory in the same process, could write the same bucket files meanwhile. Once a fan-out
  commit had claimed every bucket it writes, such a store read the group at the level the commit was replacing; bucket
  files it wrote at that level then sat under the marker the commit recorded, where lookups missed their entries and
  the next fan-out listed a key once per copy. The writes now keep the store claimed until they end, and a store opened
  meanwhile waits for them
- `lore-storage`: a flush or an obliterate of a local store dropped before its bucket writes had claimed every bucket
  they write left the store's lock held once they had written them, until the next use or flush of the store in that
  process, and another process waited for it meanwhile. Each such write now releases the lock as it ends if nothing
  else keeps it
- On a server with `[server.auth] resource_claim` set, a repository lookup by name or id answers any verified caller
  with the repository's id, name and metadata hash, so a client can learn the id it exchanges a token for. Without a
  grant on the partition the lookup answered not found, so `lore repository clone`, `lore link add`, `lore layer add`,
  `lore repository info` and `lore repository delete` failed. Reading the metadata or the contents still needs the
  grant
- `lore commit --link-message` accepts the link path in any spelling that names it, such as `./vendor/lib/`, as
  `lore branch merge` does. The path is checked in the library, so a C API commit naming a path that is not a link now
  fails as invalid arguments. A message for a link with nothing staged is ignored rather than refused
- A storage handle keeps one server-side session per partition, which its calls share while they carry the same
  correlation id. A call with another correlation id, given or generated, replaces it, and closing the handle stops it.
  A revision tree handle keeps its sessions the same way, and repository commands keep theirs on the process's
  connection to the remote. Each storage call used to leave one session open per storage connection on the connection
  every handle to the remote shares, so a process that made about 10,000 storage calls to one remote had every later
  call refused until it restarted, the server allowing that many sessions per connection
- A QUIC storage session lost with its connection is started again on the connection that replaces it, before the
  command that found it lost is sent again. The server keeps sessions per connection, so after a reconnect every
  command on a session started before it failed, and a push that had reconnected failed on its query for the fragments
  the server holds. A command is now sent under a session id only on the connection that issued it, where an id from a
  lost connection could name another session on its replacement
- A storage session pool whose start fails on one of its connections, or is cancelled, stops every session it started,
  those still starting included. They stayed open on the server for the life of their connection, so once
  one connection reached the server's session limit, every retry moved the others towards it as well
- A storage handle, and a server's remote store, reconnect to the remote once the connection they hold has failed. Each
  kept the failed connection, so once the transport had given up reconnecting, every remote call through it failed
  until the handle was opened again or the server restarted. Calls waiting on a reconnect share it. A reconnect that
  fails, or whose connection fails within its back-off, is not repeated until the back-off of 1 to 30 seconds has passed,
  the calls in between failing at once. A refusal of the credentials is not held, so the next call connects again
- A QUIC storage or replication connection reconnects when a request goes out on one of its streams that has stopped
  carrying requests while the connection stays up: the server has stopped reading the stream, as it does once it
  cannot read or frame a request on it, or the client has stopped reading its answers, as it does after one it cannot
  parse. Such a request failed without reconnecting or waited forever for an answer, and so did every later request
  the stream took, until something else made the connection reconnect
- A QUIC storage request dropped while its frame is being written, as by a caller's timeout, has the rest of the frame
  written in the background before the stream takes another request; one dropped before any of it was written sends
  nothing. Requests go to other streams meanwhile, and a stream that does not take the rest within 30 seconds is reset,
  the next request on it reconnecting. The next request on the stream went out in the middle of the dropped one, so the
  server read its bytes as the dropped request's payload
- A QUIC storage session start the server refuses as not authorized forgets the authorization it exchanged and
  presented, and tries once more with the one an exchange then gives. The process served the refused authorization
  from its cache, and from the token store, until it expired, so once the server stopped accepting it, by revocation or
  a change of grants, every storage call for the repository failed meanwhile, in a long-lived process such as the Lore
  service even after logging out and in again
- Caches and prefetches check every payload they fetch on receipt, as reads do, unless the command trusts the remote,
  where they stored most payloads unchecked. A cache walk, which trusts the lists the local store holds, fails once it
  walks deeper than any fragment tree reaches
- Every payload a read loads locally has its sizes checked against its fragment, as every fetched payload has whether
  or not the remote is trusted, and one whose sizes disagree is fetched again and overwritten, as one that does not
  decompress is, where a read that skipped verification read it as the content and panicked
- A resolved read whose local root does not load overwrites it with the root the remote resolves to, whether or not the
  read caches, as a read by address does, where it kept the broken copy and fetched the root on every read
- A write of content the local store holds and the remote does not reuses the stored payload as it is, its sizes
  checked, where it decoded and hashed it first, as the local store is trusted
- `lore-storage`: `read_stream`, `read_into_file` and `read_resolved_into_file` refuse content larger than
  `ReadOptions::max_content_size`, as every other read does, and the root load enforces the cap for each, refusing a
  remote resolve over the cap before storing it. A buffer read by address that misses locally with no remote in reach
  probes the local store once rather than twice, a placement the store answers `SlowDown` is read through the retrying
  local load before the remote, and local content over the cap is refused without being read again. A read into a
  slice shorter than its content is refused before any leaf is read
- `lore repository store immutable query` checks the payload the remote serves against its address, unless the command
  trusts the remote, and logs one that fails. The query reports it as served and does not walk it
- A server reaching its backend through the storage protocol (a `remote` store) checks every payload it fetches against
  its address on receipt, and asks the backend to heal one that fails, where it checked only the payload's sizes. A
  read of it takes the content the check expanded rather than decompressing the payload again. A buffer read probes a
  failed placement again only where the store answers `SlowDown`, whatever the store, so a payload failing its check is
  fetched and heal-requested once rather than twice; a placement the store answers `Oversized` or `NotSupported` fails
  the read, and any other failure is a miss. A payload stored in a codec the server is built without, such as Oodle, is
  refused rather than relayed unchecked, and reads as `NotSupported` rather than as missing. To the server's own reads,
  such as its HTTP content and presigned redeem endpoints, a payload still failing its check after the heal reads as
  missing, as does a backend it cannot reach or that refuses the connection or the request as unsupported
- A server checks the sizes of every payload a replica serves it or a peer replicates to it, over QUIC or gRPC, against
  its fragment, as it does for every other payload, and refuses one whose sizes disagree. A replica's `get` or
  `get_metadata` response too short to hold its header is refused rather than ending the request in a panic
- `lore-server`: a push to a protected branch needs the `push-protected` action on the repository to bypass the
  protection. The `is_service_account` token claim no longer waives it, so a deployment that relied on the claim for
  mirroring has to grant `push-protected` to those callers first
- `lore commit` with changes staged in a layer commits the layer while the parent tree holds only changes marked dirty,
  and leaves those changes dirty. It failed with `Nothing staged for commit` before committing any layer
- A merge of divergent branch history, such as the one a sync commits when the local and the remote latest have both
  moved, records the remote's history in the files it changed, and so does the merge of a linked repository whose
  history diverged in a same-branch `lore branch merge`. The merge is built from the local latest, which can give a
  file another node ID than the remote latest does when both sides added files after they diverged, and the remote's
  change was recorded on whichever file had its node ID in the merge. `lore file history` listed the change, under the
  remote file's path, for an unrelated local file, a file both sides added lost the remote's addition, and a file the
  remote changed before its latest lost that change. A remote latest that numbers a file past the local tree's last
  node block failed the commit of the merge
- `lore file history` of a file the local side added before a merge of divergent history lists that addition. It
  listed nothing, as the file's history before the merge is on the local, second-parent line
- A merge of divergent branch history, such as the one a sync stages when the local and the remote latest have both
  moved, keeps the remote latest as its first parent and the local latest as its second when something is changed on
  top of it before it is committed. Marking a file dirty, staging, moving or copying a file, setting revision or file
  metadata, changing file dependencies or links, and a `lore repository verify` that repairs the staged state replaced
  the first parent with the local latest, and `lore commit --force` committed the merge as an ordinary revision on the
  local latest, so the revision did not descend from the remote latest
- `lore file dependency add`, `lore file dependency remove`, `lore file metadata clear` on a tree with nothing staged,
  and `lore branch merge abort --ignore-links` that restores link pins, no longer carry the current revision's metadata,
  such as its reviewer, onto the next revision
- `lore status --scan` that finds a change in the parent tree while only a layer holds staged changes stages it on top
  of the current revision. It kept the current revision's parents and metadata, so a commit failed with
  `Staged state has stale parent`, or once the change was staged carried the current revision's metadata
- After a merge of divergent branch history is committed, `lore status` reports the branch as ahead of the remote. It
  reported the branch as diverged and asked for another sync until the merge was pushed
- A change committed through a link pinned at a merge revision has that revision as its only parent. It carried the
  pin's second parent, so the linked repository recorded a merge of a revision the pin had already merged
- A change staged more than once inside a nested link before it is committed is committed on the revision the nested
  link is pinned at. Each staging became the parent of the next, so the nested repository's history took in the staged
  states and push uploaded them as revisions
- A merge reports the conflict on a file both branches changed where one of them added the file at a node a deleted
  file had used. The history the node still held for the deleted file could name a merge of that file from the other
  branch, and the merge then took the other branch's change as already merged and dropped it without a conflict
- `lore sync --reset` to a revision where a path is a file, from a working tree where that path is a directory, replaces the directory with the file instead of failing with "Is a directory".
- The check that prevents the CLI from sending the tokens to 3rd party domains now reads the allowed host list from the following token claims: `root_domains`, `https://lore.org/claims/root_domains`, `aud`. These claims tell which Lore servers are trusted as token receivers. The token's `iss` is no longer added as a trusted domain. Token exchange is implicitly allowed against the same auth server where the token originated from, so the `iss` addition is not needed, and unnecessarily widens the token's allowed domains for non-token-exchange use.
- `lore repository create` syncs the created repository to disk only under `--sync-data`, as every other command does.
  It previously always forced a sync
- Opening a repository lists each store's index directory once to find the groups it has written, rather than checking
  for each of the 256 possible groups in turn, which every command paid twice before doing any work
- `lore dirty` on a path inside a link, or on the link's mount, marks the change in the linked repository, so
  `lore status` reports it without a scan and staging the mount, or a directory above it, picks it up. It previously
  recorded nothing. The markers survive a commit and a branch switch, `lore status --check-dirty` persists what it
  verifies inside a link, and a commit pins a link holding only dirty markers at the revision it was linked or updated
  to. `lore file dirty move` and `copy` refuse a path inside a link
- Fragment caching keeps at most 512 fetches in flight per connection to a remote, so a read a command needs at once is
  queued behind at most 512 of them; `LORE_CACHE_GETS` sets another cap, and 0 turns it off. A fetch of content a read
  or another fetch is already fetching, under any context, shares its get. Each fragment it fetches is retried as a
  read's fetch is and stored as fetched and as durable, so the client store may evict it. It is not decoded or checked
  against its address, as a read checks what it loads, unless a read joins its fetch or it is a fragment list, which
  walking expands; one that fails that check is healed once as a read's fetch is, and one that still fails is not stored
  and fails the caching as a failed fetch does. A fragment list the local store holds without its subfragments has them
  fetched, a local list that fails its check is fetched again and overwritten, as a read replaces a corrupt fragment,
  and an entry the store keeps without its payload is fetched as a miss
- `lore-storage`: concurrent reads of content missing from the local store fetch it from the remote once, including
  identical content in different files, and a read that misses while the same content is being stored waits for that
  store and reads it locally. Every fragment a read fetches from a remote is checked against its sizes and its address
  on receipt, whether or not the read asked for verification, and one that fails is neither returned nor stored
- Immutable stores: compaction rewrites a group only once 10% of its packfile bytes are no longer referenced, copying
  each payload once. Client stores count local-only data toward `max_size` and `max_capacity` but never evict it
- `lore repository gc` loads the whole store index before evicting, and applies `max_size` to compaction and
  `max_capacity` to eviction, where it had them swapped
- Local stores: a process stopped while writing an index bucket no longer leaves the bucket empty, which discarded
  every entry in it, such as local-only content or branch state
- Local stores: a failed packfile load, fan-out commit or bucket read no longer discards stored entries or payloads
- Local stores: an index entry that a machine failure left pointing past the end of its packfile is detected when its
  bucket loads, and its payload is stored again when next fetched, rather than read back as other content or
  overwritten by later writes. A packfile whose sync fails takes no more writes
- Local stores: a flush no longer returns before a bucket write that the delayed flush, another flush, eviction or
  compaction had started has landed. A process stopped just after a flush could lose entries the flush reported written
- Local immutable stores: an obliterate writes its tombstone to disk before zeroing the payload, and waits for reads
  that found the payload and for compaction. A process stopped in between reopened with the entry serving zeros, and a
  read could return the payload partly zeroed. An obliterate whose tombstone cannot be written fails and leaves the
  entry as it was, and one that zeroes a payload writes and syncs the index bucket of its entry. Storing the same
  content under another association, or re-encoding it in the Oodle migration, no longer brings an obliterated entry
  back with a size of 0, and obliterating a fragment list the store holds without its payload fails instead of leaving
  its fragments readable
- Local immutable stores: `PayloadStoredLocal` says whether the store holds an entry's payload, whatever flags it was
  put with, so a fragment the local store serves carries it, in `get` and in the C API's
  `LORE_EVENT_STORAGE_GET_FRAGMENT`
- Writing a file cut into one fragment more than a multiple of 6,553, with a last fragment under 40 bytes, no longer
  fails, and pushing a file cut into that many fragments no longer fails whatever the size of its last fragment. The
  file's fragment list ended in a sublist of one reference, which the server refuses, and which the local store refuses
  where the reference is larger than the fragment it names. The last sublist now holds two references
- Local stores: a fan-out commit that fails or is stopped part way no longer leaves index entries in the wrong buckets,
  or files that a later commit's recovery takes in, which served other keys' bytes or brought back an obliterated entry.
  A commit that failed after its commit point is completed by the group's next flush. A failed level marker write no
  longer leaves its temporary file behind
- Local stores: a group of the store index no longer reopens at the wrong bucket layout, where most of its entries could
  not be found, when its directory cannot be listed at open, as when the process is out of file descriptors, when a new
  store is stopped before its first index flush, or when writing a group's level marker fails
- Immutable stores: compaction writes and syncs only the index buckets it changes, rather than every bucket of the group
  for each packfile it rewrites. In a group of 256 buckets, as on a server, that was 256 synced writes per packfile, and
  could stall the store for minutes
- Local stores: a packfile that cannot be listed or read as the store loads its packfiles, as after a transient I/O
  error, fails the load, which the next access retries. It was left out, and when it was the highest, it was taken for
  a packfile never written: the index entries pointing into it were cleared, and payloads held only locally were lost
- Local stores: a store that failed to open its next packfile when one filled, as with no file descriptor left, opens
  one on its next write. Every write failed with `No more packfiles` until the store was reopened
- Local stores: a flush dropped part way, as when its caller gives up on it, runs each group flush it started to its end
  under the group's flush lock, a fan-out commit included, and an obliterate dropped part way runs the write of its
  tombstone's bucket to its end. A write either had started could otherwise land after the lock was released: a
  dropped commit could leave its buckets out of the next commit, or its `level.pending` could land after the next flush
  found none, and the next open then rolled the commit forward over newer bucket files. Either lost entries after a
  reopen
- Local stores: the delayed flush completes a fan-out commit that failed after its commit point before it writes a
  bucket of the group, where the group's buckets waited for an explicit flush. A fan-out commit removes the temporary
  files that writes stopped before their rename left in its group, and a failed write of an immutable store's info file
  leaves no temporary file behind
- Local stores: a store directory that cannot be read as the store opens or loads its packfiles, as after a transient
  I/O error, fails the open or the load, which a later one retries, with the error the read met. A group directory
  taken for one never written was laid out at the initial bucket level, where most of its entries could not be found,
  and a later write then committed that level over its bucket files
- `lore-storage`: a read of fragmented content into a buffer of its own reads each whole leaf the local store holds
  straight into its place in that buffer, and expands a compressed leaf there, as a read into a caller's buffer already
  did. Each leaf was read into a buffer of its own and copied
- C API: an item reading into `data_out` that stops part way no longer writes the caller's buffer after its call
  completes. A read's I/O runs to its end even when the read is dropped, so a call now waits for every read it lent a
  buffer to before it completes. `lore_bytes_mut_t` states that `data_out` must stay valid until then; the header's
  preamble said argument memory could be freed once an asynchronous call returned. The preamble and `lore_shutdown`
  state that `lore_shutdown` is safe only once every call has completed
- `lore-storage`: a read of fragmented content fails when its fragment list's offsets are not strictly increasing. It
  succeeded, with part of the content holding whatever was in memory before, or the bytes of the wrong leaf
- Local mutable stores: a store into a bucket whose delayed flush failed starts another. Until an explicit flush, the
  bucket and every later change to it stayed in memory only
- `lore commit` reports the error that stopped it, such as a staged file it failed to read, in place of
  `Recursion task failed`
- `lore clone` reports the error that stopped it, such as a file it failed to fetch or write, in place of
  `Recursion task failed`. A clone that fails no longer prints `Clone complete`, and the C API sends
  `LORE_EVENT_REPOSITORY_CLONE_END` only for a clone that succeeded, as documented. A consumer reads the outcome of a
  clone from `LORE_EVENT_COMPLETE`, which every clone sends whether it succeeded or failed
- `loreserver`: a revision list starting at a revision numbered above the branch's latest, such as one a client hasn't
  pushed, returns no forward cursor in place of stopping the server
- `lore push` after a push that stopped part way uploads the fragments of a file whose fragment list the server holds
  and the local store has evicted. Before, the push offered the list alone and failed with a missing fragment on every
  retry
- gRPC: concurrent copies of one content to different files each reach the server. Before, a copy answered with the
  result of another, so a push recorded content the server did not hold, and the branch push failed with a missing
  fragment on every retry
- QUIC: the client requests a 512 KiB UDP receive buffer, which Linux allocates as 1 MiB. Linux caps the request at
  `net.core.rmem_max`; set it to `524288` or more for the full buffer
- QUIC: BBR's congestion window no longer grows without limit while flow control holds back the sender, and is
  capped at the connection's send window
- QUIC: BBR keeps the minimum RTT of the last 10 seconds, floored at 250 µs, in place of the lowest sample ever
  seen, so one underestimated sample no longer reduces throughput for the rest of a connection
- `lore`: when the Lore service starts, it logs and skips a registered shared store it cannot open or list, and an
  instance whose external config it cannot read. One such store or instance left the service without its mount
  manager, so no SWFS instance was mounted and none could be created
- A file saved again within the filesystem clock tick it was last written in, at the same size, after a command such
  as `lore commit` or `lore status --scan` read it, is no longer reported unchanged. The command recorded the file's
  modified time even where a later write could carry the same one, and the next status trusted the time over the
  content. Commands now record only times older than the filesystem clock they read before reading any file
- Remote reads and uploads spread over every storage connection a command opens. They all went through one connection
  whatever `--max-connections` said
- On Linux and macOS, two Lore services started at once no longer both run as the service. A service claims its
  socket name with a lock file beside the socket before it initializes, and one that finds the name claimed waits
  until the holder answers, then exits, so the commands that started either reach the same service
- The Lore service's immutable disk stores flush their index 30 seconds after a write, durable writes included.
  Content the service fetches is stored flagged durable, which scheduled no flush, so a crash lost every entry the
  service stored since its last explicit flush. Writes that are not durable now flush 30 seconds after the write too,
  rather than 5
- A local immutable store schedules its delayed index flush after a write that only changes an existing entry's stored
  flags, and after a copy, as it does after any other write. Marking a local entry durable and copying content into
  another partition changed the index without scheduling one, so a crash before the next explicit flush lost those
  changes
- `lore-server`: the generic `MutableStore` and `MutableCompareAndSwap` storage requests refuse the repository
  metadata, branch metadata and branch latest pointer key types on every transport, answering `InvalidArgument` over
  gRPC and the generic `Failed` over QUIC, which has no code for a rejected argument. They previously wrote these keys
  directly, skipping the checks the dedicated requests make: `RepositoryMetadataSet` and `BranchMetadataSet` check
  that read-only fields are unchanged and that referenced blobs exist, and `BranchPush` checks branch protection and
  that the pushed revision's fragments are present. Write these keys through those requests. The C API's
  `lore_storage` mutable store and compare-and-swap calls made with `remote` set, and a server running with
  `mutable_store.mode = "remote"`, send these same requests, so neither can write these key types to a server any
  longer. The C API calls on a handle's local store are unchanged
- The vendored `quinn-proto` moves to 0.11.19, with `quinn` 0.11.12 and `quinn-udp` 0.5.16. A peer's minimum ACK delay
  can no longer panic an endpoint that has the ACK frequency extension enabled, and the remote memory-exhaustion,
  stream-limit bypass and Retry-token issues upstream fixed along the way are closed with it. The MTU black-hole
  detector no longer reads the ordinary congestion loss of a bulk transfer as a black hole, nor keeps a connection at
  the minimum MTU once it has fired. A sender held back by flow control now says so with `DATA_BLOCKED` and
  `STREAM_DATA_BLOCKED`, a path reset also resets pacing, ACKs ride in packets carrying stream data, and path statistics
  report the minimum RTT. The local `max_rtt` and `TransportErrorCode::is_crypto` additions carry over unchanged
- `lore-storage`: the local store's Oodle migration leaves durably stored Oodle payloads in place rather than releasing
  them, so a group holding no local-only Oodle is not rewritten and the pass finishes sooner. A read that cannot decode
  one replaces it with the remote's copy
- `lore-storage`: a read in a build without Oodle that finds a durably stored local Oodle payload fetches the remote's
  re-encoded copy and overwrites the local entry with it, whether or not the read asked to cache. With no remote to
  fetch from, or for a payload stored only locally, the read reports `NotSupported` rather than a missing address
- `lore-server`: the `UrcAuthApi` and ReBAC clients dial `[environment.endpoint] auth_url` over TLS when it's configured
  to use the `ucs-auth://` scheme. The URL was previously handed to tonic unrewritten, which applies TLS only to a
  literal `https` scheme. This change makes the client->server and server->server grpc calls behave in the same manner.
- `lore-server`: `RevisionList`'s forward cursor no longer walks a branch's whole history when a step-boundary skip
  pointer is missing. The boundary nearest the requested page is the one most likely to be absent on history predating
  the acceleration, and a miss there abandoned the binary search over the sealed boundaries and anchored the descent on
  the branch's latest revision instead — one state read per revision in between, which on a deep enough branch spends
  the whole handler timeout and answers `Cancelled`. The search now continues past an unreadable boundary and anchors on
  the lowest one it still proves sealed. A descent that does run logs progress every 10,000 revisions and its totals on
  completion, in place of the line per boundary it logged before
- The vendored `quinn-proto` moves to 0.11.15, bounding how many chunks the stream assembler retains. A peer sending
  out-of-order stream fragments with many gaps could drive the receiving connection's memory through per-fragment
  overhead that the byte-based receive windows do not account for, which is remote and needs no credentials (
  CVE-2026-25800). The local `max_rtt` and `TransportErrorCode::is_crypto` additions carry over unchanged, and the
  transport-parameter parsing fix Lore had been carrying as a local patch is now upstream
- QUIC: the client's response reader and the server's stream handler frame messages with parallel chunking. A message
  whose bytes have all arrived is handed on straight away, rather than waiting behind an earlier message on the same
  stream that is still missing bytes
- `.urc` and `.lore`, in any ASCII case, are the repository's own directories and are ignored in every read: a node
  carrying either name is skipped by clone, sync, status, reset, unstage, diff, the tree and dump listings and the
  virtual filesystem, as a directory of that name on disk already was
- `lore status --scan` given many paths scans them in parallel, up to 1,000 at once, where it scanned each path to the
  end before starting the next, so its time grew with the number of paths given however little each one held. A scan of
  an untracked path creates the directories above it that the tree does not hold yet, so the ones two or more untracked
  paths share are created once before the scans start, and only where a scan would create them: a directory holding only
  ignored or missing paths is still left out of the tree
- Fix a dirty file being left under a directory not marked as holding one, which a later `lore status` or
  `lore stage <directory>` without `--scan` reads past. `lore status --scan` dropped an add the user had reverted as
  soon as it finished the directory holding it, clearing marks up the tree while other directories were still being
  marked, and marking a file stops at the first directory above it already marked. Reverted adds are now dropped once
  every directory and every path of the scan has been walked
- `lore status` lists the files in each section in path order. It listed them in the order the scan found them, which
  changes from run to run once a scan walks several directories or paths at once
- A command carried out by the Lore service reads the global configuration and the credentials of the process that ran
  it, from that process's `LORE_GLOBAL_PATH` and `LORE_AUTH_PATH`. It read the service's own, so a caller that pointed
  either somewhere else was served from the wrong config and token store
- `lore_storage_put`, `lore_storage_put_resolved`, `lore_storage_get` and `lore_storage_get_resolved` can be carried out
  by the Lore service. A buffer a call or a `GET_DATA` event carries could not be read back on the other side of the
  socket, so relaying one failed
- Bound how many tasks resolving a conflict to one side runs at once, which `lore branch merge resolve mine` and
  `theirs` drive, as do the resolves of `lore revision cherry-pick` and `lore revision revert`. It ran a task for every
  directory it was given and another for every change below each, all live at the same time, so peak memory followed the
  size of the resolved tree; the directory tasks and the change tasks below them now draw on one budget of 1,000 in
  flight
- A sync, branch switch, reset, merge or view change sends a `LORE_EVENT_REVISION_SYNC_FILE` event for each file and
  directory it deletes. A delete that succeeded sent none, so a caller tracking the working tree through those events,
  such as an editor updating its asset registry, kept every deleted path. The progress totals counted the deletes all
  along
- Fix a deadlock between processes using one repository: a storage handle or the store keep-alive held a store's lock
  without the repository lock, while a repository command took the two in the opposite order. A store's lock is now held
  only while the store is in use, and the repository lock is taken inside it. Store directories gain an `epoch` file,
  which a process that retains stores compares (`LORE_STORE_RETAIN`). A store closes its packfiles when its lock is
  released and reopens them at its next use

## v0.10.1 (Oct 5th 2026) [#1475]

### Breaking changes

- `lore.thin_client.v1`: `DiffChange.content_from` / `content_to` and `ContentDiffRequest.address_from` / `address_to` /
  `address_base` become a `lore.model.v1.Address`, the form `TreeNode.address` already carried, and move to field
  numbers 10 and 11, and 8, 9 and 10; the hash-only `bytes` fields they replace (5 and 6, and 1 through 3) are reserved.
  A committed file's content resolves under a context generated per file, so a consumer keying content and metadata
  lookups on `hash-context` found nothing for any file in a diff, and had nowhere to put the context when asking
  `ContentDiff` for the file's text. A side that does not exist reports the field unset: an ADD has no from side and a
  DELETE no to side. A link pin change addresses each of its revisions under the linked repository. A consumer built
  against the old fields reads the new ones as absent rather than as a hash, so rebuild against the new bindings
- C API: every storage `*_ITEM_COMPLETE` event replaces `error_code` with an `error` detail carrying the failure's own
  FFI code, message and trace, as `Complete` already did, and the call's `status` becomes the dominant item failure's
  code. Re-check any branch on a per-item code: a missing payload reports `PayloadNotFound` (81) rather than
  `AddressNotFound` (80), a buffer short of the content reports `Oversized` (118) rather than `InvalidArguments` (3),
  and most failures previously reported as `Internal` now report their own code. The structs grow and are no longer
  trivially copyable, so copy the detail's strings before the callback returns and rebuild against the new `lore.h`. The
  revision-tree per-item events are unchanged: they still carry `error_code` as a `lore_error_code_t`, so the five-value
  folding still applies to them, and moving them to a detail will need a second rebuild in a later release
- C API: an empty `lore_string_t` the library emits now carries a NULL `string` pointer rather than a pointer to a
  zero-length NUL-terminated buffer, which is what `lore_string_t` has always documented and what an empty
  `lore_array_t` already answers. A consumer that read `string` without first checking `length` — `strlen(s.string)`,
  `printf("%s", s.string)` — must check `length`, or treat NULL as the empty string. This applies to every empty string
  on every event and every verb, not one field
- C API: `lore_revision_sync_args_t` appends `view`, a path to a view filter file, empty to keep the view the instance
  holds. Rebuild against the new `lore.h`
- Build: the C library is built by the new `lore-capi` package. `cargo build -p lore` builds only the Rust library;
  build `-p lore-capi` for `liblore.so` and `liblore.a` (`liblore.dylib`, or `lore.dll` and `lore.lib` on Windows) and
  `lore.h`, under the same names and paths as before. Building the CLI no longer compiles and links the C library
- Build: the build version is no longer compiled in from `LORE_BUILD_VERSION_NAME`, and a build no longer runs
  `lore revision info` to name itself. Every build reports `<package version>+local` until the new `lore-stamp` tool
  writes a build name into the finished binaries and libraries, which then report `<package version>+<name>` (see
  `CONTRIBUTING.md`). A pipeline that sets `LORE_BUILD_VERSION_NAME` must run `lore-stamp --build <name>` after the
  build and before signing instead. A changing build number no longer invalidates the build cache for every crate
- `lore-server`: a `[server.auth]` without `[environment.endpoint] auth_url`, which authorizes from token claims,
  refuses to start without `[server.auth.oidc]`, since clients have no other way to log in. Add the table with the
  provider's public `client_id`. Tier 2 (`resource_claim` set) also requires `resource_template` or `scope_template` in
  it, and Tier 1 refuses both
- `lore-server`: the server-to-server replication protocol moves `ImmutableCopy` to opcode 21, its request carrying a
  byte of behavior flags where it carried a durability byte alone. Opcode 19 is left unused, so a server on one side of
  the change answers a peer on the other with `UnknownOpcode` rather than reading the request under the wrong shape.
  Copies between servers of different builds fail while a deployment holds both, so upgrade the servers of a replication
  set together. The client-facing storage protocol is unchanged

### Features

- C API: `lore_revision_cherry_pick` and `lore_revision_cherry_pick_async` cherry-pick a revision onto the current
  branch, as `lore revision cherry-pick` does
- C API: `lore_branch_latest_list` and `lore_branch_latest_list_async` list the revisions a branch LATEST has held, most
  recent first, and relay to the Lore service when one is in use. `lore branch latest list` takes the same path, so with
  the service in use it no longer waits indefinitely on the local store the service holds
- `lore sync --view <file>` changes the view filter an instance materializes its working files under, carrying the
  working tree to what the new view holds rather than re-cloning
- `lore-server`: a copy the durable store accepts is read back and recorded, with its content, in the local store and at
  the write replicas, so a later read of the destination is answerable without a round trip to the durable store. The
  read and the writes are detached, so they do not hold up the copy.
  `[immutable_store.composite] record_copy_out_of_band = false` turns it off for a deployment where the extra read and
  write per copy is not worth that, leaving the durable store the only holder of the destination
- `lore-server`: the disk space available to the local stores is checked on a timer and a warning is logged once it
  falls below a threshold. `[server.local_store_monitor]` carries `check_interval_seconds` (default 30) and
  `low_space_threshold_bytes` (default 10 GiB); an interval of 0 turns the check off. Every local store the server
  writes at is watched, the immutable store and the mutable store among them, and the reading is taken per volume:
  stores sharing a filesystem draw one warning naming them all, rather than one warning each. A server whose stores are
  not local is not checked
- `lore file reset --last-merged-from <branch>` takes `--mine` or `--theirs`, restoring the version the reset branch
  held going into that merge or the version the named branch brought in, rather than the version the merge's conflicts
  were resolved with. A merge revision holds the resolved content, so the side that lost a resolution was otherwise
  unreachable; where no merge from the branch is found the branch point is restored as before, whichever flag is given.
  C API: `lore_file_reset_to_last_merged_args_t` appends a `merge_side` (`0` = resolved, `1` = self, `2` = other) in the
  struct's tail padding, so its size and its other fields' offsets are unchanged and a zero-initialized struct keeps
  today's behavior
- `[environment.endpoint] user_url` advertises a user directory that's decoupled from the auth service. Clients resolve
  user IDs to display names using this service's `GetUserInfo` and `GetUserId` operations. For backward compatibility,
  `auth_url` is used as the user service URL, if `user_url` is not defined. Client-side, the lookup moves off the
  `Authentication` trait onto a `UserService` trait
- `lore-server`: `lore repository list` answers through a `RepositoryCatalog` implementation. The implementation is
  selected based on server configuration. A `UrcAuthApi` deployment asks `LookupUserPermissions` as before, a server
  with no `[server.auth]` lists everything it holds. A deployment authorizing from token claims uses a catalog
  implementation based `repository_catalog` and `repository_catalog_url` configuration.
- C API: `lore_repository_delete` and `lore_repository_delete_async` delete a remote repository, and are carried out by
  the Lore service when one is in use. `lore repository delete` is relayed the same way, where it previously always ran
  in the calling process
- `lore-proto`: introduces `lore.user.v1.UserService`, the user directory in Lore's own terms: `UserGet` resolves user
  IDs to users, `UserFind` resolves a name to a user, and `PartitionList` streams the partitions the caller is allowed
  to see. These take over what `GetUserInfo`, `GetUserId` and `LookupUserPermissions` did in `UrcAuthApi`. Only the pure
  OIDC-compliant authn/authz operations are left in `UrcAuthApi` so that we can use OIDC authorization as a drop-in
  replacement for the Auth API.
- `lore-server`: the new `[server.auth.oidc]` table configures OIDC info shown to clients, through the new `oidc` field
  of `lore.environment.v1.Environment`
- `lore-server`: `[server.auth] jwt_typ` lists the accepted JWT `typ` header values, as a string or an array. When set,
  a token whose header carries no `typ`, or a `typ` that doesn't match any of the entries, is rejected. If unset, the
  token `typ` value is not checked, as before
- C API: `lore_revision_bisect` and `lore_revision_bisect_async` take one step of a bisect between two revisions,
  synchronizing the working directory to the revision halfway between them, and relay to the Lore service when one is in
  use. `lore revision bisect` takes the same path, so with the service in use it no longer waits indefinitely on the
  local store the service holds
- C API: `lore_shared_store_list` and `lore_shared_store_list_async` list the registered shared stores and, when asked,
  the instances using each, as `lore shared-store list` does

### Fixes & Improvements

- `lore link list --staged`, and an interactive `lore commit` listing the links it asks a message for, run in the Lore
  service when one is in use, as other commands do, rather than opening the repository in the client, where they waited
  on the lock of a service holding it. C API: `lore_link_list_staged` and `lore_link_list_staged_async` expose the
  listing
- `lore commit` stops with the failure of listing the links or layers its per-link and per-layer messages are resolved
  against, such as a Lore service in use that cannot be reached, where it rejected a `--link-message` or
  `--layer-message` path as matching nothing, or committed without asking for those messages
- `lore repository instance prune` no longer removes an SWFS instance when run without the Lore service, where the
  instance is not mounted and its path is absent or an empty directory. Its external `.lore` in the global data
  directory shows it still exists, so `lore repository instance list` reports it as live and prune keeps its branch and
  revision anchors
- Fix resolving another user's name failing against an auth service reached by IP address over `http://` or `https://`
- A `lore_string_t` or `lore_binary_t` a caller passes in with a NULL pointer and a non-zero `length` reads as empty
  rather than as undefined behaviour, which is what an empty `lore_array_t` already did. The library answers NULL for
  every empty string it emits, so one can come back in an argument struct beside a length the caller kept itself, and
  the pointer alone decides whether there are bytes to read
- `lore-server`: every local store the configuration asks for is reported at startup, composite tiers included. A store
  the configuration names no path for is reported as unconfigured, wherever the generated fallback points, and a store
  inside a system temporary directory is reported as ephemeral whether it was configured there or generated there; the
  two are independent, since either can hold without the other. A local immutable tier naming a path other than the
  first local tier's is reported as unused instead, every local tier being handed the store the first one creates. The
  ephemeral report previously fired on a path being absent rather than on where the resolved path pointed, so the
  shipped `local.toml`, which sets `/tmp/lore-server` explicitly, drew nothing
- `lore-server`: logs default to `warn` rather than `error`, so a server started without `RUST_LOG` reports the
  conditions an operator has to act on. `RUST_LOG` still wins where it is set, and `RUST_LOG=error` restores the
  previous output
- Fix a moved file being deleted when the change that moves it is realized over a working tree that already holds it,
  which is what re-running an interrupted `lore sync` does. The rename finds nothing at the source it was already
  carried from, and the recovery that follows removed the destination and then skipped rewriting it, because whether the
  content was in place was inferred from the view filter rather than read from the rename. It is now read from the
  rename, so a move whose source is not there is written from the store, and one the rename carried is left alone
- Bound how many subtree tasks the walk between two revisions runs at once, which `lore sync`, `lore status` and
  `lore diff` all drive. It previously ran one task per directory it descends into, all of them live at the same time,
  so peak memory followed the size of the tree rather than the work in flight; a subtree that finds no room in the
  budget is now queued for the task that found it to walk, so neither the tasks a walk holds nor the stack it stands on
  grows with the tree
- Fix a command carried out by the service being answered without part of its output. A relayed call returned its result
  while its events were still being delivered to the caller's callback, so `lore status` could report a repository with
  no staged changes. The events are now delivered before the call returns
- A call relayed to a Lore service that cannot be reached now reports its status and error detail on the `Complete`
  event, as a failing command does, rather than on a legacy `Error` event, so a caller of an `_async` entry point
  receives it
- Fix a relative `--repository` resolving against the Lore service's working directory rather than the caller's for a
  command carried out by the service. `lore repository info`, `lore repository delete`, `lore auth info`,
  `lore auth login` and `lore auth logout` reported no repository or no remote where the path named one. The service now
  resolves the path against the directory the command was run in
- Ignore and view filter files are read as UTF-8, and as UTF-16 of either byte order with a byte-order mark or, where
  the rules are mostly ASCII, without one. UTF-32, truncated UTF-16, and mark-less UTF-16 that cannot be detected are
  refused rather than read as UTF-8 into rules carrying NULs, so UTF-16 whose rules lie outside ASCII requires a mark
- Fix the wrong files being removed when a directory leaves the working tree. Where the directory's node could not be
  read, the removal was decided from the incoming revision's tree and filter rather than the ones the working tree was
  materialized under, so it could name files that were never on disk and leave behind files that were
- A change to a file's executable bit alone is now reported by `lore status --scan`, taken by `lore stage`, and recorded
  on the revision `lore commit` produces. A chmod moves neither the content, the size nor the modification time, which
  were all the comparison read, so the change was invisible from the moment it was made
- `lore sync` and `lore branch merge` keep a locally changed executable bit on a file whose content they carry, rather
  than silently reverting it to the revision's. The bit stands as a local modification and blocks nothing; `--reset` and
  `--force` apply the revision's bit as before
- Fix `lore stage` over a large tree failing with `grab_node_unused returned INVALID on a freshly-allocated block`. A
  freshly allocated block of node slots was published for every other thread to grab from before the thread that
  allocated it took its own, so under enough concurrency the block could be emptied out from under it. It takes its slot
  before it publishes the block
- `lore sync <revision>` advances the branch latest to a revision standing ahead of the one the branch holds, the remote
  answering for it. It was left where it stood, so a sync to the remote's tip drew a `lore status` reporting the branch
  behind the remote and a second sync that did not see it was already there. A named revision moves the latest forward
  only, `--local` and `--dry-run` move it not at all, and a divergent branch keeps the one it has
- Fix a moved file being left behind at the path it came from where the move crosses a mount boundary inside the working
  tree. No rename carries a file between filesystems, and the recovery read a destination that was not there yet as a
  failure of its own, so the file was written at its new path from the store and the old one never removed. A move no
  rename can carry now copies the content and removes the source, and a directory is carried a child at a time
- Fix a branch that replaced a committed folder with a link being invisible to a merge, which silently deleted the link
  and dropped its content. A directory paired with a link is the type change it is: `lore branch diff` reports the
  replacement, and a merge that changed anything below the mount conflicts at the mount
- Changes to configuring the Lore service: The service executable field can be left unset if the service was started
  with lore service run separately. There is no longer a fallback that attempts to use the current binary as the service
  binary.
- `lore-server`: permission checks are refused with `PERMISSION_DENIED` when access is checked against the grpc auth
  service, rather than `INTERNAL`
- Fix `lore push` slowing quadratically with the number of entries in a changed directory. The push and its server-side
  verification pair entries in a single pass over name-sorted lists
- Fix `lore stage` staging a delete for a removed file or directory that was never committed, which `lore status`
  reported as `D` and, after `lore unstage`, as an untracked `A`. `lore sync` no longer refuses to run after
  `lore file dirty` drops such a file
- Fix `lore stage --scan` and `lore status --scan` reading a link mount the working tree does not hold as a deletion of
  the link, which a commit then dropped from the branch. A mount stays absent while its linked repository cannot be
  read, and is kept along with the directories on its path

## v0.10.0 (Sep 17th 2026) [#1170]

### Breaking changes

- Partial hash revision identifiers are refused as `NotSupported`. A revision is named by its whole 64-character hash
  signature, by `[branch]@<number>`, by `[branch]@LATEST` or by `<branch>@<hash>`. Three consequences:
    - The `@` is optional, a target given without it applying to the branch the instance is on: `lore sync 42` is
      `lore sync @42`, `lore sync LATEST` is `lore sync @LATEST`. Digits alone are a revision number at every length but
      64, where they are a signature
    - A revision identifies the branch it was created on, and a branch point can name the child branch instead, which
      `<branch>@<hash>`, `[branch]@<number>` and `[branch]@LATEST` all do. `lore sync`, `lore clone --revision`,
      `lore history`, `lore link update --pin` and `lore link add --disable-branching --pin` follow it, and a sync moves
      onto that branch together with its layers
    - `--search-limit` bounds `lore revision bisect` and layer revision matching alone
- `lore-server`: a `signature` field that is not a whole hash is answered `FAILED_PRECONDITION` on `RevisionInfo`,
  `RevisionTree`, `RevisionDiff` and `RevisionList`. An unset (empty) field is unaffected
- C API: `lore_revision_resolve_event_data_t` replaces its `revision` string with a `target` naming what is resolved (
  `NUMBER`, `LATEST` or `SIGNATURE`) and a `revision` hash carrying the signature for `SIGNATURE`, and reports every
  form resolved on a branch rather than the numbered one alone. Rebuild against the new `lore.h`
- C API: `LoreGlobalArgs.no_atime` is removed; it gated a behavior that was never wired up. Rebuild against the new
  `lore.h`; a caller that zero-initializes the struct needs no other change
- C API: `lore_auth_local_user_info_args_t` renames `with_token` to `with_identity_token` and appends
  `with_access_token`, which emits the repository's authorization (access) token as an `AuthIdentity` event. The
  struct's size and the surviving fields' offsets are unchanged — the new flag occupies tail padding
- `lore-server`: `connection_message_limit` under `[server.quic]` / `[server.quic_internal]` is renamed
  `stream_message_limit` and applies per stream rather than per connection, so with `max_bidi_streams = 8` a value of
  500 allows 4000 requests in flight per connection instead of 500
- `lore-transport`: `user_agent()` moves from `lore_transport::grpc` to the crate root, and `set_user_agent()` is
  replaced by `set_fallback_user_agent_product()`, which supplies the product `LORE_USER_AGENT_PRODUCT` overrides. The
  fallback has to be supplied before anything opens a transport connection, since the product resolves on first read and
  is fixed from then on
- `lore-server`: `[server.auth]` refuses to start without both `jwt_issuer` and `jwt_audience`, and `auth_url` without a
  `[server.auth]` block is refused too; both previously started a server that verified no token at all.
  `lock_service.max_encoding_message_size` moves under `lock_service.general`
- `lore-storage`: Oodle is refused for new fragments as `NotSupported`, and an Oodle environment compression mode is
  remapped to Zstd with a warning. Content already stored under Oodle still reads
- `lore repository create` no longer expands a bare name from `LORE_REMOTE_URL`: a name with no host needs `--offline` /
  `--local`. `lore link add` resolves a bare name against the current repository's own remote
- C API: `LoreSharedStoreListItemEventData` is renamed `LoreSharedStoreListItem`, the `_event_data` suffix being
  reserved for real event types. Rebuild against the new `lore.h`
- C API: a call made after `lore_shutdown` reports the new `ShutDown` code (193) rather than hanging, and a second
  `lore_shutdown` reports an error
- C API: `lore_storage_open_args_t` appends `skip_verify`, taking the struct from 64 to 72 bytes, and
  `lore_storage_get_t` / `lore_storage_get_resolved_t` append `data_out`. Rebuild against the new `lore.h`; a
  zero-initialized struct keeps today's behavior

### Features

- `lore auth info --with-access-token` prints the repository-scoped authorization (access) token for the current user,
  the output-side sibling of the global `--identity-token` / `--access-token` input flags. `auth info --with-token` is
  renamed `--with-identity-token`, with the old spelling kept as an alias
- `lore commit --stats` / `lore push --stats` report files by the action each was staged with, and fragments by what
  became of them — deduplicated, compressed, written to the local store, copied as an association by the peer or
  uploaded — through the `RevisionCommitStats` and `BranchPushStats` events, on the failing path as on the succeeding
  one. `--stats=2` adds the per-fragment `FragmentWrite` stream; `--event-interval <milliseconds>` paces the progress
  events
- `lore-storage`: the local store records each entry's last access, so eviction and compaction rank by least recently
  accessed rather than by write time. A stamp dirties a bucket only when the recorded time moves by an hour or more, so
  a burst of reads costs at most one index write per bucket; the stamp advances regardless, so a smaller move rides to
  disk with whatever writes the bucket next, and dirtying schedules no flush of its own
- `lore-server`: request admission is per QUIC stream rather than per connection, so a saturated stream no longer stalls
  the others. A connection-wide ceiling answers `SlowDown` immediately once requests in handling — those still waiting
  for a stream permit included — reach `connection_inflight_limit`, and a permit wait is bounded by
  `permit_timeout_ms` (default 100ms) rather than by the request deadline. The two refusal paths report separate
  `AdmissionLimit` and `PermitTimeout` metric labels
- `lore`: `lore_storage_get_file_resolved` / `lore_storage_put_file_resolved` are the file-backed pair of
  `lore_storage_get_resolved` / `lore_storage_put_resolved`, streaming a fragment at a time so peak memory follows the
  fragment rather than the content. Publishing costs one round trip at any size, and a tree that did not reach the
  remote whole leaves its key unpublished; `lore_storage_put_resolved` gets that saving too. `offset`/`length` select a
  range as they do for `lore_storage_get_file`
- `lore-server`: forward `RepositoryGet` to a remote Lore server, opt-in via `repository_get` under
  `[server.grpc_public_services.forwarded_requests.enabled_rpcs]`
- QUIC clients announce a user agent through a `ClientIdentify` message — opcode 14 on `lore-storage/0.4`, opcode 20 on
  the replication protocol — sent on connect and on each reconnect and stamped on the `user_agent` field of every
  per-request span. The string comes from `LORE_USER_AGENT`, defaults to the product and library version (
  `lore-transport/<version>` unless `LORE_USER_AGENT_PRODUCT` or a supplied fallback names another product), and is
  normalized through the `user_agent_patterns` allow-list: an agent matching no pattern records `<unknown>` and is
  sampled, a connection that announced nothing records `<no_user_agent>`. An empty, oversized or non-ASCII value is
  discarded rather than rejected
- A child directory holding its own `.lore/` is a nested repository and bounds every walk of the parent's working tree:
  `status`, `diff` and `stage` do not index its contents. Naming one on `lore stage`, or a file inside one, is refused;
  a directory the current revision holds stays tracked
- `lore-server`: every public gRPC service takes an `enabled` flag under `[server.grpc_public_services]`, so one binary
  can serve a restricted subset — a read-only `ThinClientService` deployment among them. `lock_store.mode = "none"`
  builds no lock store, and `lore-server/config/thin.example.toml` is a worked example
- `lore-server`: authorization moves onto OIDC and OAuth 2.0, designed in
  `docs/proposals/2026-08-20-oidc-oauth2-authentication.md`. `[server.auth]` gains `permission_claim`, `resource_claim`,
  `identity_claim`, `resource_id_template`, `resource_wildcard` and `baseline_access`; `jwt_issuer` takes a list, so an
  issuer can change its `iss` without an outage; the JWKS URI comes from OIDC discovery unless
  `[server.auth.jwk].endpoint` overrides it; and `env`, `name`, `preferred_username` and `idp` become optional, so a
  stock provider's token parses. The authorizer is chosen from the configuration, and the defaults match today's
  behavior
- `lore repository create` under `--offline` / `--local` takes no URL, naming the repository after the current
  directory, and a command against a remote-less repository reports `NoRemote` rather than an internal fault
- `lore shared-store list` reports the registry of shared stores Lore now keeps, with the repositories each store backs
- Partial support for SWFS, a virtual filesystem implementation: cloning a repository only, gated behind the `swfs`
  feature, and requiring the not-yet publicly available SWFS drivers
- `lore`: `lore_set_compression_mode` and `lore_set_compression_level` choose how stored payloads are compressed, for a
  caller whose data is already compressed. `LORE_COMPRESSION_LEVEL` still wins, and the first write fixes the level
- `lore`: `skip_verify` on `lore_storage_open_args_t` turns off per-read payload re-hashing on a storage handle, for a
  caller that assures integrity at a higher layer
- `lore`: `data_out` on `lore_storage_get_t` and `lore_storage_get_resolved_t` reads content into a buffer the caller
  already owns, saving an allocation and a copy. The read then emits only `GET_HEADER` and `GET_ITEM_COMPLETE`, ignores
  `streaming`, and fails an item over the stated capacity rather than truncating it
- `lore branch diff` reports the branches and revisions it resolved on the `BranchDiffBegin` event, which the CLI prints
  from, rather than only in a log line
- `lore revision sync` warns when the remote is reachable but its revision could not be read, instead of silently
  syncing against local history: `LoreRevisionSyncTargetEventData` gains `remote_available` and `remote_authorized`
- `loreserver` container images are published from each GitHub release's own binaries to
  `ghcr.io/epicgames/lore/loreserver`, signed with cosign. `X.Y.Z`, `X.Y` and `latest` are `linux/amd64`; the
  `-graviton` tags are `linux/arm64` tuned for Graviton3 and newer. See `DOCKER.md`
- `aarch64-unknown-linux-gnu` builds portable by default rather than always tuning for `neoverse-512tvb`, which SIGILLs
  on older arm64. Opt in with `--config .cargo/neoverse-512tvb.toml --features lore-base/neoverse-512tvb`

### Fixes & Improvements

- `lore sync` and `lore clone` retain the target revision's state and tree records in the local cache, so
  `lore --offline status` works after moving to a revision whose records lack the local-cache-priority flag
- Fix an empty array crossing the C API allocating a block that was never freed, so a long-lived client leaked one per
  event it received — an empty `trace_locations` on the error detail of a successful outcome being the common case. The
  zero-sized allocation was also undefined behaviour
- Fix revisions committed with `lore commit --offline` being unreadable from every other clone once they were pushed,
  where a clone reported a revision's signature and parent but no branch, date or message and
  `lore revision metadata get` answered `Address not found`. Push now walks the second parent of a merge revision,
  oldest first, uploading what each revision owns without offering it as a new latest revision; the walk stops at the
  peer's latest for that branch, or at the branch point for a branch the peer has never seen, and a revision on that
  line that is itself a merge is followed the same way. The server collects a merge against both parents rather than
  only the first, so a push missing the tip of the merged line is refused rather than accepted silently
- `lore revision info` and `lore history` warn when a revision names metadata they cannot read and report it without the
  branch, date and message that blob carries, rather than reporting empty fields with no indication anything was
  missing (`revision info`) or failing outright (`history`). A revision read offline out of a cached revision list has
  nothing to take those fields from, which is ordinary rather than damage
- Fix `lore revision metadata set --binary` leaving its payload on the machine that set it, where every other clone
  resolved the recorded address to `Address not found`. Push now collects the fragments an address-typed value names, as
  it already did for file metadata, and the server verifies them with the revision's other new fragments
- Fix `lore repository instance list` carrying several entries for one root directory. Registering an instance retires
  any earlier registration at the same path. A `.lore/instance` naming another instance is reported `superseded` (
  `stale = 2` in the `RepositoryInstance` event) and retired by any command that reads the list; a path that has gone (
  `stale = 1`) or holds no `.lore/instance` (`stale = 3`, printed `no checkout`) is reported but left for
  `lore repository instance prune`; an instance file that exists but cannot be read leaves the registration alone
  entirely, since only positive evidence makes a registration stale. Stale entries never raise the warning that another
  instance holds the branch, and a lost `.lore/instance` is recovered from the newest registration for the path
- `lore-server`: a forwarded RPC whose peer is unreachable now answers with the origin's own `INTERNAL` status and logs
  the transport failure, instead of passing tonic's `UNAVAILABLE`/`tcp connect error` through. Affects `BranchCreate`,
  `BranchDelete`, `BranchGet`, `BranchList`, `RepositoryCreate` and `RepositoryGet`
- `lore-server`: the channel a forwarded RPC travels over now sends HTTP/2 keep-alive pings while idle and bounds its
  connect and each request, so a silently dropped connection is redialled and a peer that never answers no longer holds
  the forwarding handler. Tuned by `connect_timeout_seconds`, `request_timeout_seconds`, `tcp_keepalive_seconds`,
  `http2_keepalive_interval_seconds` and `http2_keepalive_timeout_seconds` under
  `[server.grpc_public_services.forwarded_requests.client]`, all optional
- `lore`: `lore_storage_get_file` no longer creates, truncates and replaces the destination when `offset` starts past
  the end of the content; the target is left alone and the call reports `INVALID_ARGUMENTS`. A start exactly at the end
  is still a legitimate empty read and writes the empty file
- `lore`: `lore_storage_put_file` and `lore_storage_put_file_resolved` reject a missing or non-regular `path` as
  `INVALID_ARGUMENTS` on the first open rather than after the ten-second transient-failure back-off, so a directory can
  no longer retract a published key by reporting zero size
- `lore-storage`: a write that waits on another task already storing the same address reports the placement the store
  settled at, so content whose fragment tree repeats a leaf — a file of identical blocks — is no longer reported as
  partly absent from the remote, and a key naming it is published instead of withheld
- `lore-storage`: a stopped garbage collection pass gives up within one packfile instead of after a whole compaction
  step, and the stop at the end of every repository command is gone, so background eviction and compaction continue
  across commands
- Fix a directory staged as an add and then removed before any commit being reported as a delete no command could clear;
  a scan now discards the entry with its whole subtree
- Fix `stage --scan` keeping an entry `status --scan` discards, which left the two walks with different trees
- `lore branch merge`, `branch merge into` and `revision cherry-pick` carry nothing from the source revision's metadata
  unless `--inherit-metadata <KEY>` (repeatable) names it, so `created-by`, `reviewed-by` and `change-request` no longer
  follow the source onto the new revision. `message`, `timestamp`, `branch`, `committed-by` and `merged-by` are written
  by the operation that creates the revision, and `cherry-picked-from`, `reverted-from`, `restored-from` and
  `fast-forward-merge` record an operation the new revision is not; neither set is inheritable, including under the `*`
  sentinel
- Fix `lore branch merge` refusing files that carry no local changes; the working file is measured against the node the
  current revision holds rather than the base of the three-way diff, and a file is read and hashed at most once however
  many revisions it is measured against
- `LoreFileHistoryEventData` and `LoreRevisionInfoDeltaEventData` gain a `from_path` field, set for moved files and
  empty for added, modified and deleted actions
- `lore branch diff`, `lore revision diff`, `lore file history` and `lore revision info --delta` print both paths for a
  move, as `V old -> new`
- Fix `lore revision diff` reporting a move as a delete and an add instead of one change
- `lore-storage`: fix local store entries becoming unreachable — reported as `Address not found` while the payload is
  still in the packstore — when the background flush wrote a group's bucket files without writing its level marker,
  leaving the next open to read the group at the pre-fan-out layout of 256 buckets, where nothing a lower level wrote is
  looked up again. Every path that writes a bucket file outside the two-phase commit now commits the group's level
- `lore-storage`: fix `lore branch archive` reporting success while leaving the branch's name-to-id mapping on disk, so
  it kept appearing in `lore branch list`. Concurrent flushes of one group could publish an older snapshot over a later
  write; a per-group flush lock makes the path selection and the writes one unit
- `lore-revision`: the wait for the filesystem clock to pass a recorded modification time is bounded at 10 ms, so a
  coarse or fixed clock cannot block a command on it
- `lore-revision`: fragments the peer reports it already holds are marked durable locally, rather than accumulating as
  non-durable entries eviction cannot reclaim and every later push re-queries
- `lore repository delete` honors `--dry-run`. Name and ID resolution still runs, so a bad URL is still caught
- `lore lock acquire`, `release` and `status` report the server's own denial reason — `resource already locked` among
  them — instead of a generic failure, and a refused batch rolls back the locks it took
- Read verbs no longer connect to the remote where local data is the answer: `lore revision history --branch`,
  `lore file history`, a `branch@LATEST` resolve, and the CLI's user-name lookup. Against an unreachable server a
  `--local` history falls from about 4 s to 150 ms
- `lore stage --dry-run` no longer persists the staged anchor, which made a later real stage report `No changes staged`
  and let a commit pick up the preview state
- `lore-revision`: the Unreal package tag check compares all four bytes rather than three against a four-byte value,
  which made it answer false unconditionally. No file is classified differently today
- Fix merge conflict markers being glued onto the content lines when both sides end the file without a trailing newline,
  which left the file unparsable. `merge resolve mine` and `theirs` restore the committed bytes exactly
- `lore-server`: fix revision-list acceleration sealing the boundary that contains the new revision number rather than
  the one crossed, so a push from 99 to 105 sealed 200 instead of 100 and any lookup from 106 to 200 aborted. Stale step
  keys are discarded by a key rename
- `lore-server`: `RevisionInfo`, `RevisionTree` and `RevisionDiff` resolve a `branch@number` identifier through the
  history step acceleration instead of walking the parent chain from the branch head per request. One implementation
  replaces three, so an unusable step key falls back to a walk everywhere rather than reporting an existing revision as
  missing
- `lore-server`: fix `RevisionList` reporting no newer page whenever the next revision number was not exactly one past
  the page's first item, which a merge or fast-forward makes routine. The next page is found through the branch's own
  step boundaries, and a store failure is reported rather than read as the end of history
- `lore dirty` on a path inside a layer records the marker against that layer rather than the parent. A modified layer
  file was recorded as an untracked add in the parent and a deleted one dropped silently, so neither `status` nor a
  directory-scoped `stage` saw the change
- `lore-base`: a call made after `lore_shutdown` fails rather than hanging forever. The event forwarder was spawned onto
  a runtime already torn down, so the wait for its `End` event never ended
- `lore-revision`: a walk steps the filter one component at a time instead of re-folding the whole path per node — 2.3x
  at depth 12, 8.6x at depth 40 — and a forced walk consults it not at all. Two link crossings were also wrong: `reset`
  carried the link node's own verdict into the content below it, and `unstage` matched a linked file by the linked
  repository's spelling rather than the mount path
- `lore link add` refuses a mount whose source path strictly nests inside another mount of the same repository. Both
  mounts placed the same content under separate pins, so an edit through one went stale in the other and a stage walk
  kept whichever it reached last. Identical and disjoint source paths stay allowed
- `lore-server`: the storage layer's `SlowDown` backpressure reaches the client as `RESOURCE_EXHAUSTED` instead of
  `NOT_FOUND` or `INTERNAL`, and a throttled cache read no longer falls back to a full history walk against an
  overloaded store. A throttled branch head ends a `BranchList` stream rather than omitting the branch
- `lore-server`: a fragment `put` carrying no payload fails hash validation instead of leaving the address unverified
- A `BranchPush` refused for a missing fragment answers `FAILED_PRECONDITION` carrying the address rather than
  `NOT_FOUND`, which the client read as a missing branch and answered by recreating it. The client now reports
  `missing fragment <address>`
- Staging a large changelist no longer logs every path it walks at debug: `Staging path`, `Stage file` and
  `Stage directory` move to trace, and a resolved case is reported at debug only where it had to be corrected.
  `Stage file` also named a path that did not exist
- `lore-server`: an HTTP/2 POST whose body carries no decodable gRPC message — what a plain `curl` sends — is reported
  as `InvalidArgument` rather than `Internal`, so `Internal` keeps meaning a genuine server fault
- Performance: staging a path at depth *n* stats the working tree once rather than once per component; a scan allocates
  5.9 times per file on an unchanged tree where it allocated 16.2; and a one-item storage batch runs on the calling task
  rather than spawning
- `lore-server`: a trusted internal QUIC connection's `ClientIdentify` user agent is recorded as sent, so a deployment
  need not list its own server-to-server agents in the user-agent filter
- A staging walk settles a type replacement rather than only reporting the delete and the add: the displaced subtree
  takes a staged delete and a directory replacing a file is descended. Staging twice is idempotent, a staged delete is
  taken back where the file is still there, and a staged add records the size and mode measured. `branch merge`,
  `cherry-pick`, `revert`, `reset` and `unstage` hold one filesystem operation per diff instead of freezing the working
  tree per file
- Link and layer paths are reported and resolved from the working tree rather than from the linked repository's own
  spelling. A layer mounted away from its source path can be synced and reports changes at the mount, a conflict from a
  subtree link is named under the mount rather than dropped, `status --check-dirty` clears and counts markers inside
  layers, and staging a delete below a shared pre-created ancestor resolves the link chain from there
- `lore unstage` and `lore reset` route a path inside a layer into the layer's own states, where before both reported
  success and left the layer untouched — leaving a staged layer blocking `lore sync` with no route forward that kept the
  file. `reset --revision` on a layer path is now refused
- `lore-io`: `create_dir_all` succeeds wherever the directory is already there, so a Windows container bind-mount root —
  which answers `PermissionDenied` for `mkdir` — no longer fails `sync` and `clone`. Rust 1.94 narrowed the standard
  library to forgiving `AlreadyExists` alone
- `lore-storage`: healing a failed `verify_fragment` drops the associations naming the bad payload rather than clearing
  the payload pointer. A payload-less entry answered a full match while serving nothing, so peers were told content was
  available that the store could not serve, blocking re-upload. Obliterated entries are left alone
- `lore-server`: a replicated store's `get_metadata` and `query` take the two reserved priority QUIC streams instead of
  queueing behind bulk fragment traffic — p50 falls from 15.4 ms to 1.6 ms behind 128 concurrent 32 KiB gets
- Switching between branches that mount one repository at different source paths no longer coalesces the mount delete
  and add into a rename, which left the mount holding a subtree its own source path does not name. A link node's
  identity names the repository it mounts, which every mount shares, so it is no longer read as a file identity
- `status --scan` reports each change as the walk finds it rather than after the whole diff is collected, so the first
  event fires during the scan and the walk always completes, leaving dirty marks whole
- `lore`: a storage session error keeps its classification instead of being wrapped as internal, so a peer's `SlowDown`
  reaches the retry paths in read and write again
- Documentation that described `Repository.created`, `Branch.created`, the thin-client commit timestamp and
  `lore revision history --date` as Unix epoch seconds is corrected to milliseconds. No wire or on-disk value changes;
  `--date` was the one place the documented unit made the filter never match

## v0.9.0 (Aug 28th 2026) [#782]

### Breaking changes

- `lore-credential`: auth tokens move to `tokenstore.toml` under a `tokenstore_encryption_key` key, with nothing
  migrated from `tokens.toml`; old and new clients keep entirely separate credentials, so expect one `lore login` per
  generation. An unmigrated store reads as `Not authenticated` and `--remote` reads answer empty rather than failing, so
  scripts gating on `lore status` should check the exit code
- `lore-server` (AWS store): the fragment describing a payload now travels on the S3 object as an
  `x-amz-meta-lore-fragment` header instead of in a DynamoDB record, and the fragment metadata table is replaced by a
  fragment state table holding lifecycle state alone (a row's presence means the hash exists). Existing objects are not
  rewritten — they are read through a fallback to the old table, which must stay configured for them to remain readable.
  Under `[plugins.aws.immutable_store]`:
    - Existing deployment: set `dynamodb_fragment_state_table` (required, no alias — start fails without it; normally
      the table that held fragment metadata, since the key schema is unchanged and the two row shapes coexist) and
      `dynamodb_fragment_metadata_table` (enables the fallback read, normally the same value; accepts the older
      `dynamodb_metadata_table` spelling). Roll out as a full stop followed by a full start — old and new servers must
      never run at the same time. To move the data across and retire the old table, see
      `contrib/aws-migrate-0.9.0/README.md`
    - New deployment: set `dynamodb_fragment_state_table` only. Leaving `dynamodb_fragment_metadata_table` unset
      declares that no object predating the change exists, so no fallback read is ever issued and an object without its
      own metadata is reported as damaged
- `lore.model.v1` and `lore.thin_client.v1`: `Repository.created`, `Branch.created` and `Revision.timestamp` now carry
  Unix epoch milliseconds instead of seconds
- C API: `LoreMetadataType` discriminants are now stable integers shared between `lore.h` and the on-disk metadata
  buffer, and `lore_revision_tree_metadata_set` takes typed `(key, LoreMetadata)` batches instead of text plus a format
  tag; callers using the enum names need a recompile, and callers hard-coding the old numeric values must update them
- `lore-server` (replication protocol): `ExistsBatch` is replaced by a batch `Query`, and `Get` / `GetMetadata`
  responses carry the full `StoreGetData` including the partition. Retired opcodes are reserved rather than reassigned,
  so a peer on the previous protocol is rejected instead of misreading a response — roll replicas and their upstream
  together
- C API: failures that previously reported `-1` (internal) now carry the specific code where one applies, because an
  error crossing an internal boundary keeps its variant instead of collapsing. `lore_revision_tree_metadata_set` and its
  file equivalent can now return `SlowDown` (5), `NotAuthorized` (7), `Maintenance` (11), `NotAuthenticated` (12),
  `NoRemote` (14), `NotConnected` (17) and `NotSupported` (18); remote store reads and writes add `Disconnected` (6);
  and connecting with no stored credential reports `NotAuthenticated` rather than an internal fault. A caller treating
  every non-zero status alike is unaffected; one that branches on `-1`, or reads `-1` as `retrying will not help`, now
  sees retryable and re-authenticable codes on paths that previously only ever produced `-1`, and should handle them
  before upgrading

### Features

- `lore-io`: new runtime-independent asynchronous file I/O engine backing Lore's file access — positional owned-buffer
  operations on a bounded, idle-reaped syscall pool, upgraded automatically to `io_uring` on Linux and overlapped I/O on
  Windows, with vectored scatter/gather reads and writes. `LORE_IO_BACKEND` overrides the choice; internals in
  `docs/developing/internals/file-io-engine.md`
- `lore`: `lore_revision_tree_commit` freezes a handle's in-memory tree into a revision and advances the branch tip by
  compare-and-swap, taking the branch from the handle's `branch` metadata key. Commit is exclusive and all-or-nothing —
  a failure leaves the handle where it was with every edit still staged — so services can publish revisions with no
  working tree on disk
- `lore`: `lore_revision_tree_delete`, `_modify` and `_move` join `add` as batch verbs, each validating the whole batch
  before any node changes and emitting a `*_COMPLETE` per entry plus one `BATCH_COMPLETE`. A node from the loaded
  revision is staged for deletion and reversible, one added through the handle is discarded outright, and
  `staged_action` on the child and node-info events reports what is pending. Moving into a linked repository is not
  supported yet
- `lore`: batched `lore_revision_tree_metadata_set` / `_get` / `_clear`, with `LoreMetadata`, `LoreMetadataType` and
  `LoreBinary` reworked into owning, self-describing types so binary metadata can cross event callbacks and both wire
  formats (see Breaking changes)
- `lore`: a revision tree handle holds its own store reference, so closing the parent storage handle leaves it usable;
  orphaned handles are closed when an IPC connection drops, and `lore::shutdown` drains tree handles first
- `lore`: `lore_storage_get` / `lore_storage_get_file` take `offset` and `length` per item to read only a slice of the
  content, pruning the fragment tree so the work is proportional to the range rather than the content size (a zeroed
  pair still reads the whole content; a start past the end is rejected with `INVALID_ARGUMENTS`)
- `lore`: `lore_storage_get_resolved` / `lore_storage_put_resolved`, also over QUIC and gRPC, resolve a key belonging to
  another system — an asset id, a build id — and act on the content it names in one request instead of two. A read
  resolves local-first and caches the mapping it learns, verifying the root fragment against the resolved hash; a write
  stores content before publishing the key and is last-writer-wins. Design:
  `docs/proposals/2026-08-02-resolved-storage-operations.md`
- `lore branch archive --include-layers` / `--layer <path>` archives the branch in every configured layer, or in the
  layer at one mount path; the default still touches only the repository it ran in, since archiving deletes and a layer
  owns its own branch lifecycle
- `lore --identity-token <token>` / `--access-token <token>` (and the matching `LoreGlobalArgs` fields) use
  caller-supplied tokens instead of the credential store, for CI runs and stateless services. Tokens are ephemeral and
  never stored, the identity is read from the token, and both conflict with `--identity`; given only an access token, an
  operation needing an identity token fails rather than falling back to a stored one
- `LoreRepositoryCreateArgs` / `LoreRepositoryCloneArgs`: `use_shared_store` is replaced by a `LoreSharedStoreMode`
  enum (`Inherit` = 0, `Enabled`, `Disabled`), so a caller can explicitly refuse shared-store backing on a machine where
  `use_shared_store_automatically` is set (a zero-initialized struct still follows the machine setting)
- `ImmutableStore`: `exist` / `exist_batch` are unified into a batch `query` answering with the best match level found,
  and `get` / `get_metadata` drop their match-level parameter for one `StoreGetData` carrying the fragment, the level
  and an optional payload. The contract — never over-report, obliterated never matches, reads agree with each other and
  name where a match was found — is written into the trait and enforced by a conformance battery every store runs
- `lore-server`: fragments arriving Oodle-compressed on `put` are transcoded to Zstd as the first step of retiring
  Oodle, gated behind the `oodle` feature and opt-out at runtime with `LORE_DISABLE_CONVERT_OODLE_ON_PUT`; the address
  is unchanged, since identity is over uncompressed content
- `lore-aws`: `MetadataMigrator` and `run_migrator` drain legacy DynamoDB fragment-metadata rows into the new
  S3-object-metadata plus state-row model, by segmented parallel scan across a pool of consumers. An accurate non-Oodle
  codec is re-uploaded as-is to set its S3 headers, Oodle and mismatched codecs recompress to Zstd, and a run is
  idempotent and resumable. Packaged as a standalone tool under `contrib/aws-migrate-0.9.0`
- `lore link add` / `remove` / `update` / `reset` / `list` now work on a nested link — one mounted inside another link's
  subtree — mutating the innermost repository's registry and propagating outward, and `lore commit --link <nested path>`
  commits each intermediate link as a real revision before repinning. Nested-link merge is not covered yet
- `lore-server`: the legacy `urc.rpc.RevisionService` diff and tree gain `link_partition` and `tracking` fields, so a
  consumer can tell which repository a changed path resolves under and whether a link follows its parent branch or is
  pinned (both additive on messages already marked deprecated)
- `lore-server`: `presigned_url_extra_content_types` and `presigned_url_denied_content_types` under `[server.http]`
  adjust which `Content-Type` values a redeemed presigned URL may carry. They extend rather than replace the built-in
  safe set, a type in both lists is denied, and browser-executable types can never be added — the server refuses to
  start instead of failing open
- `lore-proto`: thin-client `TreePath` exposes file size and mode, so a `ThinClientService.RevisionTree` caller gets
  both without a second request
- `lore`: self-signed certificates installed in the OS trust store are now honored (`reqwest`'s
  `rustls-tls-native-roots`), which local development against a self-signed server needs
- `lore link info <path>` reports one link's mount and source paths, its branch and whether that branch is followed or
  pinned, the pinned and remote-latest revisions, the link flags, and the staged state and staged file count inside it;
  the remote revision is omitted when offline

### Fixes & Improvements

- Thread model: the network transport moves onto its own runtime, the rayon compute pool is gone with compression,
  hashing and chunking now inline on the core workers, and the blocking pool shrinks to two threads. `LORE_MAX_THREAD`
  is now an absolute cap. Clone throughput 1.4 → 2.5 Gbps, large-commit peak memory 6 → 4 GiB
- `lore-storage`, `lore-revision`: the store buckets, packstore, fragment chunker and remaining `tokio::fs` / `std::fs`
  calls all run through `lore-io`, so a common-case bucket load completes in one dispatch and a flush gathers header,
  index and entries in one vectored write; filesystem locks back off asynchronously instead of parking a thread
- `lore-storage`: fix a file shorter than its measured size being taken as an early end, producing a chunk list covering
  fewer bytes than its root fragment recorded; the read now fails
- Windows: no handle the I/O driver opens permits a second writer, while a read-only open still shares reads and
  deletion so replace-by-rename works. `ERROR_SHARING_VIOLATION` is now transient and retried, so materializing a file
  being hashed waits rather than fails
- Fix `lore branch merge` under a sparse view dropping changes outside the view, leaving the branch divergent from the
  one it recorded as merged; nodes now merge regardless of the view, which gates only on-disk work, and an out-of-view
  conflict adopts theirs as `StagedMergeTheirs` (adopting untouched excluded subtrees whole made it 34% faster on
  110,000 files)
- Fix a link added, removed or repinned on one branch not surviving the merge that brought it over, where `link list`
  reported nothing and `lore stage` failed with `Link not found`; the registry now follows any link node a change set
  stages or deletes, and a row both sides moved refuses the merge
- Fix a revision moving a link's pin producing an empty diff when the subtree was byte-identical, and entries from a
  linked repository naming no repository so content could not be fetched from the right partition; a new `link_read`
  policy gives a caller authorized only for the parent one change for the mount path
- Fix `lore branch create` failing with `Branch <name> already exists` when a linked repository held the requested
  branch id under another name; the cascade adopts it and reports a `LinkBranchCreate` event with a `reused` flag
- Fix concurrent link edits overwriting one another: `link add`, `update` and `remove` released the runtime lock between
  reading and storing the registry, and now hold one write lock across it
- Fix `lore push` on a repository with links reporting the parent's revision against a branch that exists only in a
  linked repository; push events now carry the repository and branch they report (repository state was always correct)
- Fix `lore stage . --scan` pinning a staged revision for unchanged layers, which aborted the next `commit` with
  `Nothing staged` and made `branch switch` refuse with `Layer has uncommitted staged changes`; a layer is pinned only
  when the walk left its state dirty
- Fix `lore sync` leaving a layer's staged state on the revision it moved away from, so the next commit refused it as a
  stale parent or under `--force` reverted what the sync brought in; sync now refuses when a layer holds staged work and
  otherwise rebases onto the new pin
- Fix `lore layer remove` discarding a layer's staged work silently and leaving staged adds on disk; removal now counts
  staged files and refuses without `--force`
- Fix an unreadable `.lore/config.toml` or `layer.toml` presenting as a repository with no remote or no layers, which
  the next save made permanent; both now default only for `NotFound` and save through a renamed `.tmp` sibling
- Content the peer already holds is no longer re-uploaded: where a query reports the hash under another context or
  partition, the client asks the peer to duplicate the association instead of sending the payload, on `lore push` and
  direct writes alike
- `lore-revision`: the composite store caches metadata resolved by `query`, not only `get_metadata` hits, so a `query`
  -heavy workload stops hopping to the durable store; capped by `cache_metadata_semaphore_size` and limited to
  durable-sourced results, and `should_cache_query_results` is renamed `cache_metadata`
- `lore-server`: `get_metadata` now fans the durable store out in parallel with read replicas and sends a dedicated QUIC
  command, instead of falling back to a local `query` that reports only the durably-stored flag; edge replicas answered
  incorrectly and always paid a cross-region trip
- `lore-server`: the JWK service picks up key rotation without a restart; a verification failure only a key could
  explain triggers one refresh, so material replaced behind an unchanged `kid` no longer rejects every token
- `lore-server`: JWK fetches are bounded by timeouts, a pooled client, single-flight collapsing of concurrent misses and
  a minimum interval, with an empty cache never throttled so start-up always proceeds. The document is capped at 1 MiB
- `lore-server`: a JWK set is validated key by key — an unusable key is skipped rather than failing the fetch, a key
  whose `alg` family does not match its `kty` is refused, and an empty result errors. RS256 is inferred for an RSA key
  omitting `alg`, which previously failed start-up against Microsoft Entra ID
- `lore-server`: why a JWT verification failed no longer reaches the caller — `bad signature`, `expired` and
  `no such key id` are an oracle for someone unauthenticated; the reason goes to a debug log
- `lore-server`: fix a stored-XSS vector on presigned-URL redeems, where bytes could be served from the Lore origin
  under a caller-chosen `Content-Type` such as `text/html`; a deny-by-default allowlist rejects at mint, coerces on
  redeem, and every response carries `nosniff` and a `default-src 'none'; sandbox` CSP
- `lore-server`: `binary/octet-stream` is allowed in the presign allowlist and served verbatim — S3 assigns it to
  objects uploaded without an explicit `Content-Type`, so callers forwarding S3 metadata had theirs coerced
- `lore-server`: `RepositoryMetadataGet` and `RepositoryMetadataSet` now perform the real-time authorization check the
  other repository handlers do, so a client cannot read or write metadata on a repository it has no access to
- `lore-transport`: fix a per-item failure on the streaming storage RPCs being reported as a stream trailer, which
  killed the HTTP/2 stream and every request multiplexed onto it; outcomes now travel in-band, and field numbers are
  preserved so get responses stay wire-compatible
- `lore-transport`: fix gRPC reconnect not re-reading the epoch per attempt, and storage streaming having no working
  reconnect at all; a dead stream now rotates once across concurrent callers and replays outstanding requests
- Fix `lore` commands taking about 30 seconds when a hostname resolved first to an address family the server was not
  listening on, commonly `localhost` on `::1`; the QUIC client now follows Happy Eyeballs (RFC 8305), 30.05 s to 0.38 s
  on the reproduction
- `lore-transport`: fix the per-stream in-flight counter never being decremented on error or cancellation, which
  degraded priority routing and left the selector round-robining; also fix `add_stream` storing a count one short of the
  streams it opened
- `lore-aws`: `query` is backward compatible with fragments described by the legacy metadata table, so `BranchPush` no
  longer reports content as missing when it is stored but described by the old row
- `lore-aws`: the mutable store's zero-expected compare-and-swap matches the local store now — a row holding a zero
  value is treated as absent — so an empty branch pointer cannot make a swap report success without the write landing
- `lore-storage`: a corrupt mutable-store bucket is no longer reset to empty on an authoritative store, where it
  silently dropped branch heads, metadata and instance registrations with no upstream to refill from;
  `authoritative: true` makes it a hard error
- `lore-credential`: fix the credential store rewriting its keyring entry on every stored token, which on macOS prompted
  for keychain access from every application linking Lore and again after each rebuild; each seal now draws a random
  nonce instead of persisting a counter
- `lore-credential`: fix any keyring error being read as `no key`, so one denied prompt regenerated the key and wiped
  the token store for every Lore process on the machine; only `NoEntry` counts as absent
- Fix a notification subscription that had stopped on its own still counting as live, so every later subscribe returned
  success without subscribing; liveness is now checked by cancellation token and task state
- Fix `lore history --branch <name>` returning an empty listing for a branch that exists only on the remote; the local
  branch is preferred and the remote head used when there is no local history
- Fix an infinite loop walking revision history for a deleted file, where a zero parent hash was handed to
  `State::deserialize` repeatedly; a three-way diff base no longer resolves to the zero revision
- `lore-revision`: the diff and merge base search no longer errors as `Divergent` when it cannot prove divergence —
  branch points sharing no revision fall back to the older one, and two points with the same revision number
  short-circuit instead of spending a full search budget
- Fix `lore reset <path> --revision <revision>` failing when the path is currently a directory but was a file in that
  revision
- Fix `lore sync` undoing a change from a file to a directory; where an add and a delete carry the same node name, the
  delete now takes the `from` node
- Fix a clone failing with `Failed to create directory` when the parent already existed but was not created by that
  call — a bind-mounted clone root or a drive root
- Fix `lore stage --targets <file>` hanging on a large list, where `dedup_to_supersets` rescanned every kept path per
  candidate and timed out around 900,000 paths; sorting in subtree order takes the collapse from quadratic to O(n log n)
- `lore stage` no longer grows memory in proportion to the work ahead of it; in-flight tasks are capped at 1000 and
  drained as they complete, and directory fan-out is bounded by a semaphore, processing a child inline when no permit is
  free
- `lore-revision`: node allocation during staging no longer holds a single-permit mutex across its whole scan, and
  `node_add` skips the 65 KB zero allocation it issued when clearing a recycled slot whose metadata block was never
  written
- `lore-revision`: the per-node stage log lines drop from debug to trace; they fired once per file and made `--debug`
  unusable during a large stage
- `lore-base`: fix `Hash`, `Context`, `Partition` and `Address` being unreadable under non-self-describing formats such
  as `bitcode`, which made any record carrying one fail to decode; a truncated address is also refused rather than
  becoming the zero address
- `lore-server`: pushing a revision hash that does not exist in the immutable store returns `NotFound` instead of a
  generic `Internal` status
- `lore-server`: `LORE_SERVER_MAINTENANCE=1` stops the internal QUIC server serving its port while the internal gRPC
  server keeps a stub listening, so health checks get `unavailable` rather than a closed port
- `lore-telemetry`: `size_histogram` gains explicit power-of-two boundaries from 64 B to the 256 KiB
  `FRAGMENT_SIZE_THRESHOLD`, so `put` and `get` distributions are no longer cut off at the OpenTelemetry defaults
- `lore-error-set`: new `chain_err_from` chains a discrete error onto an error-set enum without destructuring a
  `Traced<E>`; call sites that discarded the originating trace now preserve it
- `lore-revision`: `LoreRevisionDiffFileEventData` gains `from_path`, so a receiver of `LORE_EVENT_REVISION_DIFF_FILE`
  can reconstruct where a moved or copied file came from
- `lore`: the batch verbs' id fields are named apart — `entry_id` per entry, `batch_id` on the batch args and
  `BatchComplete` — and `parent_entry` becomes `parent_entry_index`
- SWFS groundwork: the stale commented-out integration is replaced by a real interface whose types are always available
  while its methods compile only under the `swfs` feature; `InstanceOperationImpl` lets an operation in a linked or
  layered repository be finalized alongside the main one
- New `iteration` Cargo profile inherits `release` with LTO off, cutting link time when rebuilding frequently
- `lore-revision`: reject directory traversal and dot-prefixed segments in repository path components
- `lore-aws`: guard against maliciously large S3 payloads
- `lore-storage`: fix data loss in the lazy fan-out redistribute path, where buckets left unmarked after a fan-out could
  be overwritten from the stale on-disk layout by a reader racing the flush
- `lore-storage`: decide the legacy bucket layout per group rather than per store, so one written group no longer pins
  every other group at the maximum bucket count on the next open
- `lore-storage`: file modification detection no longer downloads content — `file_matches` transfers the stored header
  and, when fragmented, its fragment lists, then compares chunks against the file's own bytes
- `lore-storage`: fix a dropped consumer on a streaming read being reported as an error instead of draining gracefully
- `lore-storage`: build zstd compression and decompression contexts in workspaces this crate owns, with the pool holding
  at most 32 and further concurrency building one per call
- `lore-storage`: remove `allow_partial_fragment` from the local store, so a `lore-server` local store can cache
  `get_metadata` results
- `lore-storage`: obliterate a fragment tree without holding a lock across it, and drop an unreachable sink path from
  the defragment pipeline dispatch
- `lore-storage`: key file modification times by the lowercase path string, unifying them with node-name matching
- `lore-server`: `ReplicatedImmutableStore` implements the `Copy` message and returns the `Context` a query matched
  under, instead of leaving clients to assume the default
- `lore-transport`: a persistent connection updates the authn and authz tokens it presents, and authz exchange results
  for caller-supplied identity tokens are no longer written to the token store
- `lore-auth`: a missing token reports `NotAuthenticated` instead of a generic internal error
- Fix clone not applying view filtering inside linked and layered repositories, where `clone_node` was called with a
  path relative to the linked repository rather than the mount point
- `lore-revision`: refuse a `link remove` that would destroy local edits
- `lore-revision`: fix non-ASCII lowercase handling when building relative paths
- Fix the originating trace being lost through `::internal` and `::internal_with_context`
- `lore-revision`: staging does less work per path — targets resolve concurrently, each walks from its pre-created
  ancestor rather than the root, a directory's children are read once and matched by binary search, a shared directory's
  case resolves once, and paths are neither re-stat'd nor rebuilt through extra string allocations
- `lore-revision`, `lore-storage`, `lore-base`: allocation and locking trimmed on the hot paths — the dirty tree walks
  from an explicit stack and names into one buffer, node blocks serialize from the lock without a copy and deserialize
  once rather than once per waiter, merkle tree blocks come from their own heap, short ASCII names fold to lowercase on
  the stack, the log level reads from an atomic, and successful operations no longer allocate error strings
- `lore-transport`, `lore-revision`: reuse the lazy `StorageSession` wrapper across fragment writes instead of
  rebuilding it per write
- `lore-io`, `lore-revision`: verify a path's case by asking the filesystem for the name instead of listing its
  directory
- `lore-aws`: box SDK error payloads to shrink the `lore-aws` error types

## v0.8.6 (Jul 29th 2026)

### Breaking changes

- `lore-client`: logging CLI arguments are now strictly mutually exclusive; a command line passing conflicting logging
  flags is rejected instead of silently accepted
- Auth `login`/`info` now return `NotSupported` (code 18) when the server has no auth endpoint configured, and surface
  `NotAuthenticated` / `Disconnected` instead of a generic internal error (code -1); scripts branching on these exit/FFI
  codes must be updated
- Presigned URL vending (`POST /v1/repository/{id}/content/{address}/presign`) is now restricted to service accounts;
  normal user tokens can no longer mint presigned URLs

### Features

- `lore-server`: forward `BranchList` and `RepositoryCreate` to a remote Lore server when configured (extending the
  existing `BranchCreate`/`BranchDelete`/`BranchGet` forwarding)
- `lore`: add a batch node-add verb (`lore_revision_tree_add`) to the low-level revision API, landing a whole subtree
  atomically
- `lore`: run the service process (`lore service run`) on Linux and macOS, not just Windows
- `lore`: validate that all strings passed to the C API are valid UTF-8, rejecting invalid input up front with a named
  field
- `lore-server`: support file:// JWKS endpoints in the JWK service
- `lore-revision`: optional `durable_delay` on the composite store so read replicas can answer before the durable tier
  is queried
- `lore-storage`: self-heal corrupt (torn-write / zero-filled) mutable store buckets instead of failing to open the
  store
- `lore-server`: add `Cache-Control` headers to the presigned-URL redeem endpoint for immutable content
- Add a self-contained Terraform example for an AWS primary + edge deployment under `contrib/aws/`

### Fixes & Improvements

- `lore-storage`: `read_into` now respects the requested byte range for single-fragment reads (partial reads of files ≤
  256 KiB no longer fail)
- `lore-server`: fix JWK refresh cache check so a missing/rotated key triggers a fetch, allowing key rotation without a
  restart
- `lore-credential`: require a DNS label boundary when matching a dotless JWT `aud`, and allow exact apex-domain
  matches, closing an audience-suffix leak
- `lore-revision`: preserve dirty move status through `status --scan` instead of degrading it to delete + add
- `lore-revision`: bound commit read memory with a travelling fragment permit, and cap directory recursion fan-out
  during commit
- `lore-revision`: drain staging tasks on all error paths so failures propagate cleanly
- `lore-storage`: batch FastCDC chunking to cut per-chunk overhead on medium/large writes
- Add transport connect timeouts so local reads no longer stall on an unreachable remote
- `lore`: use the calling process's working directory for absolute-path resolution in the service process
- Fix `lore_branch_switch` and `lore_branch_reset` to restore the branch name→id mapping (so the branch appears in the
  local list), with a `--force` override
- `lore-revision`: fix `restore` failing with `Dirty node remain after nodes were committed`
- `lore-base`: update vendored `rpmalloc` to 2.0.1

## v0.8.5 (Jul 15th 2026)

### Features

- Implement the low-level revision-tree read verbs on the C API: `tree_load`/`tree_close`, `resolve_path`,
  `list_children`, revision & node `info`, and `node_path`
- Forward `BranchCreate`, `BranchDelete`, and `BranchGet` to a remote Lore server, opt-in per-RPC under
  `[server.grpc_public_services.forwarded_requests]`
- Add `repository info --local` to read repository metadata from the local store without contacting the remote

### Fixes & Improvements

- Fix a corrupt-tree race in concurrent `node_add` where a half-initialized node could be observed on the child chain
- Protect non-durable local-only fragments from being orphaned by store GC eviction/compaction
- Fix dirty-add reclassifying remaining files when committing multiple added files individually
- Parallelize staging of multiple explicit paths instead of a serialized per-path loop
- Make CLI paths relative to the current working directory across all path-printing commands
- Bump `anyhow`, `crossbeam`, and `memmap2` for security advisories

## v0.8.4 (Jun 25th 2026)

### Features

- Add `--dry-run` to `revision commit` and `lock acquire`/`release`
- Run incremental store GC by default; replace `--gc` with `--no-gc` to disable
- Expose the mutable store through the low-level storage C API
- Add `ForwardedRevisionService` gRPC endpoint to forward `BranchCreate` to a remote server
- Carry structured error detail and FFI codes on the `Complete` event
- Show staged renames as moves in diff output
- `lore status` prints paths relative to the current working directory

### Fixes & Improvements

- Fix use-after-free in `write_fragmented` when the chunker future is cancelled
- Fix `LoreArray<T>` dealloc layout mismatch in Drop
- Fix `commit --stats` panic and report real fragment stats
- Fix link contents surfacing as parent adds in file diff
- Fix clone not materializing view-filtered directories with all-excluded children
- Reject malformed `metadata set` args instead of panicking; default branch metadata to current branch
- Reset staged add/remove/update for link nodes
- Propagate dirty to committed ancestor directories on dirty add
- Make dirty add idempotent so a repeat dirty doesn't duplicate the node
- Stage empty dirty-added directories by their own path
- Honor `--dry-run` on branch push
- Rename `[server.replication]` config to `[server.grpc_internal]` (not backwards-compatible)
- Map GRPC storage errors correctly and stop classifying `Unknown`/`EOF` errors as server errors
- Handle thin-client `RevisionDiff`/`RevisionTree` RPCs with a zeroed revision
- Tag linked-repo `RevisionDiff` changes with an indexed partition table
- Standardise HTTP tracing and log levels
- Seed QUIC clients with an initial CWND on regeneration
- Remove redundant `dry_run` field from lock events
- Rename shared store config file to `shared_store.toml` with auto-migration
- Change `stats` flag to `u8` for C ABI consistency
