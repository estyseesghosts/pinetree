# 2026-10-15 — Phase 2.5 Slice 8: Visibility decision (docs only, no code change)

- Decision: KEEP Misskey's remote-remote followers fallback as specified in
  `docs/visibility-policy.md` §3 (`followers` bullet): when neither side's follow
  graph is knowable (both note and viewer remote), assume visible, per M
  `NoteEntityService.ts:316-323`, so federated followers-timeline traffic doesn't
  vanish. No code or policy change.
- Unreachable-in-production evidence (this tree, pre-Slice-7): `ViewerContext`
  (`src/domain/visibility.rs:11-50`) has three constructors; production call
  sites build only `anonymous()` (`queries.rs:17`, `packers.rs:365`,
  `actor.rs:571`, `objects.rs:1164,1188`) and `local_user(...)`
  (`queries.rs:24,109`, `packers.rs:375`, `commands/notes.rs:211`).
  `remote_user(...)` is constructed ONLY in `domain/policy.rs` tests (:325, :369)
  plus its own definition (`visibility.rs:41`). Hence the fallback branch
  (`visibility.rs:94`) cannot trigger outside tests today.
- Revisit flag: Phase 3 streaming (or any future remote-viewer/authenticated-
  remote rendering path) that constructs remote viewers in production MUST
  re-audit this fallback — it will become live behavior the moment a remote
  viewer reaches `is_visible`. Conformance cell `e2_followers_remote_remote_fallback`
  pins the current semantics until then.
