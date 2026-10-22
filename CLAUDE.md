# kafka-connect-couchbase

NetDocuments fork of the upstream Couchbase Kafka Connect plugin (source + sink). The Documents team runs the **source** side: it streams Couchbase changes to the `nmd-data` Kafka topic during the NMD -> PlatEvo migration. Version `x.y.z.n-NDSNAP` is our line on top of upstream `x.y.z`.

## Build / test

Maven wrapper (3.9.6), Java `release` 11 in `pom.xml`, JDK 11+ required.

```bash
./mvnw -B test       # unit tests (surefire; no Docker or Couchbase needed to my knowledge)
./mvnw -B package    # produces target/couchbase-kafka-connect-couchbase-<version>.zip (the connector archive)
```

**Unverified.** This machine has no working JDK: the Homebrew `openjdk`/`openjdk@11` links under `/Library/Java/JavaVirtualMachines` are dangling and nothing was installed. Both commands come from the README and `pom.xml`, not from a run.

- No linter or formatter is configured in `pom.xml`; follow `.editorconfig`.
TODO(team): CI system and quality gates for this repo (none are defined in-tree).
- The `release` profile (GPG signing, Sonatype staging) and `HOWTO-RELEASE.md` are Couchbase's upstream process. Do not use them.

## Where our code lives

NetDocuments additions are isolated so upstream merges stay clean:

- `src/main/java/com/netdocuments/connect/kafka/` (`handler/source/NDSourceHandler`, `filter/`, `transforms/`)
- `src/main/java/com/amazonaws/kafka/config/providers/` (Secrets Manager config provider, from the MSK provider)
- Tests mirror these packages under `src/test/java`.

Everything under `com/couchbase/` is upstream. Keep edits there minimal and deliberate.

## Gotchas

- `NDSourceHandler` is wired in through connector config keys `couchbase.custom.handler.nd.*` (S3 offload threshold, field extraction, key/type filters, CloudEvents type). Renaming or removing a key silently breaks deployed connector configs, so treat the keys as a public contract.
- Messages over the S3 threshold are uploaded to S3 and a pointer is published instead. Upload retry lives in the S3 upload step, not in field extraction.
- `SecretsManagerConfigProvider` is shared across connector tasks and threads. Build the AWS client once and keep `close()` synchronized; there is a concurrent re-init test guarding this.
- Config docs are generated from Javadoc by an annotation processor in the compiler plugin. Keep Javadoc on config options accurate.
- Docs under `docs/` and the AsciiDoc sources are upstream's; don't hand-edit generated config reference.

## Deploy

TODO(team): how the connector zip is built and published, where the Connect workers load the plugin from, and who rolls it out per environment.
TODO(team): how connector config JSON is managed (the repo has only quickstart examples in `config/`; the real `nmd-data` connector config lives elsewhere).

## Do NOT touch

- The `nmd-data` topic, its connectors, and connector offsets are not changed from code or scripts in this repo. Speed-ups go on the consumer, job API, or tool side.
- `examples/` and `config/` quickstarts are upstream samples, not our deployment config.
- Do not bump the version in `pom.xml` unless shipping a change: any change to the shipped plugin needs a new `-NDSNAP` revision. TODO(team): confirm the bump convention.

## Git

Branch off `main`, PR into `main`.

## Context Chain

- **Workspace** — `../documents-team/CLAUDE.md` (sibling clone of `NetDocuments/documents-team`):
  the repository inventory, cross-repo architecture, and a pointer to the local runbook.
- **Product** — `platform/documents/CLAUDE.md` in `NetDocuments/rd-specs`: what this
  product is for. Not loaded automatically; read it when you need the why
  (`gh api repos/NetDocuments/rd-specs/contents/platform/documents/CLAUDE.md -H "Accept: application/vnd.github.raw"`).
