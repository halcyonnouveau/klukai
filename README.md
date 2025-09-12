# Klukai Corrosion

Klukai Corrosion is a fork of [Corrosion](https://github.com/superfly/corrosion) that aims to support compatibility with embedding Corrosion into other Rust crates (the raison d'être being [Makiatto](https://github.com/halcyonnouveau/makiatto)).

## Notable Changes

- Updated dependencies.
- Consolidated crates and published to [crates.io](https://crates.io/crates/klukai).
- Removed PostgreSQL wire protocol API due to upgrade difficulties and not being useful for Makiatto (will possibly re-add in the future).

## Upstream Compatibility

### API Compatibility

Klukai aims to maintain ~100% API compatibility with upstream Corrosion. The binary remains as `corrosion` and supports all the same command-line options and configuration settings as the original. For embedding Corrosion as a library, migration only requires changing import names in your code.

### Upstream Synchronisation

Klukai will incorporate updates from the upstream Corrosion repository to benefit from bug fixes and improvements. We aim to sync as soon as possible after upstream releases, though timing may vary depending on the complexity of merging changes with our modifications.

This fork exists primarily to maintain compatibility with updated Rust toolchains and dependencies. If upstream Corrosion adopts these changes (updating dependencies and publishing crates), Klukai will be archived and users should migrate back to the original crate.
