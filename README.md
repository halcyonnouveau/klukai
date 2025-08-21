# Klukai Corrosion

Klukai Corrosion is a fork of [Corrosion](https://github.com/superfly/corrosion) that aims to support compatibility with embedding Corrosion into other Rust crates (the raison d'être being [Makiatto](https://github.com/halcyonnouveau/makiatto)).

## Notable Changes

- Builds in latest Rust nightly.
- Updated dependencies.
- Removed Consul support.
- Consolidated crates and published to crates.io.

## Upstream Synchronisation

Klukai will incorporate updates from the upstream Corrosion repository to benefit from bug fixes and improvements. We aim to sync as soon as possible after upstream releases, though timing may vary depending on the complexity of merging changes with our modifications.

This fork exists primarily to maintain compatibility with modern Rust toolchains and dependencies. If upstream Corrosion adopts these changes (updating dependencies and maintaining nightly compatibility), Klukai will be archived and users should migrate back to the official crate.
