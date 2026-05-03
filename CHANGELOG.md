# Changelog

## [0.1.13](https://github.com/alltuner/switchyard/compare/v0.1.12...v0.1.13) (2026-05-03)


### Miscellaneous Chores

* **deps:** update googleapis/release-please-action action to v5 ([#36](https://github.com/alltuner/switchyard/issues/36)) ([91ebe29](https://github.com/alltuner/switchyard/commit/91ebe29526773feb367db9d4c1ada88fd27d53dd))


### CI/CD Changes

* allow revert as a conventional PR title type ([#40](https://github.com/alltuner/switchyard/issues/40)) ([603780c](https://github.com/alltuner/switchyard/commit/603780c596399d7bcda3f83f845b4c7c3f1ba3f2))
* validate PR titles as conventional commits ([#38](https://github.com/alltuner/switchyard/issues/38)) ([f27a69e](https://github.com/alltuner/switchyard/commit/f27a69ec08ada9a688e325981bfff51256a38328))

## [0.1.12](https://github.com/alltuner/switchyard/compare/v0.1.11...v0.1.12) (2026-04-07)


### Bug Fixes

* use atomic writes for sync queue markers to prevent race corruption ([#34](https://github.com/alltuner/switchyard/issues/34)) ([867a0d1](https://github.com/alltuner/switchyard/commit/867a0d1174cd10fc96297c57f5359905e4a3e8c6))

## [0.1.11](https://github.com/alltuner/switchyard/compare/v0.1.10...v0.1.11) (2026-04-07)


### Bug Fixes

* don't report upstream-only blobs as locally available in HEAD ([#32](https://github.com/alltuner/switchyard/issues/32)) ([8925dfa](https://github.com/alltuner/switchyard/commit/8925dfab734ff89946e643304f79ac87b8dd6746))

## [0.1.10](https://github.com/alltuner/switchyard/compare/v0.1.9...v0.1.10) (2026-04-07)


### Features

* retry stuck sync markers immediately when missing blobs arrive ([#30](https://github.com/alltuner/switchyard/issues/30)) ([5f84fc9](https://github.com/alltuner/switchyard/commit/5f84fc984cc79013284c64b01ac8c5ba581f6f1c))


### Bug Fixes

* deduplicate missing blobs and reduce sync error noise ([#28](https://github.com/alltuner/switchyard/issues/28)) ([f53b9f0](https://github.com/alltuner/switchyard/commit/f53b9f0e29b5b3a1d6fce9c3cfa1b9b637dd999d))

## [0.1.9](https://github.com/alltuner/switchyard/compare/v0.1.8...v0.1.9) (2026-04-02)


### Bug Fixes

* abort sync when child manifest blobs are missing locally ([#26](https://github.com/alltuner/switchyard/issues/26)) ([44f5c2d](https://github.com/alltuner/switchyard/commit/44f5c2d18392e9f0a4ccc06f6159176d4da425aa))

## [0.1.8](https://github.com/alltuner/switchyard/compare/v0.1.7...v0.1.8) (2026-04-02)


### Bug Fixes

* push child manifests before image index during upstream sync ([#23](https://github.com/alltuner/switchyard/issues/23)) ([3a26e79](https://github.com/alltuner/switchyard/commit/3a26e794813eb65573df1c1fe8d374ce010f26f6))


### Miscellaneous Chores

* update lockfile ([#25](https://github.com/alltuner/switchyard/issues/25)) ([c5c32f1](https://github.com/alltuner/switchyard/commit/c5c32f136616c947696f3b4cb07cb7a2e494b7a2))

## [0.1.7](https://github.com/alltuner/switchyard/compare/v0.1.6...v0.1.7) (2026-04-02)


### Bug Fixes

* preserve Location query params when completing blob uploads ([#21](https://github.com/alltuner/switchyard/issues/21)) ([98c486f](https://github.com/alltuner/switchyard/commit/98c486fd4ec917b7f56589cc0153d67cfecd317f))

## [0.1.6](https://github.com/alltuner/switchyard/compare/v0.1.5...v0.1.6) (2026-04-02)


### Bug Fixes

* replace python-dxf with direct httpx for correct Content-Type on manifest push ([#19](https://github.com/alltuner/switchyard/issues/19)) ([9a9d358](https://github.com/alltuner/switchyard/commit/9a9d35846d0be6023caa37852926f3eb85a37c61))

## [0.1.5](https://github.com/alltuner/switchyard/compare/v0.1.4...v0.1.5) (2026-04-02)


### Bug Fixes

* fix upstream sync and adopt python-dxf ([#17](https://github.com/alltuner/switchyard/issues/17)) ([8b23ab7](https://github.com/alltuner/switchyard/commit/8b23ab7160c47d8f20c92818a752e7813a1567ba))

## [0.1.4](https://github.com/alltuner/switchyard/compare/v0.1.3...v0.1.4) (2026-04-02)


### Miscellaneous Chores

* **deps:** update docker/setup-buildx-action action to v4 ([#14](https://github.com/alltuner/switchyard/issues/14)) ([eda3721](https://github.com/alltuner/switchyard/commit/eda3721e641906c93065b1bba9b42c1b120c237f))
* **deps:** update docker/setup-qemu-action action to v4 ([#16](https://github.com/alltuner/switchyard/issues/16)) ([4aeb8e0](https://github.com/alltuner/switchyard/commit/4aeb8e04fbafaa1b1460618364062eff0c7dfc3c))


### CI/CD Changes

* build multi-arch Docker images (amd64 + arm64) ([#13](https://github.com/alltuner/switchyard/issues/13)) ([a7dc852](https://github.com/alltuner/switchyard/commit/a7dc852d0a35386894fc2f6383730c8cbece2686))

## [0.1.3](https://github.com/alltuner/switchyard/compare/v0.1.2...v0.1.3) (2026-04-01)


### Miscellaneous Chores

* **deps:** update actions/checkout action to v6 ([#10](https://github.com/alltuner/switchyard/issues/10)) ([2c357a1](https://github.com/alltuner/switchyard/commit/2c357a130a52680c8e8b0f89ca496d62945ce31a))
* **deps:** update docker/login-action action to v4 ([#11](https://github.com/alltuner/switchyard/issues/11)) ([9e5d0c8](https://github.com/alltuner/switchyard/commit/9e5d0c8f34268f50d73078ea2682eed004a6b3aa))

## [0.1.2](https://github.com/alltuner/switchyard/compare/v0.1.1...v0.1.2) (2026-04-01)


### Bug Fixes

* use --frozen in Dockerfile and regenerate lockfile ([#7](https://github.com/alltuner/switchyard/issues/7)) ([4eb3010](https://github.com/alltuner/switchyard/commit/4eb3010481c1c1c293e6d742b4fc19d71aa0f8c3))


### Documentation Updates

* add polished README, MIT license, and GitHub funding ([#9](https://github.com/alltuner/switchyard/issues/9)) ([4f91374](https://github.com/alltuner/switchyard/commit/4f9137444861db020371eeb3def31800ea03739f))

## [0.1.1](https://github.com/alltuner/switchyard/compare/v0.1.0...v0.1.1) (2026-04-01)


### Features

* add release-please and GHCR Docker publish workflow ([b60ec34](https://github.com/alltuner/switchyard/commit/b60ec34bfb242073cbb37d220221bed92023653f))


### Miscellaneous Chores

* **deps:** update actions/checkout action to v6 ([#4](https://github.com/alltuner/switchyard/issues/4)) ([7fd9037](https://github.com/alltuner/switchyard/commit/7fd9037b5c560b4a267202efaa193a429f3a4279))
* **deps:** update docker/build-push-action action to v7 ([#5](https://github.com/alltuner/switchyard/issues/5)) ([e3032cc](https://github.com/alltuner/switchyard/commit/e3032cc55630ee65fbbc2a562efc7db2a4e16c97))
