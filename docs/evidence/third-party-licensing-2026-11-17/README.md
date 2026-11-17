# Third-party licence metadata follow-up — 17 November 2026

The [original source-security inventory](../hosted-reference-2026-11-17/README.md) on licensed source `185c415` listed **126 locked components** and four entries needing review because their installed-distribution licence metadata was absent or the package was not installed on the Linux runner. This follow-up cross-references the exact package versions in `uv.lock` against **upstream version-tagged licence files**, not guesses from package names.

| Locked package | Installed on the checked Linux runner? | Upstream original licence evidence | Bounded conclusion |
|---|---|---|---|
| `huey==3.5.4` | Yes | [`coleifer/huey` tag 3.5.4 LICENSE](https://github.com/coleifer/huey/blob/3.5.4/LICENSE), Git blob `6bd3561c694aa763d0f6527726d524305b4f46a5` | MIT-style permissive grant located upstream; installed distribution metadata omitted the declaration, so automated inventory flagged it |
| `colorama==0.4.6` | No | [`tartley/colorama` tag 0.4.6 LICENSE.txt](https://github.com/tartley/colorama/blob/0.4.6/LICENSE.txt), blob `3105888ec149d10cad51c11d332779e94b548661` | BSD-style three-clause attribution/redistribution conditions located; optional/platform dependency absent from checked runner |
| `waitress==3.0.2` | No | [`Pylons/waitress` tag v3.0.2 LICENSE.txt](https://github.com/Pylons/waitress/blob/v3.0.2/LICENSE.txt), blob `e1f9ad7b3b48377bfbfaa1b380c384ddea24060f` | Zope Public License 2.1 located upstream; package absent from checked runner |
| `pywin32==312` | No | [`win32/License.txt`](https://github.com/mhammond/pywin32/blob/b312/win32/License.txt), blob `8971178abbb3dbef40e1f723476127d3bd86b1e5`, and [`com/License.txt`](https://github.com/mhammond/pywin32/blob/b312/com/License.txt), blob `d8f3c9484922bf935c4c4810c971df69bd8674d4` | Several component copyrights/permissions exist; do **not** claim one SPDX license for all bundled Windows components |

The exact source coordinates, blob hashes and installation states are also preserved in [manifest.json](manifest.json). ### Correction of the original repository-archive claim

A new recursive Git tree readback on the exact prior review commit
[`0166edb`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/0166edb790941b6a15f8e2d33cb8ebf14fdff719)
found **68 tracked `.gz` files**, not zero. All 68 are under
`docs/evidence/`: 39 `.jsonl.gz`, 28 `.log.gz`, and one `.csv.gz`.
They occupy **1,294,667 compressed bytes** and resolve to **59 distinct Git blobs**.
The same tree contains **zero filenames ending in** `.jar`, `.whl`,
`.so`, `.dll`, `.exe`, `.zip` or `.tar`.

This fixes the incorrect no-`.gz` statement in the original merged
[licensing PR #17](https://github.com/mehdisafer/industrial-streaming-mlops/pull/17).
Filename classification is not evidence that the compressed files contain
no secrets or third-party software; their original **bytes were not inspected**
by that tree metadata query.

The subsequent [tracked archive inventory](../../../scripts/repository_archive_inventory.py)
performs a bounded gzip decompression and UTF-8/pattern check of each allowed
`docs/evidence/` text fixture, binds checked-out bytes to its Git object ID and
reports the decompressed SHA-256. It refuses unexpected checked-in software
archive suffixes. CircleCI retains its original
`archive-inventory.json` report; passing CI and separate inspection of the
result are needed before claiming the new check succeeded. The policy's regex
coverage does not certify the absence of all secrets, malicious content or
third-party redistribution obligations.

## Redistribution boundary

The repository's approved [Apache-2.0](../../../LICENSE) covers only the original P03 work. It does **not** relicense any upstream project. Preserve each applicable copyright and notice when redistributing copies, binaries, images or distributions as required by their individual license texts. Particular care is required if Windows `pywin32` components, the separately downloaded Spark connector JARs, or built Docker images are packaged for users.

This targeted lookup **resolves the provenance of the four missing/absent metadata flags**; it does not certify all 126 package licenses, third-party notices in built artifacts, image SBOM completeness, or legal compliance. The original [security scan](../hosted-reference-2026-11-17/README.md) reported zero matched secret patterns and no published advisories among 123 audit entries, but that is a different evidence scope. Complete distribution review remains separate from source-only prerelease publication.
