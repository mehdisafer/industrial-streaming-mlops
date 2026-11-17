# Third-party licence metadata follow-up — 17 November 2026

The [original source-security inventory](../hosted-reference-2026-11-17/README.md) on licensed source `185c415` listed **126 locked components** and four entries needing review because their installed-distribution licence metadata was absent or the package was not installed on the Linux runner. This follow-up cross-references the exact package versions in `uv.lock` against **upstream version-tagged licence files**, not guesses from package names.

| Locked package | Installed on the checked Linux runner? | Upstream original licence evidence | Bounded conclusion |
|---|---|---|---|
| `huey==3.5.4` | Yes | [`coleifer/huey` tag 3.5.4 LICENSE](https://github.com/coleifer/huey/blob/3.5.4/LICENSE), Git blob `6bd3561c694aa763d0f6527726d524305b4f46a5` | MIT-style permissive grant located upstream; installed distribution metadata omitted the declaration, so automated inventory flagged it |
| `colorama==0.4.6` | No | [`tartley/colorama` tag 0.4.6 LICENSE.txt](https://github.com/tartley/colorama/blob/0.4.6/LICENSE.txt), blob `3105888ec149d10cad51c11d332779e94b548661` | BSD-style three-clause attribution/redistribution conditions located; optional/platform dependency absent from checked runner |
| `waitress==3.0.2` | No | [`Pylons/waitress` tag v3.0.2 LICENSE.txt](https://github.com/Pylons/waitress/blob/v3.0.2/LICENSE.txt), blob `e1f9ad7b3b48377bfbfaa1b380c384ddea24060f` | Zope Public License 2.1 located upstream; package absent from checked runner |
| `pywin32==312` | No | [`win32/License.txt`](https://github.com/mhammond/pywin32/blob/b312/win32/License.txt), blob `8971178abbb3dbef40e1f723476127d3bd86b1e5`, and [`com/License.txt`](https://github.com/mhammond/pywin32/blob/b312/com/License.txt), blob `d8f3c9484922bf935c4c4810c971df69bd8674d4` | Several component copyrights/permissions exist; do **not** claim one SPDX license for all bundled Windows components |

The exact source coordinates, blob hashes and installation states are also preserved in [manifest.json](manifest.json). A GitHub tree inspection of P03 at `0166edb` found no tracked `.jar`, `.whl`, `.so`, `.dll`, `.exe`, `.zip`, `.tar` or `.gz` artifacts. That observation is scoped to the **repository source tree**, not built distributions, external container image layers, dynamically installed packages or vendor binaries.

## Redistribution boundary

The repository's approved [Apache-2.0](../../../LICENSE) covers only the original P03 work. It does **not** relicense any upstream project. Preserve each applicable copyright and notice when redistributing copies, binaries, images or distributions as required by their individual license texts. Particular care is required if Windows `pywin32` components, the separately downloaded Spark connector JARs, or built Docker images are packaged for users.

This targeted lookup **resolves the provenance of the four missing/absent metadata flags**; it does not certify all 126 package licenses, third-party notices in built artifacts, image SBOM completeness, or legal compliance. The original [security scan](../hosted-reference-2026-11-17/README.md) reported zero matched secret patterns and no published advisories among 123 audit entries, but that is a different evidence scope. Complete distribution review remains separate from source-only prerelease publication.
