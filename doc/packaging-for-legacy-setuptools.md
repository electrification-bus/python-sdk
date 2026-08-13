# Packaging Python modules for legacy setuptools compatibility

## TL;DR

If your downstream consumers may have **setuptools < 61** in their build environment, **do not** rely on `pyproject.toml [project]` (PEP 621) alone for package metadata. PEP 621 support was added in setuptools 61.0 (March 2022). Older toolchains will silently build a wheel named **`UNKNOWN-0.0.0-py3-none-any.whl`** containing zero of your actual code — the failure is far downstream from publish time and looks like cascading dependency errors at install.

**Minimum portable shim**: include a `setup.py` (six lines) and/or `setup.cfg [metadata]` block alongside the modern `pyproject.toml`. Two PyPI-resident sentinel versions: `ebus-sdk 0.1.7` and `ebus-mqtt-client 0.1.4` (this repo's first releases that include the shim).

## Why this matters

PEP 621 (`[project]` table in `pyproject.toml`) is the modern, declarative standard for Python package metadata. Modern setuptools (>= 61) reads it directly. But Yocto and other long-life embedded build systems pin specific setuptools versions for reproducibility:

| Yocto release | Year | setuptools-native pinned at | PEP 621 support? |
|---|---|---|---|
| dunfell | 2020 | 50.x | ❌ |
| kirkstone | 2022 | **59.5.0** | ❌ |
| langdale | 2022 | 65.x | ✅ |
| nanbield | 2023 | 68.x | ✅ |
| scarthgap | 2024 | 69.x | ✅ |

If our consumers include kirkstone (they do — SPAN's panel firmware is on it as of 2026), our published source distributions must remain buildable with setuptools 59.5.0.

## The failure mode (what we saw)

1. Modern developer publishes `ebus-sdk 0.1.5` with only `pyproject.toml [project]` — works fine on the developer's mac (setuptools >= 61) and on `pip install ebus-sdk` consumers (pip auto-installs a modern setuptools per the `[build-system].requires` constraint).
2. Yocto kirkstone consumer recipe (`python3-ebus-sdk_0.1.5.bb`) downloads the sdist from PyPI and builds with kirkstone's bundled `setuptools-native` 59.5.0. The `[build-system].requires = ["setuptools>=61.0"]` constraint in `pyproject.toml` is silently ignored — Yocto uses its bundled setuptools, not pip's runtime-installed setuptools.
3. setuptools 59.5.0 cannot parse `[project]`. Falls back to legacy metadata discovery: looks for `setup.py`, then `setup.cfg [metadata]`. Both are absent. Defaults to `name="UNKNOWN"`, `version="0.0.0"`.
4. Build produces `UNKNOWN-0.0.0-py3-none-any.whl` with empty `UNKNOWN-0.0.0.dist-info/`. **No actual python code is included** because setuptools doesn't know where to find packages (the `[tool.setuptools.packages.find]` table is also unread).
5. Two such broken wheels (one for each of two packages) ship files to identical paths (`/usr/lib/python3.x/site-packages/UNKNOWN-0.0.0.dist-info/{LICENSE,METADATA,WHEEL,RECORD,top_level.txt}`). At install time, dpkg detects the file conflict, lets one package "win" and silently drops the other.
6. apt's dependency resolver, encountering the now-missing package, rolls back through the dependency chain. The error message it reports points at the *last* package in the rollback chain — typically the most-recently-added one, NOT the actual root cause. This produced two months of investigation chasing what looked like multi-arch resolver cascades in oe-core.

See [SPAN G3P-23546 postmortem](https://spanio.atlassian.net/wiki/spaces/SPAN/pages/3892511326) for the full disaster story.

## The fix (six lines of setup.py)

Add a minimal `setup.py` shim at the repo root:

```python
"""Legacy setup.py shim for setuptools < 61 (pre-PEP 621).

Modern setuptools reads all package metadata from pyproject.toml [project]
and ignores the args passed here. The explicit name/version/package_dir/
packages are needed only so older setuptools can build a correct wheel
from the sdist — without this shim the legacy build produces an
UNKNOWN-0.0.0 wheel with no real package content.

Keep name and version in sync with pyproject.toml [project].
"""

from setuptools import setup

setup(
    name="<your-package-name>",
    version="<X.Y.Z>",
    package_dir={"": "src"},
    packages=["<your_import_name>"],
)
```

That's it. The args duplicate what's in `pyproject.toml [project]`, which is the trade-off — but the duplication is small and is enforced at sdist-build time (sdist includes both files, modern consumers ignore setup.py's args, legacy consumers ignore pyproject.toml's `[project]`). For nontrivial packages with namespace packages, you may instead want `packages=find_namespace_packages(where="src")` etc.; the exact form depends on your layout.

If you prefer not to maintain duplicated metadata in two files, you can keep just an empty `setup.py` shim:

```python
from setuptools import setup
setup()
```

… *and* declare the metadata in `setup.cfg [metadata]` instead of `pyproject.toml [project]`. Older setuptools reads `setup.cfg [metadata]` natively. Modern setuptools also reads it and merges with `pyproject.toml`. But this means losing the cleaner PEP 621 syntax in `pyproject.toml`. Pick one.

## How to verify the fix works

1. Build the sdist locally: `python -m build --sdist`.
2. Extract the sdist: `tar xzf dist/<pkg>-X.Y.Z.tar.gz -C /tmp`.
3. Confirm `setup.py` (or `setup.cfg [metadata]`) is present in the extracted tree.
4. Build a wheel using an **old** setuptools to simulate the legacy environment:

   ```bash
   python -m venv /tmp/legacy-env
   /tmp/legacy-env/bin/pip install 'setuptools<61' wheel
   cd /tmp/<pkg>-X.Y.Z
   /tmp/legacy-env/bin/python -m build --wheel --no-isolation
   ```

5. Inspect the produced wheel:

   ```bash
   unzip -l dist/<pkg>-X.Y.Z-py3-none-any.whl
   ```

6. Confirm the wheel name is `<pkg>-X.Y.Z-...` (NOT `UNKNOWN-0.0.0-...`) and contains your actual `.py` files (not just `dist-info/` metadata).

If you have a Yocto consumer, a more authoritative test is to build the package via the bitbake recipe (e.g., on dsw-build-01) and inspect the produced `.deb` with `dpkg-deb -c`.

## Why not just bump the upstream setuptools in Yocto?

Yocto layer policy is to update the entire layer (kirkstone → langdale → …) as a coordinated release, not to individually bump shared dependencies. Bumping `python3-setuptools-native` in kirkstone would ripple through every Python recipe in oe-core. Span's recipes are expected to adapt to the consuming Yocto release's toolchain, not the other way around.

## Cross-references

- [SPAN postmortem G3P-23546](https://spanio.atlassian.net/wiki/spaces/SPAN/pages/3892511326) — the disaster this lesson came from
- [SPAN G3P-23661](https://spanio.atlassian.net/browse/G3P-23661) — the re-integration ticket that found the root cause
- [setuptools 61.0 release notes](https://setuptools.pypa.io/en/latest/history.html#v61-0-0) — when PEP 621 landed
- [PEP 621 — Storing project metadata in pyproject.toml](https://peps.python.org/pep-0621/)
