"""Guard `ha.customize._CAPABILITY_META` against drift from the eBus catalogs.

The customizer table restates specification facts: its outer keys are capability
names and its inner keys are property ids. Nothing enforced that, so a key could
stop matching (or never have matched) without anything failing. That is exactly
how GH #27 happened: the table carried a `battery` capability the specification
has never defined, and the SDK's own tests asserted the table against itself by
constructing the invented node type.

This walks the table and checks every name against the machine-readable catalogs
in a sibling `specification` checkout. Two caveats worth knowing:

- It reads the specification's HEAD, not the `synced_commit` pinned in
  `.ebus-spec.json`. It has to: the `capabilities/*.json` catalogs postdate that
  pin. So a rename upstream fails here on a developer machine with a fresh spec
  checkout while CI stays green, which is the intended tripwire but also an
  argument for re-syncing the lockfile promptly.
- It is SKIPPED when no specification checkout is present, which is the case in
  CI today (no workflow checks one out, and `publish.yml` runs pytest as a
  release gate, so this must never be able to redden that). Point it somewhere
  explicit with `EBUS_SPEC_DIR` if the sibling layout does not apply.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from ebus_sdk.ha import customize

_SPEC_DIR = Path(os.environ.get("EBUS_SPEC_DIR") or Path(__file__).resolve().parents[2] / "specification")
_CATALOGS = _SPEC_DIR / "capabilities"

pytestmark = pytest.mark.skipif(
    not _CATALOGS.is_dir(),
    reason=f"no eBus specification checkout at {_SPEC_DIR} (set EBUS_SPEC_DIR to point at one)",
)

_CAPABILITY_PREFIX = "energy.ebus.capability."


def _expand(pattern: str) -> list[str]:
    """`power-factor-{a,b,c}` -> [power-factor-a, power-factor-b, power-factor-c]."""
    match = re.search(r"\{([^}]*)\}", pattern)
    if not match:
        return [pattern]
    return [pattern[: match.start()] + choice.strip() + pattern[match.end() :] for choice in match.group(1).split(",")]


def _catalog_property_ids(catalog: dict) -> set:
    """Every property id a catalog defines, with its patterns expanded."""
    ids = set(catalog.get("properties", {}))
    for pattern in catalog.get("property_patterns", {}):
        ids.update(_expand(pattern))
    return ids


def _load_catalogs() -> dict:
    """capability short name -> parsed catalog, for every catalog in the spec."""
    catalogs = {}
    for path in sorted(_CATALOGS.glob("*.json")):
        catalog = json.loads(path.read_text())
        name = catalog.get("capability", "")
        if name.startswith(_CAPABILITY_PREFIX):
            catalogs[name[len(_CAPABILITY_PREFIX) :]] = catalog
    return catalogs


def test_capability_meta_matches_the_specification_catalogs():
    catalogs = _load_catalogs()
    assert catalogs, f"no capability catalogs parsed from {_CATALOGS}"

    drift = []
    for capability, properties in customize._CAPABILITY_META.items():
        catalog = catalogs.get(capability)
        if catalog is None:
            drift.append(
                f"_CAPABILITY_META[{capability!r}] is not a capability: no "
                f"{_CAPABILITY_PREFIX}{capability} in {_CATALOGS} "
                f"(known: {', '.join(sorted(catalogs))})"
            )
            continue
        known = _catalog_property_ids(catalog)
        for prop_id in properties:
            if prop_id == "*":  # the whole-capability default, not a property id
                continue
            if prop_id not in known:
                drift.append(
                    f"_CAPABILITY_META[{capability!r}][{prop_id!r}] is not a property of "
                    f"{_CAPABILITY_PREFIX}{capability} v{catalog.get('version')} "
                    f"(has: {', '.join(sorted(known))})"
                )

    assert not drift, "customizer table has drifted from the specification:\n  " + "\n  ".join(drift)


def test_soc_is_the_capability_carrying_state_of_charge():
    # The specific fact GH #27 turned on, asserted against the catalog rather than
    # against the table: a `battery` capability must not reappear, and `soc` must
    # remain the home of state of charge.
    catalogs = _load_catalogs()
    assert "battery" not in catalogs, "the specification now defines a `battery` capability; revisit customize.py"
    assert "soc" in catalogs
    assert "soc" in _catalog_property_ids(catalogs["soc"])
