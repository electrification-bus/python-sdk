# Contributing to ebus-sdk

Thanks for your interest in contributing! `ebus-sdk` is the Python SDK for the [Electrification Bus (eBus)](https://ebus.energy) integration framework, which adopts and supports the [Homie Convention](https://homieiot.github.io). The SDK provides device-role and controller-role implementations, property abstractions, and an MQTT transport layer (delegated to [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client)).

## How to contribute

### Discussions

Use [Discussions](https://github.com/electrification-bus/python-sdk/discussions) for:

- Open-ended questions about the SDK's design, API shape, or intent ("how should I model X as a Homie node?")
- Integration questions ("I'm trying to use this with broker Y / device Z, what's the recommended pattern?")
- Proposed new abstractions, callback shapes, or convenience helpers — worth aligning on the API before writing the code
- Questions about the relationship between the SDK and the [Electrification Bus specification](https://github.com/electrification-bus/specification) (the SDK aims to be a faithful implementation of the spec; spec-level questions belong in the spec repo's Discussions)
- Thinking out loud about a proposed change before scoping it

Discussions are open-ended — a good place to align on direction before something becomes a concrete change. Aligned outcomes often turn into one or more Issues or pull requests.

### Issues

Use [Issues](https://github.com/electrification-bus/python-sdk/issues) for actionable changes:

- Bug reports with reproduction steps (broker, paho version, code snippet)
- Spec-conformance gaps where the SDK diverges from the [Electrification Bus specification](https://github.com/electrification-bus/specification) (note which spec document and section)
- Concrete feature requests with a clear scope and a use case
- Documentation gaps where a specific README, example, or docstring change is intended
- Discussion outcomes that have alignment and a clear scope

If you're not sure whether something is an Issue or a Discussion, start with a Discussion — we can convert it later.

### Pull requests

Pull requests are welcome.

- For small fixes (typos, docstring tweaks, version bumps, low-risk bug fixes with a test), open a PR directly.
- For substantive changes (new public API surface, changes to existing API shapes, new dependencies, changes that alter device-lifecycle / discovery / property semantics), open a Discussion or Issue first so we can align on scope before you invest the effort.
- **Spec conformance is the north star.** The SDK exists to implement the [Electrification Bus specification](https://github.com/electrification-bus/specification). When a PR's behavior is normative (device states, property contracts, topic structure), point to the spec section it implements. If the spec is ambiguous or wrong, file an Issue against the spec repo first and reference it from the PR here.
- **MQTT-only changes belong elsewhere.** Pure transport concerns (TLS, mTLS, paho upgrades, reconnection tuning, broker auth) belong in [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client), not here. This SDK is the Homie/eBus layer on top — keep that boundary clean.
- **Lint before sending.** The repo enforces [ruff](https://github.com/astral-sh/ruff) via the [`lint`](.github/workflows/lint.yml) workflow — run `ruff check` and `ruff format` locally before pushing. CI will catch what you miss, but green-first is friendlier.
- **Tests are required.** New behavior needs a test (`pytest tests/`); new bug fixes need a regression test. Match the existing pattern (mocked paho via `ebus-mqtt-client`'s test scaffolding) unless the change genuinely requires a real broker — in which case open a Discussion first.
- **Keep comments to a minimum.** The project style is to write self-explanatory code and reserve comments for non-obvious *why* (a spec quirk, a Homie nuance, a workaround for a specific paho behavior). Don't add comments that just restate the code.
- **The version lives in one place.** When a release-worthy change lands, bump `__version__` in `src/ebus_sdk/__init__.py`; that is the single source of truth. `pyproject.toml` reads it dynamically (`dynamic = ["version"]` plus `[tool.setuptools.dynamic]`) and the `setup.py` shim reads the same literal by regex, so neither file carries a `version` value to edit (the shim exists so legacy `setuptools<61`, pinned in Yocto kirkstone, can build a wheel with correct metadata; the docstring at the top of `setup.py` explains this).
- One commit per logical change is fine; we don't require squash or any particular branch naming.

## Releases

Releases to PyPI are automated via the [`Publish to PyPI`](.github/workflows/publish.yml) GitHub Actions workflow, which runs on `v*` git tags using PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/). Contributors don't need to do anything special: once a maintainer tags `vX.Y.Z`, the workflow tests, publishes to PyPI, and creates a GitHub Release using that version's `CHANGELOG.md` section as the notes.

## Code of conduct

Be respectful and constructive. We appreciate everyone who takes the time to file an issue, start a discussion, or send a pull request.

## Maintenance posture

`ebus-sdk` is an active alpha library. Updates and maintenance, including responses to issues filed on GitHub, will take place on an "as time and resources permit" basis. The SDK is maintained alongside [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client) and the [Electrification Bus specification](https://github.com/electrification-bus/specification) — see the specification repo's README §Governance for the project's long-term governance context.
