# Consuming an eBus / Homie 5 tree

This is the guide for the **controller / subscriber** side: code that discovers devices on a broker and acts on what they publish. If you are on the publishing side, read [`doc/building-a-proxy.md`](building-a-proxy.md) instead.

Read this before you write your own parser. Nearly every integration bug we have seen on the consumer side comes from inferring a timing contract that the producer never offered, and the inference always looks safe until it isn't.

## TL;DR

**A producer SHOULD minimize `$state` and `$description` transitions. A consumer MUST react to every `$state` and `$description` update, unconditionally.**

Those are not two halves of one contract. The first is quality-of-implementation and is best-effort. The second is correctness and is unconditional. Reconcile from the state you currently hold on every update you receive; never wait for one message *because* you saw another.

## Why the asymmetry exists

A producer is licensed to coalesce, defer, suppress, and reorder its publishes in the name of not making every controller on the broker resync. That license is not a courtesy the SDK might withdraw: it is the whole reason `$description` republishes are cheap enough to be safe. The consequence is that **the observable message stream is a producer implementation detail**, so a consumer that learned a pattern from one release's producer breaks on the next.

Optimizations this SDK already grants itself as a producer:

| Optimization | Effect on the wire |
| --- | --- |
| Transaction collapsing (`state_transition()`) | N structural changes become one `init` to `ready` edge, not N |
| Description suppression (content hash) | An unchanged `$description` is not republished at all |
| Deferral inside an open transition | Interim `$description` publishes never reach the broker |
| Cascade ordering (`refresh_tree()`) | A device's own `$state` follows the content it vouches for |

Every one of those changes *when and whether* a message appears. None of them changes what is true. A consumer that reads current state and reconciles is unaffected by all four; a consumer that awaits an expected message is broken by at least three.

## What `$state = ready` actually means

`ready` is a statement about **one device's own self-description**: *my `$description` is current, you may act on it.* That is all.

It is specifically **not**:

- a claim that the devices named in this device's `children` have published anything yet
- a rollup over the subtree
- a barrier after which the tree has stopped changing

The rollup reading is the common one and it cannot be made true by any producer. Children are commissioned and decommissioned out of band, so at any instant a new child may be mid-publish. **There is no moment at which a producer knows it has published "all" its children.** The strongest guarantee any producer can offer is per-transaction: *within this cascade, my announcement follows the content it announces.* That is a statement about a transaction, not about the world.

The one thing a root's state **is** authoritative for is **effective state**. Homie 5 propagates a non-ready root down the tree: when the root is `init`, `disconnected`, `sleeping`, or `lost`, its descendants are effectively that too, because the root is the gateway. Only when the root is `ready` do a child's own reported states stand. `Controller.get_effective_state(device_id)` implements this. Note it keys on the **root**, not on the immediate parent, so a mid-tree device entering a `state_transition()` does not mask its own children.

## What a consumer MUST do

- **React to every `$state` update**, including transitions you did not expect and repeats of a state you already hold.
- **React to every `$description` update**, and re-derive your model from it rather than diffing against assumptions about what changed.
- **Treat a device named in `children` as declared, not present.** Subscribe to it and wait; do not read it.
- **Handle a tree that grows and shrinks after you first considered it complete.** Commissioning is an ongoing event, not a startup phase.
- **Carry a timeout.** A declared child may never appear, because it crashed or its own LWT fired. "Declared" can never be made to mean "present" by any amount of producer-side ordering.

## Failure mode 1: the one-shot barrier

> "Wait for every declared descendant to describe itself, then report ready."

This is the most common defence and it is a real improvement over gating on the root alone. It is still wrong, because it is a **barrier** rather than a **loop**: it runs once, passes, and stops reconciling. Commission circuit #38 a minute later and the consumer never sees it. The failure moves from startup to steady state, which makes it harder to find, not less real.

Correct version: reconcile the declared child set against the subscribed child set on **every** ready edge, forever.

## Failure mode 2: waiting for a `$description` that never arrives

> "On the `init` to `ready` edge, wait for the new `$description`."

The content-hash suppression makes an unchanged `$description` a no-op, but it does **not** suppress the `init` to `ready` edge of an otherwise-empty transition. So a consumer can observe a ready edge with no `$description` following it, ever, and a consumer that awaits the pairing hangs.

Correct version: on the ready edge, act on the `$description` you already hold. The edge means "what you have is now current", not "a new one is coming".

## Failure mode 3: inferring order across retained messages

Publish order does not survive retention. A consumer that connects after the producer receives the retained tree in broker-chosen order, and the delivery order of a retained set on `SUBSCRIBE` is unspecified. Producer-side ordering only constrains what a consumer sees if it was **already subscribed** when the publishes happened.

This matters most in exactly the scenario people reason about first: a broker restart, where a previously-healthy consumer re-reads retained state. No producer-side ordering fix reaches that case.

Correct version: never derive an ordering expectation from the wire at all. Reconcile from current state.

## The shape that works

Reconcile-from-current-state instead of wait-for-event-pairing:

```python
def on_state(device_id, state):
    if state != "ready":
        return
    # Act on the description we ALREADY hold. Do not await a fresh one.
    declared = controller.get_device(device_id).description.get("children", [])
    for child_id in declared:
        if child_id not in subscribed:
            subscribe(child_id)          # its own retained $state/$description
            subscribed.add(child_id)     # will cascade back through on_state
    for child_id in subscribed - set(declared):
        unsubscribe(child_id)            # decommissioned
```

The property that makes this correct is that it is **idempotent and order-independent**. Run it on every ready edge, in any order, as many times as you like, and it converges. It does not care whether the child published before or after its parent, whether the description was suppressed, or whether the messages arrived live or retained.

## What `Controller` already does for you

The SDK's `Controller` implements the above in tree-rooted mode. `_reconcile_descendants` fires on each `init` to `ready` edge and diffs the announced `children` against what is subscribed: added children get full topic subscriptions (their own retained state and description then cascade through the same handler, surfacing grandchildren), removed children are unsubscribed and dropped recursively. It also reconciles on a `$description` update for a device already in `ready`. That is why a `Controller`-based consumer was never exposed to the ordering defect fixed in 0.18.1: it uses `ready` as a trigger to subscribe, not as a barrier to read.

**Known gap:** there is no public "the declared tree is fully described" affordance. `Controller` reconciles correctly but does not expose a tree-complete signal, so a consumer that genuinely needs one (a topology snapshot, a one-shot export) currently has to build it. If that is you, build it as a reconciling predicate evaluated on every update, not as a barrier awaited once.

## Checklist

- [ ] Every `$state` update is handled, including unexpected and repeated states.
- [ ] Every `$description` update re-derives the model rather than assuming a delta.
- [ ] Declared children are subscribed and awaited, never read directly.
- [ ] Reconciliation runs on every ready edge, not once at startup.
- [ ] Nothing in the consumer awaits message B because message A arrived.
- [ ] A declared child that never appears is handled by timeout, not by hanging.
- [ ] Effective state is read via `get_effective_state()` rather than a device's own `$state`.
