"""Tests for ebus_sdk.property (ObservableProperty, PropertyDict, GroupedPropertyDict)."""

import threading
from unittest.mock import MagicMock


from ebus_sdk.property import (
    Property,
    PropertyDict,
    GroupedPropertyDict,
    ChangeEvent,
)


# ── Property ─────────────────────────────────────────────────────────────


class TestProperty:
    def test_init_basic(self):
        p = Property(id="temperature", value=72.5, type=float)
        assert p.id() == "temperature"
        assert p.value() == 72.5
        assert p.type() == float

    def test_init_from_dict(self):
        p = Property(from_dict={"id": "status", "value": "online", "type": str})
        assert p.id() == "status"
        assert p.value() == "online"

    def test_set_value_returns_new(self):
        p = Property(id="temp", value=70)
        result = p.set_value(75)
        assert result == 75
        assert p.value() == 75

    def test_on_change_callback_fires(self):
        p = Property(id="temp", value=70)
        cb = MagicMock()
        p.add_on_change_callback(cb)

        p.set_value(75)
        cb.assert_called_once_with(p)

    def test_on_change_callback_not_fired_same_value(self):
        p = Property(id="temp", value=70)
        cb = MagicMock()
        p.add_on_change_callback(cb)

        p.set_value(70)
        cb.assert_not_called()

    def test_on_set_callback_fires_always(self):
        p = Property(id="temp", value=70)
        cb = MagicMock()
        p.add_on_set_callback(cb)

        p.set_value(70)  # Same value
        cb.assert_called_once_with(p)

    def test_remove_callback(self):
        p = Property(id="temp", value=70)
        cb = MagicMock()
        cb_id = p.add_on_change_callback(cb)

        result = p.remove_callback(cb_id)
        assert result is True

        p.set_value(75)
        cb.assert_not_called()

    def test_remove_nonexistent_callback(self):
        import uuid

        p = Property(id="temp", value=70)
        result = p.remove_callback(uuid.uuid4())
        assert result is None

    def test_entity_setter(self):
        setter = MagicMock()
        p = Property(id="mode", value="auto", entity_setter=setter)

        p.set_entity("manual")
        setter.assert_called_once_with("manual")

    def test_entity_setter_not_set(self):
        p = Property(id="mode", value="auto")
        # Should not raise
        p.set_entity("manual")

    def test_set_entity_setter(self):
        p = Property(id="mode", value="auto")
        setter = MagicMock()
        p.set_entity_setter(setter)

        p.set_entity("manual")
        setter.assert_called_once_with("manual")

    def test_format(self):
        p = Property(id="mode", value="auto", format="auto,manual,off")
        assert p.format() == "auto,manual,off"


# ── PropertyDict ─────────────────────────────────────────────────────────


class TestPropertyDict:
    def test_add_and_get(self):
        pd = PropertyDict()
        p = Property(id="temp", value=72)
        pd.add_property(p)
        assert pd.get("temp") is p

    def test_get_missing(self):
        pd = PropertyDict()
        assert pd.get("missing") is None

    def test_has_property(self):
        pd = PropertyDict()
        pd.add_property(Property(id="temp", value=72))
        assert pd.has_property("temp") is True
        assert pd.has_property("missing") is False

    def test_add_property_from_dict(self):
        pd = PropertyDict()
        pd.add_property_from_dict({"id": "temp", "value": 72, "type": float})
        assert pd.value("temp") == 72

    def test_delete_property(self):
        pd = PropertyDict()
        pd.add_property(Property(id="temp", value=72))
        assert pd.delete_property("temp") is True
        assert pd.get("temp") is None

    def test_delete_property_missing(self):
        pd = PropertyDict()
        assert pd.delete_property("missing") is False

    def test_value(self):
        pd = PropertyDict()
        pd.add_property(Property(id="temp", value=72))
        assert pd.value("temp") == 72

    def test_set_value(self):
        pd = PropertyDict()
        pd.add_property(Property(id="temp", value=72))
        result = pd.set_value("temp", 75)
        assert result == 75
        assert pd.value("temp") == 75

    def test_set_value_missing(self):
        pd = PropertyDict()
        assert pd.set_value("missing", 75) is None

    def test_items(self):
        pd = PropertyDict()
        pd.add_property(Property(id="a", value=1))
        pd.add_property(Property(id="b", value=2))
        items = dict(pd.items())
        assert set(items.keys()) == {"a", "b"}

    def test_ids(self):
        pd = PropertyDict()
        pd.add_property(Property(id="a", value=1))
        pd.add_property(Property(id="b", value=2))
        assert set(pd.ids()) == {"a", "b"}


# ── GroupedPropertyDict ──────────────────────────────────────────────────


class TestGroupedPropertyDict:
    def test_add_property_auto_creates_group(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))
        assert gpd.value("sensors", "temp") == 72

    def test_create_and_delete_group(self):
        gpd = GroupedPropertyDict()
        gpd.create_group("sensors")
        assert gpd.group_exists("sensors") is True
        gpd.delete_group("sensors")
        assert gpd.group_exists("sensors") is False

    def test_set_value(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))
        gpd.set_value("sensors", "temp", 80)
        assert gpd.value("sensors", "temp") == 80

    def test_value_missing_group(self):
        gpd = GroupedPropertyDict()
        assert gpd.value("missing", "temp") is None

    def test_value_missing_property(self):
        gpd = GroupedPropertyDict()
        gpd.create_group("sensors")
        assert gpd.value("sensors", "missing") is None

    def test_get(self):
        gpd = GroupedPropertyDict()
        p = Property(id="temp", value=72)
        gpd.add_property("sensors", p)
        assert gpd.get("sensors", "temp") is p

    def test_groups(self):
        gpd = GroupedPropertyDict()
        gpd.create_group("a")
        gpd.create_group("b")
        assert set(gpd.groups()) == {"a", "b"}

    def test_delete_property(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))
        gpd.delete_property("sensors", "temp")
        assert gpd.get("sensors", "temp") is None

    def test_observer_fires_on_property_change(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))

        events = []
        gpd.add_observer(lambda event_type, **kw: events.append((event_type, kw)))

        gpd.set_value("sensors", "temp", 80)
        assert any(e[0] == ChangeEvent.PROPERTY_CHANGED for e in events)

    def test_observer_remove(self):
        gpd = GroupedPropertyDict()
        cb = MagicMock()
        obs_id = gpd.add_observer(cb)
        assert gpd.remove_observer(obs_id) is True
        assert gpd.remove_observer(obs_id) is False

    def test_bulk_update(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))
        gpd.add_property("sensors", Property(id="humidity", value=50))

        events = []
        gpd.add_observer(lambda event_type, **kw: events.append(event_type))

        with gpd.bulk_update():
            gpd.set_value("sensors", "temp", 80)
            gpd.set_value("sensors", "humidity", 60)

        # Should get a single BULK_UPDATE, not individual PROPERTY_CHANGED
        assert ChangeEvent.BULK_UPDATE in events

    def test_concurrent_bulk_updates_do_not_fragment_each_others_batches(self):
        """GH #55: the active bulk context is per-thread.

        Sharing one _bulk_mode/_bulk_context pair meant a second thread entering a
        context displaced the first, and whichever exited first ended bulk mode for
        both. The survivor's remaining changes then escaped as individual events. For
        a Homie publisher that is extra $description republishes and $state flapping.
        """
        gpd = GroupedPropertyDict()
        gpd.add_property("a", Property(id="x", value=0))
        gpd.add_property("b", Property(id="x", value=0))

        events = []
        events_lock = threading.Lock()

        def observe(event_type, **kw):
            with events_lock:
                events.append((event_type, kw.get("changes")))

        gpd.add_observer(observe)

        b_entered = threading.Event()
        b_exited = threading.Event()

        def thread_a():
            with gpd.bulk_update():
                gpd.set_value("a", "x", 1)
                b_entered.wait(timeout=5)  # let B open its context inside ours...
                b_exited.wait(timeout=5)  # ...and close it, before we make our 2nd change
                gpd.set_value("a", "x", 2)

        def thread_b():
            with gpd.bulk_update():
                gpd.set_value("b", "x", 1)
                b_entered.set()
            b_exited.set()

        ta = threading.Thread(target=thread_a, daemon=True)
        tb = threading.Thread(target=thread_b, daemon=True)
        ta.start()
        tb.start()
        ta.join(timeout=10)
        tb.join(timeout=10)
        assert not ta.is_alive() and not tb.is_alive()

        bulk = [changes for et, changes in events if et is ChangeEvent.BULK_UPDATE]
        stray = [et for et, _ in events if et is not ChangeEvent.BULK_UPDATE]

        # Exactly two batches, and nothing escaped as an individual event. Unlocked,
        # thread A's second set_value fires on its own because B's exit cleared the
        # shared flag.
        assert stray == [], f"changes escaped their batch: {stray}"
        assert len(bulk) == 2, f"expected one batch per thread, got {len(bulk)}"

        # Each batch holds only its own thread's changes.
        groups_per_batch = [{c.get("group_name") for c in changes} for changes in bulk]
        assert {"a"} in groups_per_batch, groups_per_batch
        assert {"b"} in groups_per_batch, groups_per_batch
        a_batch = next(c for c in bulk if {x.get("group_name") for x in c} == {"a"})
        assert len(a_batch) == 2, f"thread A's batch was fragmented: {a_batch}"

    def test_a_nested_bulk_update_resumes_the_enclosing_batch(self):
        # The context restores the previous one on exit rather than clearing it, so an
        # inner bulk_update() no longer ends the outer batch and leaks the rest of it
        # as individual events.
        gpd = GroupedPropertyDict()
        gpd.add_property("g", Property(id="x", value=0))

        events = []
        gpd.add_observer(lambda event_type, **kw: events.append((event_type, kw.get("changes"))))

        with gpd.bulk_update():
            gpd.set_value("g", "x", 1)
            with gpd.bulk_update():
                gpd.set_value("g", "x", 2)
            gpd.set_value("g", "x", 3)  # still batched: the outer context resumed

        stray = [et for et, _ in events if et is not ChangeEvent.BULK_UPDATE]
        assert stray == [], f"a change escaped the resumed outer batch: {stray}"
        batches = [changes for et, changes in events if et is ChangeEvent.BULK_UPDATE]
        assert [len(b) for b in batches] == [1, 2], batches  # inner flushes first, then outer

    def test_has_group(self):
        gpd = GroupedPropertyDict()
        assert gpd.has_group("missing") is False
        gpd.create_group("sensors")
        assert gpd.has_group("sensors") is True

    def test_get_groups_by_property_value(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("circuit-1", Property(id="type", value="circuit"))
        gpd.add_property("circuit-2", Property(id="type", value="circuit"))
        gpd.add_property("bess-1", Property(id="type", value="bess"))

        result = gpd.get_groups_by_property_value("type", "circuit")
        assert set(result) == {"circuit-1", "circuit-2"}

    def test_add_property_on_change_callback(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))

        cb = MagicMock()
        cb_id = gpd.add_property_on_change_callback("sensors", "temp", cb)
        assert cb_id is not None

        gpd.set_value("sensors", "temp", 80)
        cb.assert_called_once()

    def test_set_entity(self):
        setter = MagicMock()
        gpd = GroupedPropertyDict()
        gpd.add_property("ctrl", Property(id="mode", value="auto", entity_setter=setter))

        gpd.set_entity("ctrl", "mode", "manual")
        setter.assert_called_once_with("manual")

    def test_as_dict(self):
        gpd = GroupedPropertyDict()
        gpd.add_property("sensors", Property(id="temp", value=72))
        d = gpd.as_dict()
        assert "sensors" in d
        assert "temp" in d["sensors"]
