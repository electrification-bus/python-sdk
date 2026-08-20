"""Tests for the exported proxy/adapter helpers (adapter.py)."""

from ebus_sdk import (
    GroupedPropertyDict,
    ObservableProperty,
    bind_property_to_homie,
    set_homie_property_from_python_property,
)


class _FakeHomieProperty:
    """Stand-in for a homie.Property (only set_value is exercised)."""

    def __init__(self):
        self.value = None

    def set_value(self, v):
        self.value = v
        return True


def test_set_homie_property_from_python_property_copies_value():
    py = ObservableProperty(id="p", type=float, value=12.5)
    twin = _FakeHomieProperty()
    assert set_homie_property_from_python_property(twin, py) is True
    assert twin.value == 12.5


def test_bind_property_to_homie_mirrors_on_change():
    model = GroupedPropertyDict()
    model.create_group("meter")
    model.add_property("meter", ObservableProperty(id="active-power", type=float))
    twin = _FakeHomieProperty()

    bind_property_to_homie(model, "meter", "active-power", twin)
    model.set_value("meter", "active-power", 1850.0)
    assert twin.value == 1850.0


def test_bind_property_to_homie_does_not_fire_on_unchanged_value():
    model = GroupedPropertyDict()
    model.create_group("meter")
    model.add_property("meter", ObservableProperty(id="p", type=float, value=100.0))
    twin = _FakeHomieProperty()

    bind_property_to_homie(model, "meter", "p", twin)
    model.set_value("meter", "p", 100.0)  # unchanged -> no event -> twin untouched
    assert twin.value is None
    model.set_value("meter", "p", 200.0)  # changed -> mirrored
    assert twin.value == 200.0


class _FakeRetainedProperty(_FakeHomieProperty):
    """A twin that answers `retained()`, as a real homie.Property does."""

    def __init__(self, retained=True):
        super().__init__()
        self._retained = retained
        self.writes = []

    def retained(self):
        return self._retained

    def set_value(self, v):
        self.writes.append(v)
        return super().set_value(v)


def test_bind_event_property_mirrors_every_set_including_repeats():
    """A non-retained twin binds on-set: the broker stores nothing, so a repeat is a second event."""
    model = GroupedPropertyDict()
    model.create_group("dr")
    model.add_property("dr", ObservableProperty(id="event", type=str))
    twin = _FakeRetainedProperty(retained=False)

    bind_property_to_homie(model, "dr", "event", twin)
    model.set_value("dr", "event", "shed")
    model.set_value("dr", "event", "shed")
    assert twin.writes == ["shed", "shed"]


def test_bind_retained_property_still_drops_an_unchanged_repeat():
    model = GroupedPropertyDict()
    model.create_group("meter")
    model.add_property("meter", ObservableProperty(id="p", type=float))
    twin = _FakeRetainedProperty(retained=True)

    bind_property_to_homie(model, "meter", "p", twin)
    model.set_value("meter", "p", 100.0)
    model.set_value("meter", "p", 100.0)
    assert twin.writes == [100.0]


def test_bind_twin_without_retained_is_treated_as_retained():
    """Backward compatibility: a duck-typed mirror that predates the distinction."""
    model = GroupedPropertyDict()
    model.create_group("meter")
    model.add_property("meter", ObservableProperty(id="p", type=float))
    twin = _FakeHomieProperty()  # no retained()

    bind_property_to_homie(model, "meter", "p", twin)
    model.set_value("meter", "p", 100.0)
    assert twin.value == 100.0
    twin.value = None
    model.set_value("meter", "p", 100.0)  # unchanged -> still swallowed, as before
    assert twin.value is None
