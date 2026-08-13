"""Adapter helpers for building eBus proxies and adapters.

These bridge the observable application model (``property.py``: a
``GroupedPropertyDict`` of observable ``Property`` objects) to the Homie device
tree (``homie.py``: ``Device`` / ``Node`` / ``Property``). This is the
recommended pattern for any publisher whose device state changes over time (a
proxy for a non-eBus device, an adapter for a local device, a gateway/bridge):
keep live state in the observable model, and mirror each change onto Homie via a
per-property on-change callback. Acquisition code updates the model; publishing
is a reactive side-effect.

See ``doc/building-a-proxy.md`` for the full pattern and a worked example.
"""

from functools import partial

from .homie import Property as HomieProperty
from .property import GroupedPropertyDict
from .property import Property as ObservableProperty


def set_homie_property_from_python_property(homie_property: HomieProperty, python_property: ObservableProperty) -> bool:
    """Copy an observable ``Property``'s current value onto its Homie twin.

    This is the on-change adapter that mirrors the observable model to Homie.
    Register it as the ``GroupedPropertyDict`` on-change callback for a
    ``(group, property_id)`` pair so every value change reaches MQTT (subject to
    the Homie layer's own publish-on-change gate, which drops a republish whose
    wire payload is unchanged -- see ``homie.Property.set_value``)::

        properties.add_property_on_change_callback(
            group,
            property_id,
            partial(set_homie_property_from_python_property, homie_property),
        )

    Prefer :func:`bind_property_to_homie`, which wraps that registration.
    """
    return homie_property.set_value(python_property.value())


def bind_property_to_homie(
    properties: GroupedPropertyDict, group: str, property_id: str, homie_property: HomieProperty
):
    """Wire an observable model property to its Homie twin so changes mirror across.

    Convenience wrapper over
    :func:`set_homie_property_from_python_property`: registers the on-change
    callback that republishes ``properties[group][property_id]`` onto
    ``homie_property`` whenever the model value changes. Returns the callback id
    from ``GroupedPropertyDict.add_property_on_change_callback``.
    """
    return properties.add_property_on_change_callback(
        group, property_id, partial(set_homie_property_from_python_property, homie_property)
    )
