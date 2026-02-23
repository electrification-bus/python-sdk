import uuid
import logging
import re
from enum import Enum
from threading import Lock, RLock
from typing import List, Callable, Union, Optional, Any, Type, Dict


class ChangeEvent(Enum):
    """Events fired by GroupedPropertyDict"""

    GROUP_CREATED = "group_created"
    GROUP_DELETED = "group_deleted"
    PROPERTY_ADDED = "property_added"
    PROPERTY_REMOVED = "property_removed"
    PROPERTY_CHANGED = "property_changed"
    BULK_UPDATE = "bulk_update"


class Property:
    """
    A Property is modeled (very) loosely on a Homie Property
    (https://homieiot.github.io/specification/)
    A Property has a:
      id - string
      value - Any
      type - A Python type, or a string representing Homie types that are not native Python types (i.e. JSON)
      format - string, tells you something more about the value, see Homie
    You say: Gee that seems like a variable...
    Correct-a-mundo, BUT a Property supports a (list of) callback functions,
      which are called/invoked whenever the Property's value is set to a changed value
    Note also that a Property's type is explictly defined, a feature Python variables doesn't provide
    "format" is stolen vertbatim from Homie, used here most often to provide clients with the list of possible ENUM values,
      which is valuable/useful because often the client doesn't have access to the enum itself (due to scoping)
    All methods/operations are intended to be thread-safe, please file an issue if you beleive otherwise
    """

    def __init__(
        self,
        id: Optional[str] = None,
        value: Any = None,
        type: Union[Type, str, None] = None,
        format: Optional[str] = None,
        entity_setter: Optional[Callable] = None,
        from_dict: dict = {},
    ):
        self._lock = Lock()
        self._change_callbacks = {}
        self._set_callbacks = {}
        self._entity_setter = None
        if not from_dict:
            self._id = id
            self._value = value
            self._type = type
            self._format = format
            self._entity_setter = entity_setter
        else:
            self._id = from_dict.get("id")
            self._value = from_dict.get("value", None)
            self._type = from_dict.get("type", None)
            self._format = from_dict.get("format", None)
            self._entity_setter = from_dict.get("entity_setter", None)
        if not self._id:
            # Specifying the id is required!
            logging.warning(f"reason=propertyInitNoIdSpecified!")
            # TODO, throw an exception?

    def id(self) -> str:
        """
        Returns id of the Property
        """
        with self._lock:
            return self._id

    def value(self) -> Any:
        """
        Returns value of the Property
        """
        with self._lock:
            return self._value

    def type(self) -> Any:
        """
        Returns type of the Property
        """
        with self._lock:
            return self._type

    def format(self) -> Any:
        """
        Returns format of the Property
        """
        with self._lock:
            return self._format

    def add_on_change_callback(self, callback: Callable) -> uuid.UUID:
        """
        Adds callback to the "list" of callbacks that will be called when the Property is set to a changed value
        Returns a callback_id, (a uuid1) that can be used subsequently to remove the callback
        A callback is a function of one argument, the Property
        """
        callback_id = uuid.uuid1()
        with self._lock:
            self._change_callbacks.update({callback_id: callback})
        return callback_id

    def add_on_set_callback(self, callback: Callable) -> uuid.UUID:
        """
        Adds callback to the "list" of callbacks that will be called when the Property is set, even if that set doesn't change the value
        Returns a callback_id, (a uuid1) that can be used subsequently to remove the callback
        A callback is a function of one argument, the Property
        """
        callback_id = uuid.uuid1()
        with self._lock:
            self._set_callbacks.update({callback_id: callback})
        return callback_id

    def remove_callback(self, callback_id: uuid.UUID) -> bool:
        """
        Removes the callback associated with callback_id from the "list" of callbacks
        Returns True if successful, False if callback_id not found
        """
        with self._lock:
            if callback_id in self._change_callbacks:
                self._change_callbacks.pop(callback_id, None)
                return True
            elif callback_id in self._set_callbacks:
                self._set_callbacks.pop(callback_id, None)
                return True
            else:
                logging.warning(
                    f"removeCallbackNoSuchId,callbackId={callback_id},propertyId={self._id}"
                )

    def set_value(self, new_value: Any) -> Any:
        """
        Sets the value of the Property
        Invokes all on_set callbacks
        If the value changes as a result of this method, then invoke all on_change callbacks
        Returns the new value
        """
        old_value = self._value
        with self._lock:
            self._value = new_value
            on_change_callback_items = self._change_callbacks.items()
            on_set_callback_items = self._set_callbacks.items()
            # We have mutated the value, and obtained all the callbacks, so OK to release lock?
        # Invoke on_set callbacks
        for callback_id, callback in on_set_callback_items:
            try:
                callback(self)
            except Exception as e:
                logging.warning(
                    f"reason=setValueOnSetCallbackException,propertyId={self._id},newValue={new_value},callbackId={callback_id},e={e}"
                )
        # Do we need to invoke on_change callbacks?
        if new_value != old_value:
            for callback_id, callback in on_change_callback_items:
                try:
                    callback(self)
                except Exception as e:
                    logging.warning(
                        f"reason=setValueOnChangeCallbackException,propertyId={self._id},newValue={new_value},callbackId={callback_id}"
                    )
        return new_value

    def set_entity(self, new_value: Any) -> Any:
        """
        Used by client(s) to set the state of the entity the property represents,
        which is done by the property invoking a registered set_entity_callback function
        Presumably changing the entity state will eventually result in the property's state being set/changed,
        which will then invoke any on_set and/or in_change callbacks
        Returns the new value
        """
        if self._entity_setter:
            logging.info(
                f"reason=setEntity,propertyId={self._id},new_value={new_value}"
            )
            try:
                self._entity_setter(new_value)
            except Exception as e:
                logging.warning(
                    f"reason=setEntityException,propertyId={self._id},new_value={new_value},e={e}"
                )
        else:
            logging.warning(f"reason=setEntityNoSetter,propertyId={self._id}")
        return new_value

    def set_entity_setter(self, entity_setter: Callable) -> None:
        """
        Registers an entity_setter function for the property
        """
        with self._lock:
            self._entity_setter = entity_setter

    def as_dict(self) -> dict:
        return vars(self)


class BulkUpdateContext:
    """Context manager for bulk updates to GroupedPropertyDict"""

    def __init__(self, grouped_dict):
        self.grouped_dict = grouped_dict
        self.pending_events = []

    def __enter__(self):
        self.grouped_dict._bulk_mode = True
        self.grouped_dict._bulk_context = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.grouped_dict._bulk_mode = False
        self.grouped_dict._bulk_context = None
        if not exc_type and self.pending_events:
            # Fire single BULK_UPDATE event with all changes
            for observer_id, callback in self.grouped_dict._observers.items():
                try:
                    callback(ChangeEvent.BULK_UPDATE, changes=self.pending_events)
                except Exception as e:
                    logging.warning(
                        f"reason=bulkUpdateObserverException,observerId={observer_id},e={e}"
                    )
        return False

    def add_event(self, event_type: ChangeEvent, **kwargs):
        """Add event to pending list"""
        event = {"event_type": event_type}
        event.update(kwargs)
        self.pending_events.append(event)


class PropertyDict:
    """
    PropertyDict is a dict of Property objects keyed by property.id.
    All methods/operations are intended to be thread-safe, please file an issue if you believe otherwise.
    Thread-safety implementation relies on both the PropertyDict itself AND
    the thread-safety of the underlying Property method calls.
    """

    def __init__(self):
        self._lock = Lock()
        self._properties = {}

    def get(self, id: str) -> Optional[Property]:
        """
        Returns the Property with id, or None if not found
        """
        with self._lock:
            if id not in self._properties:
                logging.debug(f"reason=propertyDictNoPropertyByThatId,id={id}")
            return self._properties.get(id, None)

    def has_property(self, id: str) -> bool:
        """Check if a property exists, without logging"""
        with self._lock:
            return id in self._properties

    def add_property(self, property: Property) -> Property:
        """
        Adds Property to the dict, returns the Property
        """
        # id() is thread-safe for a Property
        property_id = property.id()
        with self._lock:
            self._properties[property_id] = property
        return property

    def add_property_from_dict(self, property_dict: dict = {}) -> Property:
        """
        Creates a Property from property_dict and adds it.
        Returns the created Property.
        """
        property = Property(from_dict=property_dict)
        return self.add_property(property)

    def delete_property(self, id: str) -> bool:
        """
        Remove a property by id.
        Returns True if removed, False if not found.
        """
        with self._lock:
            if id in self._properties:
                del self._properties[id]
                return True
            return False

    def value(self, id: str) -> Any:
        """Returns the value of Property id"""
        property = self.get(id)
        if property:
            return property.value()
        return None

    def type(self, id: str) -> Any:
        """Returns the type of Property id"""
        property = self.get(id)
        if property:
            return property.type()
        return None

    def format(self, id: str) -> Any:
        """Returns the format of Property id"""
        property = self.get(id)
        if property:
            return property.format()
        return None

    def set_value(self, id: str, value: Any) -> Any:
        """
        Sets the value of Property id.
        Returns the new value.
        """
        this_property = self.get(id)
        if not this_property:
            logging.warning(
                f"reason=setPropertyValueNoPropertyByThatId,id={id},value={value}"
            )
            return None
        # set_value() is thread-safe for Property
        return this_property.set_value(value)

    def set_entity(self, id: str, value: Any) -> Any:
        """Calls the entity_setter of Property id. Returns the new value."""
        this_property = self.get(id)
        if not this_property:
            logging.warning(
                f"reason=propertyDictSetEntityNoPropertyByThatId,id={id},value={value}"
            )
            return None
        return this_property.set_entity(value)

    def set_entity_setter(self, id: str, callback: Callable) -> None:
        """Sets entity_setter on Property id"""
        this_property = self.get(id)
        if not this_property:
            logging.warning(
                f"reason=propertyDictSetEntitySetterNoPropertyByThatId,id={id}"
            )
            return None
        this_property.set_entity_setter(callback)

    def add_on_change_callback(
        self, id: str, callback: Callable
    ) -> Optional[uuid.UUID]:
        """Adds on_change callback to Property id. Returns callback_id."""
        this_property = self.get(id)
        if not this_property:
            logging.warning(
                f"reason=propertyDictAddOnChangeCallbackNoPropertyByThatId,id={id}"
            )
            return None
        return this_property.add_on_change_callback(callback)

    def add_on_set_callback(self, id: str, callback: Callable) -> Optional[uuid.UUID]:
        """Adds on_set callback to Property id. Returns callback_id."""
        this_property = self.get(id)
        if not this_property:
            logging.warning(
                f"reason=propertyDictAddOnSetCallbackNoPropertyByThatId,id={id}"
            )
            return None
        return this_property.add_on_set_callback(callback)

    def remove_callback(self, id: str, callback_id: uuid.UUID) -> bool:
        """Removes callback from Property id. Returns True if successful."""
        this_property = self.get(id)
        if not this_property:
            logging.warning(
                f"reason=propertyDictRemoveCallbackNoPropertyByThatId,id={id}"
            )
            return False
        return this_property.remove_callback(callback_id)

    def items(self) -> List:
        """
        Returns a list containing tuples of each (id, Property) in the PropertyDict
        """
        with self._lock:
            return self._properties.items()

    def ids(self) -> List:
        """
        Returns a list containing id of each Property in the PropertyDict
        """
        with self._lock:
            return list(self._properties.keys())

    def as_dict(self) -> dict:
        """Serialize all properties to a dict"""
        returned_dict = {}
        for id, property in self.items():
            returned_dict[id] = property.as_dict()
        return returned_dict


class GroupedPropertyDict:
    """
    GroupedPropertyDict is a dict of PropertyDict instances,
    keyed by group name. Each PropertyDict contains Property instances keyed by property.id.
    In practice, this is a good way to deal with "a bunch of Properties",
      and to "group" collections of Properties.
    All methods/operations are intended to be thread-safe, please file an issue if you beleive otherwise
      Thread-safety is implementation is (attempted?) to rely on both the GroupedPropertyDict itself AND
        the thread-safety of the underlying PropertyDict and Property method calls.
    """

    def __init__(self):
        self._lock = (
            RLock()
        )  # RLock needed because _fire_event is called while holding lock
        self._groups = {}
        self._observers = {}
        self._bulk_mode = False
        self._bulk_context = None

    def _get_group(self, group: str) -> Optional[PropertyDict]:
        """Get the PropertyDict for the given group name, or None if not found"""
        with self._lock:
            return self._groups.get(group, None)

    def value(self, group: str, id: str) -> Any:
        """
        Returns the value of Property group.id
        """
        pd = self._get_group(group)
        if pd is None:
            logging.debug(
                f"reason=groupedPropertiesGetNoGroupByThatName,group={group},id={id}"
            )
            return None
        return pd.value(id)

    def type(self, group: str, id: str) -> Any:
        """
        Returns the type of Property group.id
        """
        pd = self._get_group(group)
        if pd is None:
            logging.debug(
                f"reason=groupedPropertiesGetNoGroupByThatName,group={group},id={id}"
            )
            return None
        return pd.type(id)

    def format(self, group: str, id: str) -> Any:
        """
        Returns the format of Property group.id
        """
        pd = self._get_group(group)
        if pd is None:
            logging.debug(
                f"reason=groupedPropertiesGetNoGroupByThatName,group={group},id={id}"
            )
            return None
        return pd.format(id)

    def get(self, group: str, id: str) -> Optional[Property]:
        """
        Returns the Property group.id
        """
        pd = self._get_group(group)
        if pd is None:
            logging.debug(
                f"reason=groupedPropertiesGetNoGroupByThatName,group={group},id={id}"
            )
            return None
        return pd.get(id)

    def create_group(self, group_name: str) -> None:
        """Explicitly create a new group"""
        if not isinstance(group_name, str) or not group_name:
            logging.warning(
                f"reason=createGroupInvalidGroupName,groupName={group_name},type={type(group_name).__name__}"
            )
            return
        with self._lock:
            if group_name in self._groups:
                logging.warning(f"reason=groupAlreadyExists,groupName={group_name}")
                return
            self._groups[group_name] = PropertyDict()
            self._fire_event(ChangeEvent.GROUP_CREATED, group_name=group_name)

    def delete_group(self, group_name: str) -> None:
        """Delete a group and all its properties"""
        with self._lock:
            if group_name not in self._groups:
                logging.warning(f"reason=deleteGroupNotFound,groupName={group_name}")
                return
            del self._groups[group_name]
            self._fire_event(ChangeEvent.GROUP_DELETED, group_name=group_name)

    def delete_property(self, group: str, property_id: str) -> None:
        """Delete a specific property from a group"""
        with self._lock:
            if group not in self._groups:
                logging.warning(
                    f"reason=deletePropertyGroupNotFound,group={group},propertyId={property_id}"
                )
                return
            pd = self._groups[group]
            if not pd.has_property(property_id):
                logging.warning(
                    f"reason=deletePropertyNotFound,group={group},propertyId={property_id}"
                )
                return
            pd.delete_property(property_id)
            self._fire_event(
                ChangeEvent.PROPERTY_REMOVED, group_name=group, property_id=property_id
            )

    def group_exists(self, group_name: str) -> bool:
        """Check if a group exists"""
        with self._lock:
            return group_name in self._groups

    def add_property(self, group: str, property: Property) -> Property:
        """
        Adds Property to the group, returns the Property
        """
        if not isinstance(group, str) or not group:
            logging.warning(
                f"reason=addPropertyInvalidGroupName,group={group},type={type(group).__name__}"
            )
            return None
        # id() thread-safe for Property
        property_id = property.id()
        with self._lock:
            if group not in self._groups:
                # Group doesn't exist, create it first
                self._groups[group] = PropertyDict()
                self._fire_event(ChangeEvent.GROUP_CREATED, group_name=group)
            self._groups[group].add_property(property)
            self._fire_event(
                ChangeEvent.PROPERTY_ADDED,
                group_name=group,
                property_id=property_id,
                property=property,
            )
            return property

    def add_property_from_dict(self, group: str, property_dict: dict = {}) -> Property:
        """
        Creates a Property from property_dict, and adds the Property to the group
        Returns the created Property
        """
        property = Property(from_dict=property_dict)
        # add_property() itself is thread-safe, so no lock needed here
        return self.add_property(group, property)

    def add_property_on_change_callback(
        self, group: str, id: str, callback: Callable
    ) -> uuid.UUID:
        """
        Adds callback to the list of callbacks that will be called when the Property group.id is set to a changed value
        Returns a callback_id, (a uuid1) that can be used subsequently to remove the callback
        """
        pd = self._get_group(group)
        if pd is None:
            logging.warning(
                f"reason=groupedPropertiesAddPropertyOnChangeCallbackNoPropertyByThatId,group={group},id={id}"
            )
            return None
        result = pd.add_on_change_callback(id, callback)
        if result is None:
            logging.warning(
                f"reason=groupedPropertiesAddPropertyOnChangeCallbackNoPropertyByThatId,group={group},id={id}"
            )
        return result

    def add_property_on_set_callback(
        self, group: str, id: str, callback: Callable
    ) -> uuid.UUID:
        """
        Adds callback to the list of callbacks that will be called when the Property group.id is set
        Returns a callback_id, (a uuid1) that can be used subsequently to remove the callback
        """
        pd = self._get_group(group)
        if pd is None:
            logging.warning(
                f"reason=groupedPropertiesAddPropertyOnSetCallbackNoPropertyByThatId,group={group},id={id}"
            )
            return None
        result = pd.add_on_set_callback(id, callback)
        if result is None:
            logging.warning(
                f"reason=groupedPropertiesAddPropertyOnSetCallbackNoPropertyByThatId,group={group},id={id}"
            )
        return result

    def remove_property_callback(
        self, group: str, id: str, callback_id: uuid.UUID
    ) -> bool:
        """
        Removes the callback associated with callback_id from the "list" of callbacks for Property group.id
        Returns True if successful, False if callback_id not found
        """
        pd = self._get_group(group)
        if pd is None:
            logging.warning(
                f"reason=groupedPropertiesRemovePropertyCallbackNoPropertyByThatId,group={group},id={id}"
            )
            return False
        return pd.remove_callback(id, callback_id)

    def set_value(self, group: str, id: str, value: Any) -> Any:
        """
        Sets the value of the Property group.id
        Returns the new value
        """
        pd = self._get_group(group)
        if pd is None:
            logging.warning(
                f"reason=groupedPropertiesSetValueNoGroupByThatName,group={group},id={id},value={value}"
            )
            return None
        this_property = pd.get(id)
        if not this_property:
            logging.warning(
                f"reason=groupedPropertiesSetValueNoPropertyByThatId,group={group},id={id},value={value}"
            )
            return None
        old_value = this_property.value()
        result = this_property.set_value(value)
        if old_value != value:
            self._fire_event(
                ChangeEvent.PROPERTY_CHANGED,
                group_name=group,
                property_id=id,
                property=this_property,
                old_value=old_value,
                new_value=value,
            )
        return result

    def set_entity(self, group: str, id: str, value: Any) -> Any:
        """
        Calls the entity_setter of the Property group.id
        Returns the new value
        """
        # TODO change .info to .debug
        logging.info(f"reason=groupedProperties,group={group},id={id},value={value}")
        pd = self._get_group(group)
        if pd is None:
            logging.warning(
                f"reason=groupedPropertiesSetEntityNoGroupByThatName,group={group},id={id},value={value}"
            )
            return None
        return pd.set_entity(id, value)

    def set_entity_setter(self, group: str, id: str, callback: Callable) -> None:
        """
        Sets entity_setter on the Property group.id
        """
        pd = self._get_group(group)
        if pd is None:
            logging.warning(
                f"reason=groupedPropertiesSetEntitySetterNoPropertyByThatId,group={group},id={id}"
            )
            return None
        return pd.set_entity_setter(id, callback)

    def groups(self) -> List:
        """
        Returns a list of groups
        """
        with self._lock:
            return list(self._groups.keys())

    def items(self, group: str) -> List:
        """
        Returns a list containing tuples of each (id, Property) in the group
        """
        with self._lock:
            return self._groups[group].items()

    def as_dict(self) -> dict:
        with self._lock:
            returned_dict = {}
            for group_name, pd in self._groups.items():
                returned_dict[group_name] = pd.as_dict()
            return returned_dict

    def add_observer(
        self,
        callback: Callable,
        event_types: List[ChangeEvent] = None,
        group_filter: str = None,
    ) -> uuid.UUID:
        """
        Register an observer for changes
        callback signature: callback(event_type: ChangeEvent, **kwargs)
        Returns observer_id for later removal
        """
        observer_id = uuid.uuid1()
        with self._lock:
            self._observers[observer_id] = callback
        logging.info(f"reason=observerRegistered,observerId={observer_id}")
        return observer_id

    def remove_observer(self, observer_id: uuid.UUID) -> bool:
        """Remove an observer"""
        with self._lock:
            if observer_id in self._observers:
                del self._observers[observer_id]
                logging.info(f"reason=observerRemoved,observerId={observer_id}")
                return True
            return False

    def bulk_update(self) -> BulkUpdateContext:
        """Return a context manager for bulk updates"""
        return BulkUpdateContext(self)

    def _fire_event(self, event_type: ChangeEvent, **kwargs):
        """Fire an event to all observers"""
        if self._bulk_mode and self._bulk_context:
            # In bulk mode, accumulate events
            self._bulk_context.add_event(event_type, **kwargs)
        else:
            # Fire immediately
            with self._lock:
                observers = list(self._observers.items())
            for observer_id, callback in observers:
                try:
                    callback(event_type, **kwargs)
                except Exception as e:
                    logging.warning(
                        f"reason=observerCallbackException,observerId={observer_id},eventType={event_type},e={e}"
                    )

    def get_groups_by_property_value(self, property_id: str, value: Any) -> List[str]:
        """
        Return list of group names containing a property with the specified id and value.

        Args:
            property_id: The id of the property to search for
            value: The value the property must have

        Returns:
            List of group names where the property exists and has the matching value
        """
        with self._lock:
            matching_groups = []
            for group_name, pd in self._groups.items():
                if pd.has_property(property_id) and pd.value(property_id) == value:
                    matching_groups.append(group_name)
            return matching_groups

    def has_group(self, group_name: str) -> bool:
        """Check if specific group exists"""
        with self._lock:
            return group_name in self._groups
