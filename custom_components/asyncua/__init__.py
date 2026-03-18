"""The asyncua integration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
import functools
import logging
import time
from typing import Any, Union

from asyncua import Client, ua
from asyncua.common import ua_utils
from asyncua.ua.uatypes import DataValue
import voluptuous as vol

from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryError,
    ConfigEntryNotReady,
)
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_component import DEFAULT_SCAN_INTERVAL
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    ATTR_NODE_HUB,
    ATTR_NODE_ID,
    ATTR_VALUE,
    CONF_HUB_ID,
    CONF_HUB_MANUFACTURER,
    CONF_HUB_MODEL,
    CONF_HUB_PASSWORD,
    CONF_HUB_SCAN_INTERVAL,
    CONF_HUB_SUBSCRIBE,
    CONF_HUB_SUBSCRIBE_PERIOD,
    CONF_HUB_URL,
    CONF_HUB_USERNAME,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    DOMAIN,
    SERVICE_SET_VALUE,
)

_LOGGER = logging.getLogger("asyncua")
_LOGGER.setLevel(logging.WARNING)

BASE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HUB_ID): cv.string,
        vol.Required(CONF_HUB_URL): cv.string,
        vol.Optional(CONF_HUB_MANUFACTURER, default=""): cv.string,
        vol.Optional(CONF_HUB_MODEL, default=""): cv.string,
        vol.Optional(CONF_HUB_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL): int,
        vol.Optional(CONF_HUB_SUBSCRIBE, default=False): cv.boolean,
        vol.Optional(CONF_HUB_SUBSCRIBE_PERIOD, default=500): int,
        vol.Inclusive(CONF_HUB_USERNAME, None): cv.string,
        vol.Inclusive(CONF_HUB_PASSWORD, None): cv.string,
    }
)

SERVICE_SET_VALUE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_NODE_HUB): cv.string,
        vol.Required(ATTR_NODE_ID): cv.string,
        vol.Required(ATTR_VALUE): vol.Any(
            float,
            int,
            str,
            cv.byte,
            cv.boolean,
            cv.time,
        ),
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.All(
            cv.ensure_list,
            [
                vol.Any(BASE_SCHEMA),
            ],
        ),
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(
    hass: HomeAssistant,
    config: ConfigType,
) -> bool:
    """Set up the template for asyncua including OpcuaHub and AsyncuaCoordinator."""
    hass.data[DOMAIN] = {}

    async def _set_value(service):
        coordinator: AsyncuaCoordinator = hass.data[DOMAIN][service.data.get(ATTR_NODE_HUB)]
        hub = coordinator.hub
        if coordinator.subscribe and hub.connected:
            await hub.set_value_direct(
                nodeid=service.data[ATTR_NODE_ID],
                value=service.data[ATTR_VALUE],
            )
        else:
            await hub.set_value(
                nodeid=service.data[ATTR_NODE_ID],
                value=service.data[ATTR_VALUE],
            )
        return True

    async def _configure_hub(hub: dict) -> None:
        if hub[CONF_HUB_ID] in hass.data[DOMAIN]:
            raise ConfigEntryError(
                f"Duplicated hub detected {hub[CONF_HUB_ID]}. "
                f"OPCUA hub name must be unique."
            )
        subscribe = hub.get(CONF_HUB_SUBSCRIBE, False)
        coordinator = AsyncuaCoordinator(
            hass=hass,
            name=hub[CONF_HUB_ID],
            hub=OpcuaHub(
                hub_name=hub[CONF_HUB_ID],
                hub_manufacturer=hub[CONF_HUB_MANUFACTURER],
                hub_model=hub[CONF_HUB_MODEL],
                hub_url=hub[CONF_HUB_URL],
                username=hub.get(CONF_HUB_USERNAME),
                password=hub.get(CONF_HUB_PASSWORD),
            ),
            subscribe=subscribe,
            subscribe_period_ms=hub.get(CONF_HUB_SUBSCRIBE_PERIOD, 500),
            update_interval_in_second=timedelta(
                seconds=hub.get(
                    CONF_HUB_SCAN_INTERVAL,
                    DEFAULT_SCAN_INTERVAL,
                ),
            ),
        )
        hass.data[DOMAIN][hub[CONF_HUB_ID]] = coordinator

        if subscribe:
            async def _start_subscription(_event=None) -> None:
                await coordinator.start_subscription()

            hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _start_subscription)
        else:
            await coordinator.async_request_refresh()

        hass.services.async_register(
            domain=DOMAIN,
            service=f"{SERVICE_SET_VALUE}",
            service_func=_set_value,
            schema=SERVICE_SET_VALUE_SCHEMA,
        )

    configure_hub_tasks = [_configure_hub(hub) for hub in config[DOMAIN]]
    await asyncio.gather(*configure_hub_tasks)

    return True


class OpcuaSubHandler:
    """OPC UA subscription handler that dispatches data-change notifications."""

    def __init__(self, callback: Callable[[str, Any], None]) -> None:
        self._callback = callback
        self._name_map: dict = {}  # NodeId -> entity name

    def set_node_map(self, name_map: dict) -> None:
        """Map NodeId objects to entity names."""
        self._name_map = name_map

    def datachange_notification(self, node, val, data) -> None:
        """Called by asyncua when a subscribed node value changes."""
        name = self._name_map.get(node.nodeid)
        if name is not None:
            self._callback(name, val)

    def event_notification(self, event) -> None:
        pass

    def status_change_notification(self, status) -> None:
        pass


class OpcuaHub:
    """Hub that coordinate communicate to OPCUA server."""

    def __init__(
        self,
        hub_name: str,
        hub_manufacturer: str,
        hub_model: str,
        hub_url: str,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 4,
    ) -> None:
        """Initialize the OPCUA hub."""
        self._hub_name = hub_name
        self._hub_url = hub_url
        self._username = username
        self._password = password
        self._timeout = timeout
        self._connected: bool = False
        self.device_info = DeviceInfo(
            configuration_url=hub_url,
            manufacturer=hub_manufacturer,
            model=hub_model,
        )

        """Asyncua client"""
        self.client: Client = Client(
            url=hub_url,
            timeout=5,
        )
        self.client.secure_channel_timeout = 60000  # 1 minute
        self.client.session_timeout = 60000  # 1 minute
        if self._username is not None:
            self.client.set_user(username=self._username)
        if self._password is not None:
            self.client.set_password(pwd=self._password)

        self.packet_count: int = 0
        self.elapsed_time: float = 0
        self.cache_val: dict[str, Any] = {}
        self._sub_handler: OpcuaSubHandler | None = None
        self._subscription: Any = None

    @property
    def hub_name(self) -> str:
        """Return opcua hub name."""
        return self._hub_name

    @property
    def hub_url(self) -> str:
        """Return opcua hub url."""
        return self._hub_url

    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected

    @connected.setter
    def connected(self, val: bool) -> None:
        """Set connection status."""
        self._connected = val

    @staticmethod
    def asyncua_wrapper(
        func: Callable[..., Any],
    ) -> Callable[..., Any]:
        """Wrap function to manage OPCUA transaction."""

        @functools.wraps(func)
        async def get_set_wrapper(self, *args: Any, **kwargs: Any) -> Any:
            data = {}
            try:
                start_time = time.perf_counter()
                async with self.client:
                    data = await func(self, *args, **kwargs)
                    self.packet_count += 1
                    self.elapsed_time = time.perf_counter() - start_time
                    self.connected = True
            except RuntimeError as e:
                _LOGGER.error(
                    "RuntimeError while connecting to %s @ %s: %s",
                    self.hub_name,
                    self.hub_url,
                    e,
                )
                self.connected = False
            except TimeoutError as e:
                _LOGGER.error(
                    "Timeout while connecting to %s @ %s: %s",
                    self.hub_name,
                    self.hub_url,
                    e,
                )
                self.connected = False
            except ConnectionRefusedError as e:
                _LOGGER.error(
                    "Connection Refused Error while connecting to %s @ %s: %s",
                    self.hub_name,
                    self.hub_url,
                    e,
                )
                self.connected = False
            return data

        return get_set_wrapper

    @asyncua_wrapper
    async def get_value(self, nodeid: str) -> Any:
        """Get node value and return value."""
        node = self.client.get_node(nodeid=nodeid)
        return await node.read_value()

    @asyncua_wrapper
    async def get_values(self, node_key_pair: dict[str, str]) -> dict | None:
        """Get multiple node values and return value in zip dictionary format."""
        if not (node_key_pair):
            return {}
        nodes = [
            self.client.get_node(nodeid=nodeid) for key, nodeid in node_key_pair.items()
        ]
        vals = await self.client.read_values(nodes=nodes)
        self.cache_val = dict(zip(node_key_pair.keys(), vals, strict=True))
        return self.cache_val

    @asyncua_wrapper
    async def set_value(self, nodeid: str, value: Any) -> bool:
        """Get node variant type automatically and set the value."""
        node = self.client.get_node(nodeid=nodeid)
        node_type = await node.read_data_type_as_variant_type()
        var = ua.Variant(
            ua_utils.string_to_variant(
                string=str(value),
                vtype=node_type,
            )
        )
        await node.write_value(DataValue(var))
        return True

    async def set_value_direct(self, nodeid: str, value: Any) -> bool:
        """Set value using the existing persistent connection (subscribe mode)."""
        try:
            node = self.client.get_node(nodeid=nodeid)
            node_type = await node.read_data_type_as_variant_type()
            var = ua.Variant(
                ua_utils.string_to_variant(
                    string=str(value),
                    vtype=node_type,
                )
            )
            await node.write_value(DataValue(var))
            return True
        except Exception as e:
            _LOGGER.error(
                "Error writing value to node %s on hub %s: %s",
                nodeid,
                self._hub_name,
                e,
            )
            self.connected = False
            return False

    async def start_subscription(
        self,
        node_key_pair: dict[str, str],
        callback: Callable[[str, Any], None],
        period_ms: int = 500,
    ) -> bool:
        """Connect persistently and subscribe to data changes for all nodes."""
        try:
            await self.client.connect()
            self.connected = True

            name_map: dict = {}
            nodes = []
            for name, nodeid_str in node_key_pair.items():
                node = self.client.get_node(nodeid=nodeid_str)
                name_map[node.nodeid] = name
                nodes.append(node)

            self._sub_handler = OpcuaSubHandler(callback=callback)
            self._sub_handler.set_node_map(name_map)
            self._subscription = await self.client.create_subscription(
                period_ms, self._sub_handler
            )
            await self._subscription.subscribe_data_change(nodes)
            _LOGGER.info(
                "OPC UA subscription started for hub %s (%d nodes, period %dms)",
                self._hub_name,
                len(nodes),
                period_ms,
            )
            return True
        except Exception as e:
            _LOGGER.error(
                "Failed to start OPC UA subscription for hub %s: %s",
                self._hub_name,
                e,
            )
            self.connected = False
            return False

    async def stop_subscription(self) -> None:
        """Delete the subscription and disconnect."""
        try:
            if self._subscription is not None:
                await self._subscription.delete()
                self._subscription = None
            await self.client.disconnect()
            self.connected = False
        except Exception as e:
            _LOGGER.warning(
                "Error stopping subscription for hub %s: %s",
                self._hub_name,
                e,
            )


class AsyncuaCoordinator(DataUpdateCoordinator):
    """Coordinator to manage fetching data using OpcuaHub from OPCUA server."""

    def __init__(
        self,
        hass: HomeAssistant,
        name: str,
        hub: OpcuaHub,
        subscribe: bool = False,
        subscribe_period_ms: int = 500,
        update_interval_in_second: timedelta = DEFAULT_SCAN_INTERVAL,
    ) -> None:
        """Initialize the coordinator."""
        self._hub = hub
        self._sensors: list = []
        self._node_key_pair: dict[str, str] = {}
        self._subscribe = subscribe
        self._subscribe_period_ms = subscribe_period_ms
        self._subscribed_cache: dict[str, Any] = {}
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=name,
            update_interval=None if subscribe else update_interval_in_second,
        )

    @property
    def hub(self) -> OpcuaHub:
        """Return OpcuaHub class."""
        return self._hub

    @property
    def sensors(self) -> list:
        """Return all sensors mapped to the OpcuaHub."""
        return self._sensors

    @property
    def node_key_pair(self) -> dict:
        """Return all the node key pairs mapped to the OpcuaHub."""
        return self._node_key_pair

    def add_sensors(self, sensors: list[dict[str, str]]) -> bool:
        """Add new sensors to the sensor list."""
        self._sensors.extend(sensors)
        for _idx_sensor, val_sensor in enumerate(self._sensors):
            self._node_key_pair[val_sensor[CONF_NODE_NAME]] = val_sensor[CONF_NODE_ID]
        return True

    @property
    def subscribe(self) -> bool:
        """Return whether this coordinator uses OPC UA subscriptions."""
        return self._subscribe

    def _subscription_callback(self, name: str, val: Any) -> None:
        """Called from OpcuaSubHandler when a node value changes."""
        self._subscribed_cache[name] = val
        self.async_set_updated_data(dict(self._subscribed_cache))

    async def start_subscription(self) -> None:
        """Start OPC UA subscription for all registered nodes."""
        if not self._node_key_pair:
            _LOGGER.warning(
                "Coordinator %s has no nodes registered; subscription not started.",
                self.name,
            )
            return
        await self._hub.start_subscription(
            node_key_pair=self._node_key_pair,
            callback=self._subscription_callback,
            period_ms=self._subscribe_period_ms,
        )

    async def stop_subscription(self) -> None:
        """Stop OPC UA subscription."""
        await self._hub.stop_subscription()

    async def _async_update_data(self) -> dict[str, Any]:
        """Update the state of the sensor."""
        if self._subscribe:
            return self._subscribed_cache
        vals = await self.hub.get_values(node_key_pair=self.node_key_pair)
        if not self.hub.connected:
            return {}
        return {**vals} if vals is not None else {}
