# This file is part of dax_apdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["ApdbConfigFreezer"]

import json
from collections.abc import Iterable
from operator import attrgetter
from typing import Generic, TypeVar

import pydantic

from .apdb import ApdbConfig

_Config = TypeVar("_Config", bound=ApdbConfig)


class ApdbConfigFreezer(Generic[_Config]):
    """Class that handles freezing of the configuration parameters, this is
    an implementation detail for use in Apdb subclasses.

    Parameters
    ----------
    field_names : `~collections.abc.Iterable` [`str`]
        Names of configuration fields to be frozen, they can be hierarchical
        (dot-separated).
    """

    def __init__(self, field_names: Iterable[str]):
        self._field_names = list(field_names)
        self._getters = {name.split(".")[-1]: attrgetter(name) for name in self._field_names}
        self._attr_parents = {}
        for name in self._field_names:
            path = name.split(".")
            self._attr_parents[path[-1]] = path[:-1]

    def to_json(self, config: ApdbConfig) -> str:
        """Convert part of the configuration object to JSON string.

        Parameters
        ----------
        config : `ApdbConfig`
            Configuration object.

        Returns
        -------
        json_str : `str`
            JSON representation of the frozen part of the config. For
            hierarchical dot-separated names only that last part of the name is
            used in the returned JSON mapping.
        """
        data = {name: getter(config) for name, getter in self._getters.items()}
        return json.dumps(data)

    def update(self, config: _Config, json_str: str) -> _Config:
        """Update configuration field values from a JSON string.

        Parameters
        ----------
        config : `ApdbConfig`
            Configuration object.
        json_str : str
            String containing JSON representation of configuration.

        Returns
        -------
        updated : `ApdbConfig`
            Copy of the ``config`` with some fields updated from JSON object.

        Raises
        ------
        TypeError
            Raised if JSON string does not represent JSON object.
        ValueError
            Raised if JSON object contains key which is not present in
            ``field_names``.
        """
        data = json.loads(json_str)
        if not isinstance(data, dict):
            raise TypeError(f"JSON string must be convertible to object: {json_str!r}")

        # Older config used different parameter name for it.
        if "use_insert_id" in data:
            data["enable_replica"] = data.pop("use_insert_id")
        if "use_insert_id_skips_diaobjects" in data:
            data["replica_skips_diaobjects"] = data.pop("use_insert_id_skips_diaobjects")

        # We want to update some fields and re-validate the model, the easiest
        # way to do it is to convert model to a dict first, update the dict and
        # convert back to model.
        model_data = config.model_dump()
        for attr, value in data.items():
            parent_path = self._attr_parents.get(attr)
            if parent_path is None:
                raise ValueError(f"Frozen configuration contains unexpected attribute {attr}={value}")
            obj = model_data
            for parent_attr in parent_path:
                obj = obj[parent_attr]
            obj[attr] = value

        try:
            new_config = type(config).model_validate(model_data, strict=True)
            return new_config
        except pydantic.ValidationError as exc:
            raise ValueError("Validation error for frozen config") from exc
