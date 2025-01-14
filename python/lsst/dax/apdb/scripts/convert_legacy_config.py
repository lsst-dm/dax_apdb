# This file is part of dax_apdb
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["convert_legacy_config"]

from lsst.resources import ResourcePath

from ..legacy_config import ApdbConfig


def convert_legacy_config(legacy_config: str, new_config: str) -> None:
    """List contents of APDB index file.

    Parameters
    ----------
    lecagy_config : `str`
        Path or URL for the existing APDB configuration in pex_config format.
    new_config : `str`
        Path or URL for the output configuration in YAML format.
    """
    path = ResourcePath(legacy_config)
    config_bytes = path.read()

    pex_config = ApdbConfig.legacy_load(config_bytes)
    pydantic_config = pex_config.to_model()
    pydantic_config.save(new_config)
