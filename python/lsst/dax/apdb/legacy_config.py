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

"""Module containing old pex_config-based configuration class.

The main purpose of this class is to support conversion of the old
configuration files to the new pydantic-based format. It will be removed
after deprecation release 29.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from lsst.pex.config import Config, Field

if TYPE_CHECKING:
    from .config import ApdbConfig as NewApdbConfig


def _data_file_name(basename: str) -> str:
    """Return path name of a data file in sdm_schemas package."""
    return os.path.join("${SDM_SCHEMAS_DIR}", "yml", basename)


class ApdbConfig(Config):
    """Part of Apdb configuration common to all implementations."""

    read_sources_months = Field[int](doc="Number of months of history to read from DiaSource", default=12)
    read_forced_sources_months = Field[int](
        doc="Number of months of history to read from DiaForcedSource", default=12
    )
    schema_file = Field[str](
        doc="Location of (YAML) configuration file with standard schema", default=_data_file_name("apdb.yaml")
    )
    schema_name = Field[str](doc="Name of the schema in YAML configuration file.", default="ApdbSchema")
    extra_schema_file = Field[str](
        doc=(
            "Location of (YAML) configuration file with extra schema, "
            "definitions in this file are merged with the definitions in "
            "'schema_file', extending or replacing parts of the schema."
        ),
        default=None,
        optional=True,
        deprecated="This field is deprecated, its value is not used.",
    )
    use_insert_id = Field[bool](
        doc=(
            "If True, make and fill additional tables used for replication. "
            "Databases created with earlier versions of APDB may not have these tables, "
            "and corresponding methods will not work for them."
        ),
        default=False,
    )
    replica_chunk_seconds = Field[int](
        default=600,
        doc="Time extent for replica chunks, new chunks are created every specified number of seconds.",
    )

    def to_model(self) -> NewApdbConfig:
        """Convert pex_config configuration to a new pydantic model."""
        raise NotImplementedError()

    @classmethod
    def legacy_load(cls, config: bytes) -> ApdbConfig:
        """Load legacy configuration from pex_config configuration file.

        Parameters
        ----------
        config : `bytes`
            Configuration data.

        Returns
        -------
        config : `ApdbConfig`
            Legacy configuration instance.
        """
        # As old configuration classes have been moved, we need to rewrite
        # their locations in the config.
        config = config.replace(b"lsst.dax.apdb.sql.apdbSql", b"lsst.dax.apdb.sql.legacy_config")
        config = config.replace(b"lsst.dax.apdb.apdbSql", b"lsst.dax.apdb.sql.legacy_config")
        config = config.replace(
            b"lsst.dax.apdb.cassandra.apdbCassandra", b"lsst.dax.apdb.cassandra.legacy_config"
        )
        config = config.replace(b"lsst.dax.apdb.apdbCassandra", b"lsst.dax.apdb.cassandra.legacy_config")
        return Config._fromPython(config.decode())
