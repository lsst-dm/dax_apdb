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

from lsst.pex.config import ChoiceField, Field, ListField

from .. import legacy_config
from . import config


class ApdbSqlConfig(legacy_config.ApdbConfig):
    """Legacy APDB configuration class for SQL implementation (ApdbSql)."""

    db_url = Field[str](doc="SQLAlchemy database connection URI")
    isolation_level = ChoiceField[str](
        doc=(
            "Transaction isolation level, if unset then backend-default value "
            "is used, except for SQLite backend where we use READ_UNCOMMITTED. "
            "Some backends may not support every allowed value."
        ),
        allowed={
            "READ_COMMITTED": "Read committed",
            "READ_UNCOMMITTED": "Read uncommitted",
            "REPEATABLE_READ": "Repeatable read",
            "SERIALIZABLE": "Serializable",
        },
        default=None,
        optional=True,
    )
    connection_pool = Field[bool](
        doc="If False then disable SQLAlchemy connection pool. Do not use connection pool when forking.",
        default=True,
    )
    connection_timeout = Field[float](
        doc=(
            "Maximum time to wait time for database lock to be released before exiting. "
            "Defaults to sqlalchemy defaults if not set."
        ),
        default=None,
        optional=True,
    )
    sql_echo = Field[bool](doc="If True then pass SQLAlchemy echo option.", default=False)
    dia_object_index = ChoiceField[str](
        doc="Indexing mode for DiaObject table",
        allowed={
            "baseline": "Index defined in baseline schema",
            "pix_id_iov": "(pixelId, objectId, iovStart) PK",
            "last_object_table": "Separate DiaObjectLast table",
        },
        default="baseline",
    )
    htm_level = Field[int](doc="HTM indexing level", default=20)
    htm_max_ranges = Field[int](doc="Max number of ranges in HTM envelope", default=64)
    htm_index_column = Field[str](
        default="pixelId", doc="Name of a HTM index column for DiaObject and DiaSource tables"
    )
    ra_dec_columns = ListField[str](default=["ra", "dec"], doc="Names of ra/dec columns in DiaObject table")
    dia_object_columns = ListField[str](
        doc="List of columns to read from DiaObject, by default read all columns", default=[]
    )
    prefix = Field[str](doc="Prefix to add to table names and index names", default="")
    namespace = Field[str](
        doc=(
            "Namespace or schema name for all tables in APDB database. "
            "Presently only works for PostgreSQL backend. "
            "If schema with this name does not exist it will be created when "
            "APDB tables are created."
        ),
        default=None,
        optional=True,
    )
    timer = Field[bool](doc="If True then print/log timing information", default=False)

    def validate(self) -> None:
        super().validate()
        if len(self.ra_dec_columns) != 2:
            raise ValueError("ra_dec_columns must have exactly two column names")

    def to_model(self) -> config.ApdbSqlConfig:
        # Docstring inherited from base class.
        connection_config = config.ApdbSqlConnectionConfig(
            isolation_level=self.isolation_level,
            connection_pool=self.connection_pool,
            connection_timeout=self.connection_timeout,
        )
        if self.sql_echo:
            connection_config.extra_parameters["echo"] = self.sql_echo
        pixelization_config = config.ApdbSqlPixelizationConfig(
            htm_level=self.htm_level,
            htm_max_ranges=self.htm_max_ranges,
            htm_index_column=self.htm_index_column,
        )
        new_config = config.ApdbSqlConfig(
            schema_file=self.schema_file,
            schema_name=self.schema_name,
            read_sources_months=self.read_sources_months,
            read_forced_sources_months=self.read_forced_sources_months,
            enable_replica=self.use_insert_id,
            replica_chunk_seconds=self.replica_chunk_seconds,
            db_url=self.db_url,
            namespace=self.namespace,
            connection_config=connection_config,
            pixelization=pixelization_config,
            dia_object_index=self.dia_object_index,
            ra_dec_columns=list(self.ra_dec_columns),
            dia_object_columns=list(self.dia_object_columns),
            prefix=self.prefix,
        )
        return new_config
