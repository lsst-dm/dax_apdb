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

__all__ = ["cassandra_config_options", "common_apdb_options", "sql_config_options"]

import argparse
from typing import TYPE_CHECKING, Any

from ..apdb import ApdbConfig
from ..cassandra import ApdbCassandraConfig
from ..cassandra.config import ApdbCassandraConnectionConfig, ApdbCassandraPartitioningConfig
from ..sql import ApdbSqlConfig
from ..sql.config import ApdbSqlConnectionConfig, ApdbSqlPixelizationConfig

if TYPE_CHECKING:
    from pydantic import BaseModel


def _option_from_pydantic_field(
    group: argparse._ArgumentGroup,
    config_class: type[BaseModel],
    field_name: str,
    *,
    name: str | None = None,
    help: str | None = None,
    type: type | None = None,
    **kwargs: Any,
) -> None:
    """Convert pex_config Field to argparse argument definition."""
    field_info = config_class.model_fields[field_name]
    if name is None:
        name = "--" + field_name.replace("_", "-")
    if help is None:
        help = field_info.description

    if type is None:
        type = field_info.annotation
        if type in (str, int, float):
            kwargs["type"] = type
    else:
        kwargs["type"] = type

    group.add_argument(name, dest=field_name, help=help, **kwargs)


# Options for fields in ApdbConfig.
def common_apdb_options(parser: argparse.ArgumentParser) -> None:
    """Define common configuration options."""
    group = parser.add_argument_group("common APDB options")
    _option_from_pydantic_field(group, ApdbConfig, "schema_file", metavar="URL")
    _option_from_pydantic_field(group, ApdbConfig, "schema_name")
    _option_from_pydantic_field(group, ApdbConfig, "read_sources_months", metavar="NUMBER")
    _option_from_pydantic_field(group, ApdbConfig, "read_forced_sources_months", metavar="NUMBER")
    _option_from_pydantic_field(group, ApdbConfig, "enable_replica", action="store_true", default=False)


# Options for fields in ApdbSqlConfig, db_url is not included.
def sql_config_options(parser: argparse.ArgumentParser) -> None:
    """Define SQL backend configuration options."""
    group = parser.add_argument_group("SQL backend options")
    _option_from_pydantic_field(group, ApdbSqlConfig, "namespace", metavar="IDENTIFIER")
    _option_from_pydantic_field(group, ApdbSqlConnectionConfig, "connection_timeout", metavar="SECONDS")
    _option_from_pydantic_field(
        group, ApdbSqlConfig, "dia_object_index", choices=["baseline", "pix_id_iov", "last_object_table"]
    )
    _option_from_pydantic_field(group, ApdbSqlPixelizationConfig, "htm_level")
    _option_from_pydantic_field(group, ApdbSqlPixelizationConfig, "htm_index_column")
    _option_from_pydantic_field(
        group,
        ApdbSqlConfig,
        "ra_dec_columns",
        help="Names of ra/dec columns in DiaObject table, comma-separated.",
        metavar="RA_COLUMN,DEC_COLUMN",
    )
    _option_from_pydantic_field(group, ApdbSqlConfig, "prefix")


# Options for fields in ApdbCassandraConfig, contact_points is not included.
def cassandra_config_options(parser: argparse.ArgumentParser) -> None:
    """Define Cassandra backend configuration options."""
    group = parser.add_argument_group("Cassandra backend options")
    _option_from_pydantic_field(group, ApdbCassandraConfig, "replica_skips_diaobjects", action="store_true")
    _option_from_pydantic_field(group, ApdbCassandraConnectionConfig, "port", metavar="PORT")
    _option_from_pydantic_field(group, ApdbCassandraConnectionConfig, "username", metavar="USER")
    _option_from_pydantic_field(group, ApdbCassandraConfig, "prefix")
    group.add_argument("--replication-factor", help="Replication factor used when creating new keyspace.")
    group.add_argument(
        "--table-options", help="Path or URI of YAML file containing table options.", metavar="URI"
    )
    _option_from_pydantic_field(
        group,
        ApdbCassandraConnectionConfig,
        "read_consistency",
        choices=["ONE", "TWO", "THREE", "QUORUM", "ALL"],
    )
    _option_from_pydantic_field(
        group,
        ApdbCassandraConnectionConfig,
        "write_consistency",
        choices=["ONE", "TWO", "THREE", "QUORUM", "ALL"],
    )
    _option_from_pydantic_field(group, ApdbCassandraConnectionConfig, "read_timeout", metavar="SECONDS")
    _option_from_pydantic_field(group, ApdbCassandraConnectionConfig, "write_timeout", metavar="SECONDS")
    _option_from_pydantic_field(
        group,
        ApdbCassandraConfig,
        "ra_dec_columns",
        help="Names of ra/dec columns in DiaObject table, comma-separated.",
        metavar="RA_COLUMN,DEC_COLUMN",
    )
    group = parser.add_argument_group("Cassandra partitioning options")
    _option_from_pydantic_field(group, ApdbCassandraPartitioningConfig, "part_pixelization", metavar="NAME")
    _option_from_pydantic_field(group, ApdbCassandraPartitioningConfig, "part_pix_level", metavar="LEVEL")
    _option_from_pydantic_field(
        group, ApdbCassandraPartitioningConfig, "time_partition_tables", action="store_true"
    )
    _option_from_pydantic_field(
        group, ApdbCassandraPartitioningConfig, "time_partition_start", metavar="TIME"
    )
    _option_from_pydantic_field(group, ApdbCassandraPartitioningConfig, "time_partition_end", metavar="TIME")
