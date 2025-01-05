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

# If cassandra-driver is not there the module can still be imported.
try:
    import cassandra

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

from .. import legacy_config
from .._auth import DB_AUTH_PATH
from . import config


class ApdbCassandraConfig(legacy_config.ApdbConfig):
    """Configuration class for Cassandra-based APDB implementation."""

    contact_points = ListField[str](
        doc="The list of contact points to try connecting for cluster discovery.", default=["127.0.0.1"]
    )
    private_ips = ListField[str](doc="List of internal IP addresses for contact_points.", default=[])
    port = Field[int](doc="Port number to connect to.", default=9042)
    keyspace = Field[str](doc="Default keyspace for operations.", default="apdb")
    username = Field[str](
        doc=f"Cassandra user name, if empty then {DB_AUTH_PATH} has to provide it with password.",
        default="",
    )
    read_consistency = Field[str](
        doc="Name for consistency level of read operations, default: QUORUM, can be ONE.", default="QUORUM"
    )
    write_consistency = Field[str](
        doc="Name for consistency level of write operations, default: QUORUM, can be ONE.", default="QUORUM"
    )
    read_timeout = Field[float](doc="Timeout in seconds for read operations.", default=120.0)
    write_timeout = Field[float](doc="Timeout in seconds for write operations.", default=60.0)
    remove_timeout = Field[float](doc="Timeout in seconds for remove operations.", default=600.0)
    read_concurrency = Field[int](doc="Concurrency level for read operations.", default=500)
    protocol_version = Field[int](
        doc="Cassandra protocol version to use, default is V4",
        default=cassandra.ProtocolVersion.V4 if CASSANDRA_IMPORTED else 4,
    )
    dia_object_columns = ListField[str](
        doc="List of columns to read from DiaObject[Last], by default read all columns", default=[]
    )
    prefix = Field[str](doc="Prefix to add to table names", default="")
    part_pixelization = ChoiceField[str](
        allowed=dict(htm="HTM pixelization", q3c="Q3C pixelization", mq3c="MQ3C pixelization"),
        doc="Pixelization used for partitioning index.",
        default="mq3c",
    )
    part_pix_level = Field[int](doc="Pixelization level used for partitioning index.", default=11)
    part_pix_max_ranges = Field[int](doc="Max number of ranges in pixelization envelope", default=128)
    ra_dec_columns = ListField[str](default=["ra", "dec"], doc="Names of ra/dec columns in DiaObject table")
    timer = Field[bool](doc="If True then print/log timing information", default=False)
    time_partition_tables = Field[bool](
        doc="Use per-partition tables for sources instead of partitioning by time", default=False
    )
    time_partition_days = Field[int](
        doc=(
            "Time partitioning granularity in days, this value must not be changed after database is "
            "initialized"
        ),
        default=30,
    )
    time_partition_start = Field[str](
        doc=(
            "Starting time for per-partition tables, in yyyy-mm-ddThh:mm:ss format, in TAI. "
            "This is used only when time_partition_tables is True."
        ),
        default="2018-12-01T00:00:00",
    )
    time_partition_end = Field[str](
        doc=(
            "Ending time for per-partition tables, in yyyy-mm-ddThh:mm:ss format, in TAI. "
            "This is used only when time_partition_tables is True."
        ),
        default="2030-01-01T00:00:00",
    )
    query_per_time_part = Field[bool](
        default=False,
        doc=(
            "If True then build separate query for each time partition, otherwise build one single query. "
            "This is only used when time_partition_tables is False in schema config."
        ),
    )
    query_per_spatial_part = Field[bool](
        default=False,
        doc="If True then build one query per spatial partition, otherwise build single query.",
    )
    use_insert_id_skips_diaobjects = Field[bool](
        default=False,
        doc=(
            "If True then do not store DiaObjects when use_insert_id is True "
            "(DiaObjectsChunks has the same data)."
        ),
    )
    idle_heartbeat_interval = Field[int](
        doc=(
            "Interval, in seconds, on which to heartbeat idle connections. "
            "Zero (default) disables heartbeats."
        ),
        default=0,
    )
    idle_heartbeat_timeout = Field[int](
        doc="Timeout, in seconds, on which the heartbeat wait for idle connection responses.",
        default=30,
    )

    def to_model(self) -> config.ApdbCassandraConfig:
        # Docstring inherited from base class.

        # control_connection_timeout is not in the pex_config, but it is set as
        # a default in init_database, so we use the same value here.
        connection_config = config.ApdbCassandraConnectionConfig(
            port=self.port,
            private_ips=list(self.private_ips),
            username=self.username,
            read_consistency=self.read_consistency,
            write_consistency=self.write_consistency,
            read_timeout=self.read_timeout,
            write_timeout=self.write_timeout,
            remove_timeout=self.remove_timeout,
            read_concurrency=self.read_concurrency,
            protocol_version=self.protocol_version,
            extra_parameters={
                "idle_heartbeat_interval": self.idle_heartbeat_interval,
                "idle_heartbeat_timeout": self.idle_heartbeat_timeout,
                "control_connection_timeout": 100,
            },
        )
        partitioning_config = config.ApdbCassandraPartitioningConfig(
            part_pixelization=self.part_pixelization,
            part_pix_level=self.part_pix_level,
            part_pix_max_ranges=self.part_pix_max_ranges,
            time_partition_tables=self.time_partition_tables,
            time_partition_days=self.time_partition_days,
            time_partition_start=self.time_partition_start,
            time_partition_end=self.time_partition_end,
            query_per_time_part=self.query_per_time_part,
            query_per_spatial_part=self.query_per_spatial_part,
        )
        new_config = config.ApdbCassandraConfig(
            schema_file=self.schema_file,
            schema_name=self.schema_name,
            read_sources_months=self.read_sources_months,
            read_forced_sources_months=self.read_forced_sources_months,
            enable_replica=self.use_insert_id,
            replica_chunk_seconds=self.replica_chunk_seconds,
            contact_points=list(self.contact_points),
            keyspace=self.keyspace,
            connection_config=connection_config,
            partitioning=partitioning_config,
            dia_object_columns=list(self.dia_object_columns),
            prefix=self.prefix,
            ra_dec_columns=list(self.ra_dec_columns),
            replica_skips_diaobjects=self.use_insert_id_skips_diaobjects,
        )
        return new_config
