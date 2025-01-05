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

__all__ = ["ApdbCassandraReplica"]

import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, cast

import astropy.time
from lsst.utils.iteration import chunk_iterable

from ..apdbReplica import ApdbReplica, ApdbTableData, ReplicaChunk
from ..monitor import MonAgent
from ..timer import Timer
from ..versionTuple import VersionTuple
from .apdbCassandraSchema import ApdbCassandraSchema, ExtraTables
from .cassandra_utils import ApdbCassandraTableData, PreparedStatementCache

if TYPE_CHECKING:
    from .apdbCassandra import ApdbCassandra

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)

VERSION = VersionTuple(1, 0, 0)
"""Version for the code controlling replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""


class ApdbCassandraReplica(ApdbReplica):
    """Implementation of `ApdbReplica` for Cassandra backend.

    Parameters
    ----------
    apdb : `ApdbCassandra`
        Instance of ApbdCassandra for database.
    schema : `ApdbCassandraSchema`
        Instance of ApdbCassandraSchema for database.
    session
        Instance of cassandra session type.
    """

    def __init__(self, apdb: ApdbCassandra, schema: ApdbCassandraSchema, session: Any):
        # Note that ApdbCassandra instance must stay alive while this object
        # exists, so we keep reference to it.
        self._apdb = apdb
        self._schema = schema
        self._session = session
        self._config = apdb.config

        # Cache for prepared statements
        self._preparer = PreparedStatementCache(self._session)

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, tags=tags)

    @classmethod
    def apdbReplicaImplementationVersion(cls) -> VersionTuple:
        # Docstring inherited from base class.
        return VERSION

    def getReplicaChunks(self) -> list[ReplicaChunk] | None:
        # docstring is inherited from a base class
        if not self._schema.has_replica_chunks:
            return None

        # everything goes into a single partition
        partition = 0

        table_name = self._schema.tableName(ExtraTables.ApdbReplicaChunks)
        # We want to avoid timezone mess so return timestamps as milliseconds.
        query = (
            "SELECT toUnixTimestamp(last_update_time), apdb_replica_chunk, unique_id "
            f'FROM "{self._config.keyspace}"."{table_name}" WHERE partition = ?'
        )

        with self._timer("chunks_select_time") as timer:
            result = self._session.execute(
                self._preparer.prepare(query),
                (partition,),
                timeout=self._config.connection_config.read_timeout,
                execution_profile="read_tuples",
            )
            # order by last_update_time
            rows = sorted(result)
            timer.add_values(row_count=len(rows))
        return [
            ReplicaChunk(
                id=row[1],
                last_update_time=astropy.time.Time(row[0] / 1000, format="unix_tai"),
                unique_id=row[2],
            )
            for row in rows
        ]

    def deleteReplicaChunks(self, chunks: Iterable[int]) -> None:
        # docstring is inherited from a base class
        if not self._schema.has_replica_chunks:
            raise ValueError("APDB is not configured for replication")

        # There is 64k limit on number of markers in Cassandra CQL
        for chunk_ids in chunk_iterable(chunks, 20_000):
            chunk_list = list(chunk_ids)
            params = ",".join("?" * len(chunk_ids))

            # everything goes into a single partition
            partition = 0

            table_name = self._schema.tableName(ExtraTables.ApdbReplicaChunks)
            query = (
                f'DELETE FROM "{self._config.keyspace}"."{table_name}" '
                f"WHERE partition = ? AND apdb_replica_chunk IN ({params})"
            )

            with self._timer("chunks_delete_time") as timer:
                self._session.execute(
                    self._preparer.prepare(query),
                    [partition] + chunk_list,
                    timeout=self._config.connection_config.remove_timeout,
                )
                timer.add_values(row_count=len(chunk_list))

            # Also remove those chunk_ids from Dia*Chunks tables.
            for table in (
                ExtraTables.DiaObjectChunks,
                ExtraTables.DiaSourceChunks,
                ExtraTables.DiaForcedSourceChunks,
            ):
                table_name = self._schema.tableName(table)
                query = (
                    f'DELETE FROM "{self._config.keyspace}"."{table_name}"'
                    f" WHERE apdb_replica_chunk IN ({params})"
                )
                with self._timer("table_chunk_detele_time", tags={"table": table_name}) as timer:
                    self._session.execute(
                        self._preparer.prepare(query),
                        chunk_list,
                        timeout=self._config.connection_config.remove_timeout,
                    )
                    timer.add_values(row_count=len(chunk_list))

    def getDiaObjectsChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_chunks(ExtraTables.DiaObjectChunks, chunks)

    def getDiaSourcesChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_chunks(ExtraTables.DiaSourceChunks, chunks)

    def getDiaForcedSourcesChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_chunks(ExtraTables.DiaForcedSourceChunks, chunks)

    def _get_chunks(self, table: ExtraTables, chunks: Iterable[int]) -> ApdbTableData:
        """Return records from a particular table given set of insert IDs."""
        if not self._schema.has_replica_chunks:
            raise ValueError("APDB is not configured for replication")

        # We do not expect too may chunks in this query.
        chunks = list(chunks)
        params = ",".join("?" * len(chunks))

        table_name = self._schema.tableName(table)
        # I know that chunk table schema has only regular APDB columns plus
        # apdb_replica_chunk column, and this is exactly what we need to return
        # from this method, so selecting a star is fine here.
        query = (
            f'SELECT * FROM "{self._config.keyspace}"."{table_name}" WHERE apdb_replica_chunk IN ({params})'
        )
        statement = self._preparer.prepare(query)

        with self._timer("table_chunk_select_time", tags={"table": table_name}) as timer:
            result = self._session.execute(statement, chunks, execution_profile="read_raw")
            table_data = cast(ApdbCassandraTableData, result._current_rows)
            timer.add_values(row_count=len(table_data.rows()))
        return table_data
