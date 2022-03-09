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

__all__ = ["ApdbCassandraConfig", "ApdbCassandra"]

from datetime import datetime, timedelta
import logging
import numpy as np
import pandas
from typing import cast, Any, Dict, Callable, Iterable, List, Mapping, Optional, Set, Tuple, Union

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    import cassandra
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.concurrent import execute_concurrent
    from cassandra.policies import RoundRobinPolicy, WhiteListRoundRobinPolicy, AddressTranslator
    import cassandra.query
    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

import lsst.daf.base as dafBase
from lsst.pex.config import ChoiceField, Field, ListField
from lsst import sphgeom
from .timer import Timer
from .apdb import Apdb, ApdbConfig
from .apdbSchema import ApdbTables, TableDef
from .apdbCassandraSchema import ApdbCassandraSchema


_LOG = logging.getLogger(__name__)


class CassandraMissingError(Exception):
    def __init__(self) -> None:
        super().__init__("cassandra-driver module cannot be imported")


class ApdbCassandraConfig(ApdbConfig):

    contact_points = ListField(
        dtype=str,
        doc="The list of contact points to try connecting for cluster discovery.",
        default=["127.0.0.1"]
    )
    private_ips = ListField(
        dtype=str,
        doc="List of internal IP addresses for contact_points.",
        default=[]
    )
    keyspace = Field(
        dtype=str,
        doc="Default keyspace for operations.",
        default="apdb"
    )
    read_consistency = Field(
        dtype=str,
        doc="Name for consistency level of read operations, default: QUORUM, can be ONE.",
        default="QUORUM"
    )
    write_consistency = Field(
        dtype=str,
        doc="Name for consistency level of write operations, default: QUORUM, can be ONE.",
        default="QUORUM"
    )
    read_timeout = Field(
        dtype=float,
        doc="Timeout in seconds for read operations.",
        default=120.
    )
    write_timeout = Field(
        dtype=float,
        doc="Timeout in seconds for write operations.",
        default=10.
    )
    read_concurrency = Field(
        dtype=int,
        doc="Concurrency level for read operations.",
        default=500
    )
    protocol_version = Field(
        dtype=int,
        doc="Cassandra protocol version to use, default is V4",
        default=cassandra.ProtocolVersion.V4 if CASSANDRA_IMPORTED else 0
    )
    dia_object_columns = ListField(
        dtype=str,
        doc="List of columns to read from DiaObject, by default read all columns",
        default=[]
    )
    prefix = Field(
        dtype=str,
        doc="Prefix to add to table names",
        default=""
    )
    part_pixelization = ChoiceField(
        dtype=str,
        allowed=dict(htm="HTM pixelization", q3c="Q3C pixelization", mq3c="MQ3C pixelization"),
        doc="Pixelization used for partitioning index.",
        default="mq3c"
    )
    part_pix_level = Field(
        dtype=int,
        doc="Pixelization level used for partitioning index.",
        default=10
    )
    part_pix_max_ranges = Field(
        dtype=int,
        doc="Max number of ranges in pixelization envelope",
        default=64
    )
    ra_dec_columns = ListField(
        dtype=str,
        default=["ra", "decl"],
        doc="Names ra/dec columns in DiaObject table"
    )
    timer = Field(
        dtype=bool,
        doc="If True then print/log timing information",
        default=False
    )
    time_partition_tables = Field(
        dtype=bool,
        doc="Use per-partition tables for sources instead of partitioning by time",
        default=True
    )
    time_partition_days = Field(
        dtype=int,
        doc="Time partitoning granularity in days, this value must not be changed"
            " after database is initialized",
        default=30
    )
    time_partition_start = Field(
        dtype=str,
        doc="Starting time for per-partion tables, in yyyy-mm-ddThh:mm:ss format, in TAI."
            " This is used only when time_partition_tables is True.",
        default="2018-12-01T00:00:00"
    )
    time_partition_end = Field(
        dtype=str,
        doc="Ending time for per-partion tables, in yyyy-mm-ddThh:mm:ss format, in TAI"
            " This is used only when time_partition_tables is True.",
        default="2030-01-01T00:00:00"
    )
    query_per_time_part = Field(
        dtype=bool,
        default=False,
        doc="If True then build separate query for each time partition, otherwise build one single query. "
            "This is only used when time_partition_tables is False in schema config."
    )
    query_per_spatial_part = Field(
        dtype=bool,
        default=False,
        doc="If True then build one query per spacial partition, otherwise build single query. "
    )
    pandas_delay_conv = Field(
        dtype=bool,
        default=True,
        doc="If True then combine result rows before converting to pandas. "
    )
    prepared_statements = Field(
        dtype=bool,
        default=True,
        doc="If True use Cassandra prepared statements."
    )


class Partitioner:
    """Class that calculates indices of the objects for partitioning.

    Used internally by `ApdbCassandra`

    Parameters
    ----------
    config : `ApdbCassandraConfig`
    """
    def __init__(self, config: ApdbCassandraConfig):
        pix = config.part_pixelization
        if pix == "htm":
            self.pixelator = sphgeom.HtmPixelization(config.part_pix_level)
        elif pix == "q3c":
            self.pixelator = sphgeom.Q3cPixelization(config.part_pix_level)
        elif pix == "mq3c":
            self.pixelator = sphgeom.Mq3cPixelization(config.part_pix_level)
        else:
            raise ValueError(f"unknown pixelization: {pix}")
        self.part_pix_max_ranges = config.part_pix_max_ranges

    def pixels(self, region: sphgeom.Region) -> List[int]:
        """Compute set of the pixel indices for given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
        """
        # we want finest set of pixels, so ask as many pixel as possible
        ranges = self.pixelator.envelope(region, 1_000_000)
        indices = []
        for lower, upper in ranges:
            indices += list(range(lower, upper))
        return indices

    def pixel(self, direction: sphgeom.UnitVector3d) -> int:
        """Compute the index of the pixel for given direction.

        Parameters
        ----------
        direction : `lsst.sphgeom.UnitVector3d`
        """
        index = self.pixelator.index(direction)
        return index

    def envelope(self, region: sphgeom.Region) -> List[Tuple[int, int]]:
        """Generate a set of HTM indices covering specified region.

        Parameters
        ----------
        region: `sphgeom.Region`
            Region that needs to be indexed.

        Returns
        -------
        ranges : `list` of `tuple`
            Sequence of ranges, range is a tuple (minHtmID, maxHtmID).
        """
        _LOG.debug('region: %s', region)
        indices = self.pixelator.envelope(region, self.part_pix_max_ranges)

        if _LOG.isEnabledFor(logging.DEBUG):
            for irange in indices.ranges():
                _LOG.debug('range: %s %s', self.pixelator.toString(irange[0]),
                           self.pixelator.toString(irange[1]))

        return indices.ranges()


if CASSANDRA_IMPORTED:

    class _AddressTranslator(AddressTranslator):
        """Translate internal IP address to external.

        Only used for docker-based setup, not viable long-term solution.
        """
        def __init__(self, public_ips: List[str], private_ips: List[str]):
            self._map = dict((k, v) for k, v in zip(private_ips, public_ips))

        def translate(self, private_ip: str) -> str:
            return self._map.get(private_ip, private_ip)


def _rows_to_pandas(colnames: List[str], rows: List[Tuple]) -> pandas.DataFrame:
    """Convert result rows to pandas.

    Unpacks BLOBs that were packed on insert.

    Parameters
    ----------
    colname : `list` [ `str` ]
        Names of the columns.
    rows : `list` of `tuple`
        Result rows.

    Returns
    -------
    catalog : `pandas.DataFrame`
        DataFrame with the result set.
    """
    return pandas.DataFrame.from_records(rows, columns=colnames)


class _PandasRowFactory:
    """Create pandas DataFrame from Cassandra result set.
    """

    def __call__(self, colnames: List[str], rows: List[Tuple]) -> pandas.DataFrame:
        """Convert result set into output catalog.

        Parameters
        ----------
        colname : `list` [ `str` ]
            Names of the columns.
        rows : `list` of `tuple`
            Result rows

        Returns
        -------
        catalog : `pandas.DataFrame`
            DataFrame with the result set.
        """
        return _rows_to_pandas(colnames, rows)


class _RawRowFactory:
    """Row factory that makes no conversions.
    """

    def __call__(self, colnames: List[str], rows: List[Tuple]) -> Tuple[List[str], List[Tuple]]:
        """Return parameters without change.

        Parameters
        ----------
        colname : `list` of `str`
            Names of the columns.
        rows : `list` of `tuple`
            Result rows

        Returns
        -------
        colname : `list` of `str`
            Names of the columns.
        rows : `list` of `tuple`
            Result rows
        """
        return (colnames, rows)


class ApdbCassandra(Apdb):
    """Implementation of APDB database on to of Apache Cassandra.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbCassandraConfig` configuration class. For an example of
    different configurations check config/ folder.

    Parameters
    ----------
    config : `ApdbCassandraConfig`
        Configuration object.
    """

    partition_zero_epoch = dafBase.DateTime(1970, 1, 1, 0, 0, 0, dafBase.DateTime.TAI)
    """Start time for partition 0, this should never be changed."""

    def __init__(self, config: ApdbCassandraConfig):

        if not CASSANDRA_IMPORTED:
            raise CassandraMissingError()

        self.config = config

        _LOG.debug("ApdbCassandra Configuration:")
        for key, value in self.config.items():
            _LOG.debug("    %s: %s", key, value)

        self._partitioner = Partitioner(config)

        addressTranslator: Optional[AddressTranslator] = None
        if config.private_ips:
            loadBalancePolicy = WhiteListRoundRobinPolicy(hosts=config.contact_points)
            addressTranslator = _AddressTranslator(config.contact_points, config.private_ips)
        else:
            loadBalancePolicy = RoundRobinPolicy()

        self._keyspace = config.keyspace

        src_row_factory: Callable
        if self.config.pandas_delay_conv:
            src_row_factory = _RawRowFactory()
        else:
            src_row_factory = _PandasRowFactory()

        read_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=self.config.read_timeout,
            row_factory=_PandasRowFactory(),
            load_balancing_policy=loadBalancePolicy,
        )
        read_src_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=self.config.read_timeout,
            row_factory=src_row_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        write_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.write_consistency),
            request_timeout=self.config.write_timeout,
            load_balancing_policy=loadBalancePolicy,
        )
        # Also set default profile to be the same as read profile for sources
        # because execute_concurrent() (which is used for reading sources)
        # does not accept non-default profiles.
        profiles = {
            'read': read_profile,
            'read_src': read_src_profile,
            'write': write_profile,
            EXEC_PROFILE_DEFAULT: read_src_profile,
        }

        self._cluster = Cluster(execution_profiles=profiles,
                                contact_points=self.config.contact_points,
                                address_translator=addressTranslator,
                                protocol_version=self.config.protocol_version)
        self._session = self._cluster.connect()
        # Disable result paging
        self._session.default_fetch_size = None

        self._schema = ApdbCassandraSchema(session=self._session,
                                           keyspace=self._keyspace,
                                           schema_file=self.config.schema_file,
                                           extra_schema_file=self.config.extra_schema_file,
                                           prefix=self.config.prefix,
                                           time_partition_tables=self.config.time_partition_tables)
        self._partition_zero_epoch_mjd = self.partition_zero_epoch.get(system=dafBase.DateTime.MJD)

    def tableDef(self, table: ApdbTables) -> Optional[TableDef]:
        # docstring is inherited from a base class
        return self._schema.tableSchemas.get(table)

    def makeSchema(self, drop: bool = False) -> None:
        # docstring is inherited from a base class

        if self.config.time_partition_tables:
            time_partition_start = dafBase.DateTime(self.config.time_partition_start, dafBase.DateTime.TAI)
            time_partition_end = dafBase.DateTime(self.config.time_partition_end, dafBase.DateTime.TAI)
            part_range = (
                self._time_partition(time_partition_start),
                self._time_partition(time_partition_end) + 1
            )
            self._schema.makeSchema(drop=drop, part_range=part_range)
        else:
            self._schema.makeSchema(drop=drop)

    def getDiaObjects(self, region: sphgeom.Region) -> pandas.DataFrame:
        # docstring is inherited from a base class

        spatial_where = self._spatial_where(region)
        _LOG.debug("getDiaObjects: #partitions: %s", len(spatial_where))

        query = f'SELECT * from "{self._keyspace}"."DiaObjectLast"'
        statements: List[Tuple] = [
            (cassandra.query.SimpleStatement(f'{query} WHERE {where}'), {}) for where in spatial_where
        ]
        _LOG.debug("getDiaObjects: #queries: %s", len(statements))
        # _LOG.debug("getDiaObjects: queries: %s", queries)

        with Timer('DiaObject select', self.config.timer):
            objects = self._run_queries(statements)

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

    def getDiaSources(self, region: sphgeom.Region,
                      object_ids: Optional[Iterable[int]],
                      visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        # docstring is inherited from a base class
        months = self.config.read_sources_months
        if months == 0:
            return None
        mjd_end = visit_time.get(system=dafBase.DateTime.MJD)
        mjd_start = mjd_end - months*30

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaSource)

    def getDiaForcedSources(self, region: sphgeom.Region,
                            object_ids: Optional[Iterable[int]],
                            visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        # docstring is inherited from a base class
        months = self.config.read_forced_sources_months
        if months == 0:
            return None
        mjd_end = visit_time.get(system=dafBase.DateTime.MJD)
        mjd_start = mjd_end - months*30

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaForcedSource)

    def _getSources(self, region: sphgeom.Region,
                    object_ids: Optional[Iterable[int]],
                    mjd_start: float,
                    mjd_end: float,
                    table_name: ApdbTables) -> Optional[pandas.DataFrame]:
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
        object_ids :
            Collection of DiaObject IDs
        mjd_start : `float`
            Lower bound of time interval.
        mjd_end : `float`
            Upper bound of time interval.
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog contaning DiaSource records. `None` is returned if
            ``months`` is 0 or when ``object_ids`` is empty.
        """
        object_id_set: Set[int] = set()
        if object_ids is not None:
            object_id_set = set(object_ids)
            if len(object_id_set) == 0:
                return self._make_empty_catalog(table_name)

        # spatial pixels included into query
        pixels = self._partitioner.pixels(region)
        _LOG.debug("_getSources: %s #partitions: %s", table_name.name, len(pixels))

        # spatial part of WHERE
        spatial_where = []
        if self.config.query_per_spatial_part:
            spatial_where = [f'"apdb_part" = {pixel}' for pixel in pixels]
        else:
            pixels_str = ",".join([str(pix) for pix in pixels])
            spatial_where = [f'"apdb_part" IN ({pixels_str})']

        # temporal part of WHERE, can be empty
        temporal_where = []
        # time partitions and table names to query, there may be multiple
        # tables depending on configuration
        full_name = self._schema.tableName(table_name)
        tables = [full_name]
        time_part_start = self._time_partition(mjd_start)
        time_part_end = self._time_partition(mjd_end)
        time_parts = list(range(time_part_start, time_part_end + 1))
        if self.config.time_partition_tables:
            tables = [f"{full_name}_{part}" for part in time_parts]
        else:
            if self.config.query_per_time_part:
                temporal_where = [f'"apdb_time_part" = {time_part}' for time_part in time_parts]
            else:
                time_part_list = ",".join([str(part) for part in time_parts])
                temporal_where = [f'"apdb_time_part" IN ({time_part_list})']

        # Build all queries
        queries: List[str] = []
        for table in tables:
            query = f'SELECT * from "{self._keyspace}"."{table}" WHERE '
            for spacial in spatial_where:
                if temporal_where:
                    for temporal in temporal_where:
                        queries.append(query + spacial + " AND " + temporal)
                else:
                    queries.append(query + spacial)
        # _LOG.debug("_getSources: queries: %s", queries)

        statements: List[Tuple] = [
            (cassandra.query.SimpleStatement(query), {})
            for query in queries
        ]
        _LOG.debug("_getSources %s: #queries: %s", table_name, len(statements))

        with Timer(table_name.name + ' select', self.config.timer):
            catalog = self._run_queries(statements)

        # filter by given object IDs
        if len(object_id_set) > 0:
            catalog = cast(pandas.DataFrame, catalog[catalog["diaObjectId"].isin(object_id_set)])

        # precise filtering on midPointTai
        catalog = cast(pandas.DataFrame, catalog[catalog["midPointTai"] > mjd_start])

        _LOG.debug("found %d %ss", catalog.shape[0], table_name.name)
        return catalog

    def getDiaObjectsHistory(self,
                             start_time: dafBase.DateTime,
                             end_time: Optional[dafBase.DateTime] = None,
                             region: Optional[sphgeom.Region] = None) -> pandas.DataFrame:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def getDiaSourcesHistory(self,
                             start_time: dafBase.DateTime,
                             end_time: Optional[dafBase.DateTime] = None,
                             region: Optional[sphgeom.Region] = None) -> pandas.DataFrame:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def getDiaForcedSourcesHistory(self,
                                   start_time: dafBase.DateTime,
                                   end_time: Optional[dafBase.DateTime] = None,
                                   region: Optional[sphgeom.Region] = None) -> pandas.DataFrame:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def getSSObjects(self) -> pandas.DataFrame:
        # docstring is inherited from a base class
        tableName = self._schema.tableName(ApdbTables.SSObject)
        query = f'SELECT * from "{self._keyspace}"."{tableName}"'

        objects = None
        with Timer('SSObject select', self.config.timer):
            result = self._session.execute(query, execution_profile="read")
            objects = result._current_rows

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

    def store(self,
              visit_time: dafBase.DateTime,
              objects: pandas.DataFrame,
              sources: Optional[pandas.DataFrame] = None,
              forced_sources: Optional[pandas.DataFrame] = None) -> None:
        # docstring is inherited from a base class

        # fill region partition column for DiaObjects
        objects = self._add_obj_part(objects)
        self._storeDiaObjects(objects, visit_time)

        if sources is not None:
            # copy apdb_part column from DiaObjects to DiaSources
            sources = self._add_src_part(sources, objects)
            self._storeDiaSources(ApdbTables.DiaSource, sources, visit_time)

        if forced_sources is not None:
            forced_sources = self._add_fsrc_part(forced_sources, objects)
            self._storeDiaSources(ApdbTables.DiaForcedSource, forced_sources, visit_time)

    def _storeDiaObjects(self, objs: pandas.DataFrame, visit_time: dafBase.DateTime) -> None:
        """Store catalog of DiaObjects from current visit.

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        visit_time_dt = visit_time.toPython()
        extra_columns = dict(lastNonForcedSource=visit_time_dt)
        self._storeObjectsPandas(objs, ApdbTables.DiaObjectLast, extra_columns=extra_columns)

        extra_columns["validityStart"] = visit_time_dt
        time_part: Optional[int] = self._time_partition(visit_time)
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(objs, ApdbTables.DiaObject, extra_columns=extra_columns, time_part=time_part)

    def _storeDiaSources(self, table_name: ApdbTables, sources: pandas.DataFrame,
                         visit_time: dafBase.DateTime) -> None:
        """Store catalog of DIASources or DIAForcedSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        time_part: Optional[int] = self._time_partition(visit_time)
        extra_columns = {}
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(sources, table_name, extra_columns=extra_columns, time_part=time_part)

    def storeSSObjects(self, objects: pandas.DataFrame) -> None:
        # docstring is inherited from a base class
        self._storeObjectsPandas(objects, ApdbTables.SSObject)

    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def dailyJob(self) -> None:
        # docstring is inherited from a base class
        pass

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def _spatial_where(self, region: Optional[sphgeom.Region], use_ranges: bool = False) -> List[str]:
        """Generate expressions for spatial part of WHERE clause.

        Parameters
        ----------
        region : `sphgeom.Region`
            Spatial region for query results.
        use_ranges : `bool`
            If True then use pixel ranges ("apdb_part >= p1 AND apdb_part <=
            p2") instead of exact list of pixels. Should be set to True for
            large regions covering very many pixels.

        Returns
        -------
        expressions : `list` [ `str` ]
            Empty list is returned if ``region`` is `None`, otherwise a list
            of one or more expressions.
        """
        if region is None:
            return []
        if use_ranges:
            pixel_ranges = self._partitioner.envelope(region)
            expressions = []
            for lower, upper in pixel_ranges:
                upper -= 1
                if lower == upper:
                    expressions.append(f'"apdb_part" = {lower}')
                else:
                    expressions.append(f'"apdb_part" >= {lower} AND "apdb_part" <= {upper}')
            return expressions
        else:
            pixels = self._partitioner.pixels(region)
            if self.config.query_per_spatial_part:
                return [f'"apdb_part" = {pixel}' for pixel in pixels]
            else:
                pixels_str = ",".join([str(pix) for pix in pixels])
                return [f'"apdb_part" IN ({pixels_str})']

    def _run_queries(self, statements: List[Tuple]) -> pandas.DataFrame:
        """Execute bunch of queries concurrently and merge their results into
        a single DataFrame."""
        results = execute_concurrent(
            self._session, statements, results_generator=True, raise_on_first_error=False,
            concurrency=self.config.read_concurrency
        )
        if self.config.pandas_delay_conv:
            _LOG.debug("making pandas data frame out of rows/columns")
            columns: Any = None
            rows = []
            for success, result in results:
                result = result._current_rows
                if success:
                    if columns is None:
                        columns = result[0]
                    elif columns != result[0]:
                        _LOG.error("different columns returned by queries: %s and %s", columns, result[0])
                        raise ValueError(
                            f"different columns returned by queries: {columns} and {result[0]}"
                        )
                    rows += result[1]
                else:
                    _LOG.error("error returned by query: %s", result)
                    raise result
            catalog = _rows_to_pandas(columns, rows)
        else:
            _LOG.debug("making pandas data frame out of set of data frames")
            dataframes = []
            for success, result in results:
                if success:
                    dataframes.append(result._current_rows)
                else:
                    _LOG.error("error returned by query: %s", result)
                    raise result
            # concatenate all frames
            if len(dataframes) == 1:
                catalog = dataframes[0]
            else:
                catalog = pandas.concat(dataframes)

        _LOG.debug("pandas catalog shape: %s", catalog.shape)
        return catalog

    def _storeObjectsPandas(self, objects: pandas.DataFrame, table_name: ApdbTables,
                            extra_columns: Optional[Mapping] = None,
                            time_part: Optional[int] = None) -> None:
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `pandas.DataFrame`
            Catalog containing object records
        table_name : `ApdbTables`
            Name of the table as defined in APDB schema.
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        time_part : `int`, optional
            If not `None` then insert into a per-partition table.
        """

        def qValue(v: Any) -> Any:
            """Transform object into a value for query"""
            if v is None:
                pass
            elif isinstance(v, datetime):
                v = int((v - datetime(1970, 1, 1)) / timedelta(seconds=1))*1000
            elif isinstance(v, (bytes, str)):
                pass
            else:
                try:
                    if not np.isfinite(v):
                        v = None
                except TypeError:
                    pass
            return v

        def quoteId(columnName: str) -> str:
            """Smart quoting for column names.
            Lower-case names are not quoted.
            """
            if not columnName.islower():
                columnName = '"' + columnName + '"'
            return columnName

        # use extra columns if specified
        if extra_columns is None:
            extra_columns = {}
        extra_fields = list(extra_columns.keys())

        df_fields = [column for column in objects.columns
                     if column not in extra_fields]

        column_map = self._schema.getColumnMap(table_name)
        # list of columns (as in cat schema)
        fields = [column_map[field].name for field in df_fields if field in column_map]
        fields += extra_fields

        # check that all partitioning and clustering columns are defined
        required_columns = self._schema.partitionColumns(table_name) \
            + self._schema.clusteringColumns(table_name)
        missing_columns = [column for column in required_columns if column not in fields]
        if missing_columns:
            raise ValueError(f"Primary key columns are missing from catalog: {missing_columns}")

        qfields = [quoteId(field) for field in fields]
        qfields_str = ','.join(qfields)

        with Timer(table_name.name + ' query build', self.config.timer):

            table = self._schema.tableName(table_name)
            if time_part is not None:
                table = f"{table}_{time_part}"

            prepared: Optional[cassandra.query.PreparedStatement] = None
            if self.config.prepared_statements:
                holders = ','.join(['?']*len(qfields))
                query = f'INSERT INTO "{self._keyspace}"."{table}" ({qfields_str}) VALUES ({holders})'
                prepared = self._session.prepare(query)
            queries = cassandra.query.BatchStatement()
            for rec in objects.itertuples(index=False):
                values = []
                for field in df_fields:
                    if field not in column_map:
                        continue
                    value = getattr(rec, field)
                    if column_map[field].type == "DATETIME":
                        if isinstance(value, pandas.Timestamp):
                            value = qValue(value.to_pydatetime())
                        else:
                            # Assume it's seconds since epoch, Cassandra
                            # datetime is in milliseconds
                            value = int(value*1000)
                    values.append(qValue(value))
                for field in extra_fields:
                    value = extra_columns[field]
                    values.append(qValue(value))
                holders = ','.join(['%s']*len(values))
                if prepared is not None:
                    stmt = prepared
                else:
                    query = f'INSERT INTO "{self._keyspace}"."{table}" ({qfields_str}) VALUES ({holders})'
                    # _LOG.debug("query: %r", query)
                    # _LOG.debug("values: %s", values)
                    stmt = cassandra.query.SimpleStatement(query)
                queries.add(stmt, values)

        # _LOG.debug("query: %s", query)
        _LOG.debug("%s: will store %d records", self._schema.tableName(table_name), objects.shape[0])
        with Timer(table_name.name + ' insert', self.config.timer):
            self._session.execute(queries, timeout=self.config.write_timeout, execution_profile="write")

    def _add_obj_part(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate spacial partition for each record and add it to a
        DataFrame.

        Notes
        -----
        This overrides any existing column in a DataFrame with the same name
        (apdb_part). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        # calculate HTM index for every DiaObject
        apdb_part = np.zeros(df.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (ra, dec) in enumerate(zip(df[ra_col], df[dec_col])):
            uv3d = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(ra, dec))
            idx = self._partitioner.pixel(uv3d)
            apdb_part[i] = idx
        df = df.copy()
        df["apdb_part"] = apdb_part
        return df

    def _add_src_part(self, sources: pandas.DataFrame, objs: pandas.DataFrame) -> pandas.DataFrame:
        """Add apdb_part column to DiaSource catalog.

        Notes
        -----
        This method copies apdb_part value from a matching DiaObject record.
        DiaObject catalog needs to have a apdb_part column filled by
        ``_add_obj_part`` method and DiaSource records need to be
        associated to DiaObjects via ``diaObjectId`` column.

        This overrides any existing column in a DataFrame with the same name
        (apdb_part). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        pixel_id_map: Dict[int, int] = {
            diaObjectId: apdb_part for diaObjectId, apdb_part
            in zip(objs["diaObjectId"], objs["apdb_part"])
        }
        apdb_part = np.zeros(sources.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (diaObjId, ra, dec) in enumerate(zip(sources["diaObjectId"],
                                                    sources[ra_col], sources[dec_col])):
            if diaObjId == 0:
                # DiaSources associated with SolarSystemObjects do not have an
                # associated DiaObject hence we skip them and set partition
                # based on its own ra/dec
                uv3d = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(ra, dec))
                idx = self._partitioner.pixel(uv3d)
                apdb_part[i] = idx
            else:
                apdb_part[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources["apdb_part"] = apdb_part
        return sources

    def _add_fsrc_part(self, sources: pandas.DataFrame, objs: pandas.DataFrame) -> pandas.DataFrame:
        """Add apdb_part column to DiaForcedSource catalog.

        Notes
        -----
        This method copies apdb_part value from a matching DiaObject record.
        DiaObject catalog needs to have a apdb_part column filled by
        ``_add_obj_part`` method and DiaSource records need to be
        associated to DiaObjects via ``diaObjectId`` column.

        This overrides any existing column in a DataFrame with the same name
        (apdb_part). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        pixel_id_map: Dict[int, int] = {
            diaObjectId: apdb_part for diaObjectId, apdb_part
            in zip(objs["diaObjectId"], objs["apdb_part"])
        }
        apdb_part = np.zeros(sources.shape[0], dtype=np.int64)
        for i, diaObjId in enumerate(sources["diaObjectId"]):
            apdb_part[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources["apdb_part"] = apdb_part
        return sources

    def _time_partition(self, time: Union[float, dafBase.DateTime]) -> int:
        """Calculate time partiton number for a given time.

        Parameters
        ----------
        time : `float` or `lsst.daf.base.DateTime`
            Time for which to calculate partition number. Can be float to mean
            MJD or `lsst.daf.base.DateTime`

        Returns
        -------
        partition : `int`
            Partition number for a given time.
        """
        if isinstance(time, dafBase.DateTime):
            mjd = time.get(system=dafBase.DateTime.MJD)
        else:
            mjd = time
        days_since_epoch = mjd - self._partition_zero_epoch_mjd
        partition = int(days_since_epoch) // self.config.time_partition_days
        return partition

    def _make_empty_catalog(self, table_name: ApdbTables) -> pandas.DataFrame:
        """Make an empty catalog for a table with a given name.

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        catalog : `pandas.DataFrame`
            An empty catalog.
        """
        table = self._schema.tableSchemas[table_name]

        data = {columnDef.name: pandas.Series(dtype=columnDef.dtype) for columnDef in table.columns}
        return pandas.DataFrame(data)
