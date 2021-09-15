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

"""Module defining Apdb class and related methods.
"""

__all__ = ["ApdbConfig", "Apdb"]

from contextlib import contextmanager
from datetime import datetime
import logging
import numpy as np
import os
import pandas

import lsst.geom as geom
import lsst.afw.table as afwTable
import lsst.pex.config as pexConfig
from lsst.pex.config import Field, ChoiceField, ListField
import sqlalchemy
from sqlalchemy import (func, sql)
from sqlalchemy.pool import NullPool
from . import timer, apdbSchema


_LOG = logging.getLogger(__name__)


class Timer(object):
    """Timer class defining context manager which tracks execution timing.

    Typical use:

        with Timer("timer_name"):
            do_something

    On exit from block it will print elapsed time.

    See also :py:mod:`timer` module.
    """
    def __init__(self, name, do_logging=True, log_before_cursor_execute=False):
        self._log_before_cursor_execute = log_before_cursor_execute
        self._do_logging = do_logging
        self._timer1 = timer.Timer(name)
        self._timer2 = timer.Timer(name + " (before/after cursor)")

    def __enter__(self):
        """
        Enter context, start timer
        """
#         event.listen(engine.Engine, "before_cursor_execute", self._start_timer)
#         event.listen(engine.Engine, "after_cursor_execute", self._stop_timer)
        self._timer1.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit context, stop and dump timer
        """
        if exc_type is None:
            self._timer1.stop()
            if self._do_logging:
                self._timer1.dump()
#         event.remove(engine.Engine, "before_cursor_execute", self._start_timer)
#         event.remove(engine.Engine, "after_cursor_execute", self._stop_timer)
        return False

    def _start_timer(self, conn, cursor, statement, parameters, context, executemany):
        """Start counting"""
        if self._log_before_cursor_execute:
            _LOG.info("before_cursor_execute")
        self._timer2.start()

    def _stop_timer(self, conn, cursor, statement, parameters, context, executemany):
        """Stop counting"""
        self._timer2.stop()
        if self._do_logging:
            self._timer2.dump()


def _split(seq, nItems):
    """Split a sequence into smaller sequences"""
    seq = list(seq)
    while seq:
        yield seq[:nItems]
        del seq[:nItems]


def _coerce_uint64(df: pandas.DataFrame) -> pandas.DataFrame:
    """Change type of the uint64 columns to int64, return copy of data frame.
    """
    names = [c[0] for c in df.dtypes.items() if c[1] == np.uint64]
    return df.astype({name: np.int64 for name in names})


@contextmanager
def _ansi_session(engine):
    """Returns a connection, makes sure that ANSI mode is set for MySQL
    """
    with engine.begin() as conn:
        if engine.name == 'mysql':
            conn.execute(sql.text("SET SESSION SQL_MODE = 'ANSI'"))
        yield conn
    return


def _data_file_name(basename):
    """Return path name of a data file.
    """
    return os.path.join("${DAX_APDB_DIR}", "data", basename)


class ApdbConfig(pexConfig.Config):

    db_url = Field(dtype=str, doc="SQLAlchemy database connection URI")
    isolation_level = ChoiceField(dtype=str,
                                  doc="Transaction isolation level",
                                  allowed={"READ_COMMITTED": "Read committed",
                                           "READ_UNCOMMITTED": "Read uncommitted",
                                           "REPEATABLE_READ": "Repeatable read",
                                           "SERIALIZABLE": "Serializable"},
                                  default="READ_COMMITTED",
                                  optional=True)
    connection_pool = Field(dtype=bool,
                            doc=("If False then disable SQLAlchemy connection pool. "
                                 "Do not use connection pool when forking."),
                            default=True)
    connection_timeout = Field(dtype=float,
                               doc="Maximum time to wait time for database lock to be released before "
                                   "exiting. Defaults to sqlachemy defaults if not set.",
                               default=None,
                               optional=True)
    sql_echo = Field(dtype=bool,
                     doc="If True then pass SQLAlchemy echo option.",
                     default=False)
    dia_object_index = ChoiceField(dtype=str,
                                   doc="Indexing mode for DiaObject table",
                                   allowed={'baseline': "Index defined in baseline schema",
                                            'pix_id_iov': "(pixelId, objectId, iovStart) PK",
                                            'last_object_table': "Separate DiaObjectLast table"},
                                   default='baseline')
    dia_object_nightly = Field(dtype=bool,
                               doc="Use separate nightly table for DiaObject",
                               default=False)
    read_sources_months = Field(dtype=int,
                                doc="Number of months of history to read from DiaSource",
                                default=12)
    read_forced_sources_months = Field(dtype=int,
                                       doc="Number of months of history to read from DiaForcedSource",
                                       default=12)
    dia_object_columns = ListField(dtype=str,
                                   doc="List of columns to read from DiaObject, by default read all columns",
                                   default=[])
    object_last_replace = Field(dtype=bool,
                                doc="If True (default) then use \"upsert\" for DiaObjectsLast table",
                                default=True)
    schema_file = Field(dtype=str,
                        doc="Location of (YAML) configuration file with standard schema",
                        default=_data_file_name("apdb-schema.yaml"))
    extra_schema_file = Field(dtype=str,
                              doc="Location of (YAML) configuration file with extra schema",
                              default=_data_file_name("apdb-schema-extra.yaml"))
    column_map = Field(dtype=str,
                       doc="Location of (YAML) configuration file with column mapping",
                       default=_data_file_name("apdb-afw-map.yaml"))
    prefix = Field(dtype=str,
                   doc="Prefix to add to table names and index names",
                   default="")
    explain = Field(dtype=bool,
                    doc="If True then run EXPLAIN SQL command on each executed query",
                    default=False)
    timer = Field(dtype=bool,
                  doc="If True then print/log timing information",
                  default=False)
    diaobject_index_hint = Field(dtype=str,
                                 doc="Name of the index to use with Oracle index hint",
                                 default=None,
                                 optional=True)
    dynamic_sampling_hint = Field(dtype=int,
                                  doc="If non-zero then use dynamic_sampling hint",
                                  default=0)
    cardinality_hint = Field(dtype=int,
                             doc="If non-zero then use cardinality hint",
                             default=0)

    def validate(self):
        super().validate()
        if self.isolation_level == "READ_COMMITTED" and self.db_url.startswith("sqlite"):
            raise ValueError("Attempting to run Apdb with SQLITE and isolation level 'READ_COMMITTED.' "
                             "Use 'READ_UNCOMMITTED' instead.")


class Apdb(object):
    """Interface to L1 database, hides all database access details.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbConfig` configuration class. For an example of different
    configurations check config/ folder.

    Parameters
    ----------
    config : `ApdbConfig`
    afw_schemas : `dict`, optional
        Dictionary with table name for a key and `afw.table.Schema`
        for a value. Columns in schema will be added to standard
        APDB schema.
    """

    def __init__(self, config, afw_schemas=None):

        self.config = config

        # logging.getLogger('sqlalchemy').setLevel(logging.INFO)
        _LOG.debug("APDB Configuration:")
        _LOG.debug("    dia_object_index: %s", self.config.dia_object_index)
        _LOG.debug("    dia_object_nightly: %s", self.config.dia_object_nightly)
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    object_last_replace: %s", self.config.object_last_replace)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    column_map: %s", self.config.column_map)
        _LOG.debug("    schema prefix: %s", self.config.prefix)

        # engine is reused between multiple processes, make sure that we don't
        # share connections by disabling pool (by using NullPool class)
        kw = dict(echo=self.config.sql_echo)
        conn_args = dict()
        if not self.config.connection_pool:
            kw.update(poolclass=NullPool)
        if self.config.isolation_level is not None:
            kw.update(isolation_level=self.config.isolation_level)
        if self.config.connection_timeout is not None:
            if self.config.db_url.startswith("sqlite"):
                conn_args.update(timeout=self.config.connection_timeout)
            elif self.config.db_url.startswith(("postgresql", "mysql")):
                conn_args.update(connect_timeout=self.config.connection_timeout)
        kw.update(connect_args=conn_args)
        self._engine = sqlalchemy.create_engine(self.config.db_url, **kw)

        self._schema = apdbSchema.ApdbSchema(engine=self._engine,
                                             dia_object_index=self.config.dia_object_index,
                                             dia_object_nightly=self.config.dia_object_nightly,
                                             schema_file=self.config.schema_file,
                                             extra_schema_file=self.config.extra_schema_file,
                                             column_map=self.config.column_map,
                                             afw_schemas=afw_schemas,
                                             prefix=self.config.prefix)

    def tableRowCount(self):
        """Returns dictionary with the table names and row counts.

        Used by ``ap_proto`` to keep track of the size of the database tables.
        Depending on database technology this could be expensive operation.

        Returns
        -------
        row_counts : `dict`
            Dict where key is a table name and value is a row count.
        """
        res = {}
        tables = [self._schema.objects, self._schema.sources, self._schema.forcedSources]
        if self.config.dia_object_index == 'last_object_table':
            tables.append(self._schema.objects_last)
        for table in tables:
            stmt = sql.select([func.count()]).select_from(table)
            count = self._engine.scalar(stmt)
            res[table.name] = count

        return res

    def getDiaObjects(self, pixel_ranges, return_pandas=False):
        """Returns catalog of DiaObject instances from given region.

        Objects are searched based on pixelization index and region is
        determined by the set of indices. There is no assumption on a
        particular type of index, client is responsible for consistency
        when calculating pixelization indices.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types
        or units are not changed.

        Returns only the last version of each DiaObject.

        Parameters
        ----------
        pixel_ranges : `list` of `tuple`
            Sequence of ranges, range is a tuple (minPixelID, maxPixelID).
            This defines set of pixel indices to be included in result.
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog` or `pandas.DataFrame`
            Catalog containing DiaObject records.
        """

        # decide what columns we need
        if self.config.dia_object_index == 'last_object_table':
            table = self._schema.objects_last
        else:
            table = self._schema.objects
        if not self.config.dia_object_columns:
            query = table.select()
        else:
            columns = [table.c[col] for col in self.config.dia_object_columns]
            query = sql.select(columns)

        if self.config.diaobject_index_hint:
            val = self.config.diaobject_index_hint
            query = query.with_hint(table, 'index_rs_asc(%(name)s "{}")'.format(val))
        if self.config.dynamic_sampling_hint > 0:
            val = self.config.dynamic_sampling_hint
            query = query.with_hint(table, 'dynamic_sampling(%(name)s {})'.format(val))
        if self.config.cardinality_hint > 0:
            val = self.config.cardinality_hint
            query = query.with_hint(table, 'FIRST_ROWS_1 cardinality(%(name)s {})'.format(val))

        # build selection
        exprlist = []
        for low, upper in pixel_ranges:
            upper -= 1
            if low == upper:
                exprlist.append(table.c.pixelId == low)
            else:
                exprlist.append(sql.expression.between(table.c.pixelId, low, upper))
        query = query.where(sql.expression.or_(*exprlist))

        # select latest version of objects
        if self.config.dia_object_index != 'last_object_table':
            query = query.where(table.c.validityEnd == None)  # noqa: E711

        _LOG.debug("query: %s", query)

        if self.config.explain:
            # run the same query with explain
            self._explain(query, self._engine)

        # execute select
        with Timer('DiaObject select', self.config.timer):
            with self._engine.begin() as conn:
                if return_pandas:
                    objects = pandas.read_sql_query(query, conn)
                else:
                    res = conn.execute(query)
                    objects = self._convertResult(res, "DiaObject")
        _LOG.debug("found %s DiaObjects", len(objects))
        return objects

    def getDiaSourcesInRegion(self, pixel_ranges, dt, return_pandas=False):
        """Returns catalog of DiaSource instances from given region.

        Sources are searched based on pixelization index and region is
        determined by the set of indices. There is no assumption on a
        particular type of index, client is responsible for consistency
        when calculating pixelization indices.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        pixel_ranges : `list` of `tuple`
            Sequence of ranges, range is a tuple (minPixelID, maxPixelID).
            This defines set of pixel indices to be included in result.
        dt : `datetime.datetime`
            Time of the current visit
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`, `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0.
        """

        if self.config.read_sources_months == 0:
            _LOG.info("Skip DiaSources fetching")
            return None

        table = self._schema.sources
        query = table.select()

        # build selection
        exprlist = []
        for low, upper in pixel_ranges:
            upper -= 1
            if low == upper:
                exprlist.append(table.c.pixelId == low)
            else:
                exprlist.append(sql.expression.between(table.c.pixelId, low, upper))
        query = query.where(sql.expression.or_(*exprlist))

        # execute select
        with Timer('DiaSource select', self.config.timer):
            with _ansi_session(self._engine) as conn:
                if return_pandas:
                    sources = pandas.read_sql_query(query, conn)
                else:
                    res = conn.execute(query)
                    sources = self._convertResult(res, "DiaSource")
        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def getDiaSources(self, object_ids, dt, return_pandas=False):
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.


        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`, `pandas.DataFrame`, or `None`
            Catalog contaning DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """

        if self.config.read_sources_months == 0:
            _LOG.info("Skip DiaSources fetching")
            return None

        if len(object_ids) <= 0:
            _LOG.info("Skip DiaSources fetching - no Objects")
            # this should create a catalog, but the list of columns may be empty
            return None

        table = self._schema.sources
        sources = None
        with Timer('DiaSource select', self.config.timer):
            with _ansi_session(self._engine) as conn:
                for ids in _split(sorted(object_ids), 1000):
                    query = 'SELECT *  FROM "' + table.name + '" WHERE '

                    # select by object id
                    ids = ",".join(str(id) for id in ids)
                    query += '"diaObjectId" IN (' + ids + ') '

                    # execute select
                    if return_pandas:
                        df = pandas.read_sql_query(sql.text(query), conn)
                        if sources is None:
                            sources = df
                        else:
                            sources = sources.append(df)
                    else:
                        res = conn.execute(sql.text(query))
                        sources = self._convertResult(res, "DiaSource", sources)

        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def getDiaForcedSources(self, object_ids, dt, return_pandas=False):
        """Returns catalog of DiaForcedSource instances matching given
        DiaObjects.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of L1 database table. Re-mapping of the column names may
        be done for some columns (based on column map passed to constructor)
        but types or units are not changed.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog` or `None`
            Catalog contaning DiaForcedSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """

        if self.config.read_forced_sources_months == 0:
            _LOG.info("Skip DiaForceSources fetching")
            return None

        if len(object_ids) <= 0:
            _LOG.info("Skip DiaForceSources fetching - no Objects")
            # this should create a catalog, but the list of columns may be empty
            return None

        table = self._schema.forcedSources
        sources = None

        with Timer('DiaForcedSource select', self.config.timer):
            with _ansi_session(self._engine) as conn:
                for ids in _split(sorted(object_ids), 1000):

                    query = 'SELECT *  FROM "' + table.name + '" WHERE '

                    # select by object id
                    ids = ",".join(str(id) for id in ids)
                    query += '"diaObjectId" IN (' + ids + ') '

                    # execute select
                    if return_pandas:
                        df = pandas.read_sql_query(sql.text(query), conn)
                        if sources is None:
                            sources = df
                        else:
                            sources = sources.append(df)
                    else:
                        res = conn.execute(sql.text(query))
                        sources = self._convertResult(res, "DiaForcedSource", sources)

        _LOG.debug("found %s DiaForcedSources", len(sources))
        return sources

    def storeDiaObjects(self, objs, dt):
        """Store catalog of DiaObjects from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - some columns names are re-mapped based on column map passed to
            constructor
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from afw schema
          - this method knows how to fill interval-related columns
            (validityStart, validityEnd) they do not need to appear in
            afw schema

        Parameters
        ----------
        objs : `lsst.afw.table.BaseCatalog` or `pandas.DataFrame`
            Catalog with DiaObject records
        dt : `datetime.datetime`
            Time of the visit
        """

        if isinstance(objs, pandas.DataFrame):
            ids = sorted(objs['diaObjectId'])
        else:
            ids = sorted([obj['id'] for obj in objs])
        _LOG.debug("first object ID: %d", ids[0])

        # NOTE: workaround for sqlite, need this here to avoid
        # "database is locked" error.
        table = self._schema.objects

        # everything to be done in single transaction
        with _ansi_session(self._engine) as conn:

            ids = ",".join(str(id) for id in ids)

            if self.config.dia_object_index == 'last_object_table':

                # insert and replace all records in LAST table, mysql and postgres have
                # non-standard features (handled in _storeObjectsAfw)
                table = self._schema.objects_last
                do_replace = self.config.object_last_replace
                # If the input data is of type Pandas, we drop the previous
                # objects regardless of the do_replace setting due to how
                # Pandas inserts objects.
                if not do_replace or isinstance(objs, pandas.DataFrame):
                    query = 'DELETE FROM "' + table.name + '" '
                    query += 'WHERE "diaObjectId" IN (' + ids + ') '

                    if self.config.explain:
                        # run the same query with explain
                        self._explain(query, conn)

                    with Timer(table.name + ' delete', self.config.timer):
                        res = conn.execute(sql.text(query))
                    _LOG.debug("deleted %s objects", res.rowcount)

                extra_columns = dict(lastNonForcedSource=dt)
                if isinstance(objs, pandas.DataFrame):
                    with Timer("DiaObjectLast insert", self.config.timer):
                        objs = _coerce_uint64(objs)
                        for col, data in extra_columns.items():
                            objs[col] = data
                        objs.to_sql("DiaObjectLast", conn, if_exists='append',
                                    index=False)
                else:
                    self._storeObjectsAfw(objs, conn, table, "DiaObjectLast",
                                          replace=do_replace,
                                          extra_columns=extra_columns)

            else:

                # truncate existing validity intervals
                table = self._schema.objects
                query = 'UPDATE "' + table.name + '" '
                query += "SET \"validityEnd\" = '" + str(dt) + "' "
                query += 'WHERE "diaObjectId" IN (' + ids + ') '
                query += 'AND "validityEnd" IS NULL'

                # _LOG.debug("query: %s", query)

                if self.config.explain:
                    # run the same query with explain
                    self._explain(query, conn)

                with Timer(table.name + ' truncate', self.config.timer):
                    res = conn.execute(sql.text(query))
                _LOG.debug("truncated %s intervals", res.rowcount)

            # insert new versions
            if self.config.dia_object_nightly:
                table = self._schema.objects_nightly
            else:
                table = self._schema.objects
            extra_columns = dict(lastNonForcedSource=dt, validityStart=dt,
                                 validityEnd=None)
            if isinstance(objs, pandas.DataFrame):
                with Timer("DiaObject insert", self.config.timer):
                    objs = _coerce_uint64(objs)
                    for col, data in extra_columns.items():
                        objs[col] = data
                    objs.to_sql("DiaObject", conn, if_exists='append',
                                index=False)
            else:
                self._storeObjectsAfw(objs, conn, table, "DiaObject",
                                      extra_columns=extra_columns)

    def storeDiaSources(self, sources):
        """Store catalog of DIASources from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of L1 database table:

          - column names must correspond to database table columns
          - some columns names may be re-mapped based on column map passed to
            constructor
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from afw schema

        Parameters
        ----------
        sources : `lsst.afw.table.BaseCatalog` or `pandas.DataFrame`
            Catalog containing DiaSource records
        """

        # everything to be done in single transaction
        with _ansi_session(self._engine) as conn:

            if isinstance(sources, pandas.DataFrame):
                with Timer("DiaSource insert", self.config.timer):
                    sources = _coerce_uint64(sources)
                    sources.to_sql("DiaSource", conn, if_exists='append',
                                   index=False)
            else:
                table = self._schema.sources
                self._storeObjectsAfw(sources, conn, table, "DiaSource")

    def storeDiaForcedSources(self, sources):
        """Store a set of DIAForcedSources from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of L1 database table:

          - column names must correspond to database table columns
          - some columns names may be re-mapped based on column map passed to
            constructor
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from afw schema

        Parameters
        ----------
        sources : `lsst.afw.table.BaseCatalog` or `pandas.DataFrame`
            Catalog containing DiaForcedSource records
        """

        # everything to be done in single transaction
        with _ansi_session(self._engine) as conn:

            if isinstance(sources, pandas.DataFrame):
                with Timer("DiaForcedSource insert", self.config.timer):
                    sources = _coerce_uint64(sources)
                    sources.to_sql("DiaForcedSource", conn, if_exists='append',
                                   index=False)
            else:
                table = self._schema.forcedSources
                self._storeObjectsAfw(sources, conn, table, "DiaForcedSource")

    def countUnassociatedObjects(self):
        """Return the number of DiaObjects that have only one DiaSource associated
        with them.

        Used as part of ap_verify metrics.

        Returns
        -------
        count : `int`
            Number of DiaObjects with exactly one associated DiaSource.
        """
        # Retrieve the DiaObject table.
        table = self._schema.objects

        # Construct the sql statement.
        stmt = sql.select([func.count()]).select_from(table).where(table.c.nDiaSources == 1)
        stmt = stmt.where(table.c.validityEnd == None)  # noqa: E711

        # Return the count.
        count = self._engine.scalar(stmt)

        return count

    def isVisitProcessed(self, visitInfo):
        """Test whether data from an image has been loaded into the database.

        Used as part of ap_verify metrics.

        Parameters
        ----------
        visitInfo : `lsst.afw.image.VisitInfo`
            The metadata for the image of interest.

        Returns
        -------
        isProcessed : `bool`
            `True` if the data are present, `False` otherwise.
        """
        id = visitInfo.getExposureId()
        table = self._schema.sources
        idField = table.c.ccdVisitId

        # Hopefully faster than SELECT DISTINCT
        query = sql.select([idField]).select_from(table) \
            .where(idField == id).limit(1)

        return self._engine.scalar(query) is not None

    def dailyJob(self):
        """Implement daily activities like cleanup/vacuum.

        What should be done during daily cleanup is determined by
        configuration/schema.
        """

        # move data from DiaObjectNightly into DiaObject
        if self.config.dia_object_nightly:
            with _ansi_session(self._engine) as conn:
                query = 'INSERT INTO "' + self._schema.objects.name + '" '
                query += 'SELECT * FROM "' + self._schema.objects_nightly.name + '"'
                with Timer('DiaObjectNightly copy', self.config.timer):
                    conn.execute(sql.text(query))

                query = 'DELETE FROM "' + self._schema.objects_nightly.name + '"'
                with Timer('DiaObjectNightly delete', self.config.timer):
                    conn.execute(sql.text(query))

        if self._engine.name == 'postgresql':

            # do VACUUM on all tables
            _LOG.info("Running VACUUM on all tables")
            connection = self._engine.raw_connection()
            ISOLATION_LEVEL_AUTOCOMMIT = 0
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            cursor.execute("VACUUM ANALYSE")

    def makeSchema(self, drop=False, mysql_engine='InnoDB', oracle_tablespace=None, oracle_iot=False):
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`
            If True then drop tables before creating new ones.
        mysql_engine : `str`, optional
            Name of the MySQL engine to use for new tables.
        oracle_tablespace : `str`, optional
            Name of Oracle tablespace.
        oracle_iot : `bool`, optional
            Make Index-organized DiaObjectLast table.
        """
        self._schema.makeSchema(drop=drop, mysql_engine=mysql_engine,
                                oracle_tablespace=oracle_tablespace,
                                oracle_iot=oracle_iot)

    def _explain(self, query, conn):
        """Run the query with explain
        """

        _LOG.info("explain for query: %s...", query[:64])

        if conn.engine.name == 'mysql':
            query = "EXPLAIN EXTENDED " + query
        else:
            query = "EXPLAIN " + query

        res = conn.execute(sql.text(query))
        if res.returns_rows:
            _LOG.info("explain: %s", res.keys())
            for row in res:
                _LOG.info("explain: %s", row)
        else:
            _LOG.info("EXPLAIN returned nothing")

    def _storeObjectsAfw(self, objects, conn, table, schema_table_name,
                         replace=False, extra_columns=None):
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `lsst.afw.table.BaseCatalog`
            Catalog containing object records
        conn :
            Database connection
        table : `sqlalchemy.Table`
            Database table
        schema_table_name : `str`
            Name of the table to be used for finding table schema.
        replace : `boolean`
            If `True` then use replace instead of INSERT (should be more efficient)
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        """

        def quoteValue(v):
            """Quote and escape values"""
            if v is None:
                v = "NULL"
            elif isinstance(v, datetime):
                v = "'" + str(v) + "'"
            elif isinstance(v, str):
                # we don't expect nasty stuff in strings
                v = "'" + v + "'"
            elif isinstance(v, geom.Angle):
                v = v.asDegrees()
                if np.isfinite(v):
                    v = str(v)
                else:
                    v = "NULL"
            else:
                if np.isfinite(v):
                    v = str(v)
                else:
                    v = "NULL"
            return v

        def quoteId(columnName):
            """Smart quoting for column names.
            Lower-case names are not quoted.
            """
            if not columnName.islower():
                columnName = '"' + columnName + '"'
            return columnName

        if conn.engine.name == "oracle":
            return self._storeObjectsAfwOracle(objects, conn, table,
                                               schema_table_name, replace,
                                               extra_columns)

        schema = objects.getSchema()
        # use extra columns if specified
        extra_fields = list((extra_columns or {}).keys())

        afw_fields = [field.getName() for key, field in schema
                      if field.getName() not in extra_fields]

        column_map = self._schema.getAfwColumns(schema_table_name)
        # list of columns (as in cat schema)
        fields = [column_map[field].name for field in afw_fields if field in column_map]

        if replace and conn.engine.name in ('mysql', 'sqlite'):
            query = 'REPLACE INTO '
        else:
            query = 'INSERT INTO '
        qfields = [quoteId(field) for field in fields + extra_fields]
        query += quoteId(table.name) + ' (' + ','.join(qfields) + ') ' + 'VALUES '

        values = []
        for rec in objects:
            row = []
            for field in afw_fields:
                if field not in column_map:
                    continue
                value = rec[field]
                if column_map[field].type == "DATETIME" and \
                   np.isfinite(value):
                    # convert seconds into datetime
                    value = datetime.utcfromtimestamp(value)
                row.append(quoteValue(value))
            for field in extra_fields:
                row.append(quoteValue(extra_columns[field]))
            values.append('(' + ','.join(row) + ')')

        if self.config.explain:
            # run the same query with explain, only give it one row of data
            self._explain(query + values[0], conn)

        query += ','.join(values)

        if replace and conn.engine.name == 'postgresql':
            # This depends on that "replace" can only be true for DiaObjectLast table
            pks = ('pixelId', 'diaObjectId')
            query += " ON CONFLICT (\"{}\", \"{}\") DO UPDATE SET ".format(*pks)
            fields = [column_map[field].name for field in afw_fields if field in column_map]
            fields = ['"{0}" = EXCLUDED."{0}"'.format(field)
                      for field in fields if field not in pks]
            query += ', '.join(fields)

        # _LOG.debug("query: %s", query)
        _LOG.info("%s: will store %d records", table.name, len(objects))
        with Timer(table.name + ' insert', self.config.timer):
            res = conn.execute(sql.text(query))
        _LOG.debug("inserted %s intervals", res.rowcount)

    def _storeObjectsAfwOracle(self, objects, conn, table, schema_table_name,
                               replace=False, extra_columns=None):
        """Store method for Oracle.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `lsst.afw.table.BaseCatalog`
            Catalog containing object records
        conn :
            Database connection
        table : `sqlalchemy.Table`
            Database table
        schema_table_name : `str`
            Name of the table to be used for finding table schema.
        replace : `boolean`
            If `True` then use replace instead of INSERT (should be more efficient)
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        """

        def quoteId(columnName):
            """Smart quoting for column names.
            Lower-case naems are not quoted (Oracle backend needs them unquoted).
            """
            if not columnName.islower():
                columnName = '"' + columnName + '"'
            return columnName

        schema = objects.getSchema()

        # use extra columns that as overrides always.
        extra_fields = list((extra_columns or {}).keys())

        afw_fields = [field.getName() for key, field in schema
                      if field.getName() not in extra_fields]
        # _LOG.info("afw_fields: %s", afw_fields)

        column_map = self._schema.getAfwColumns(schema_table_name)
        # _LOG.info("column_map: %s", column_map)

        # list of columns (as in cat schema)
        fields = [column_map[field].name for field in afw_fields
                  if field in column_map]
        # _LOG.info("fields: %s", fields)

        qfields = [quoteId(field) for field in fields + extra_fields]

        if not replace:
            vals = [":col{}".format(i) for i in range(len(fields))]
            vals += [":extcol{}".format(i) for i in range(len(extra_fields))]
            query = 'INSERT INTO ' + quoteId(table.name)
            query += ' (' + ','.join(qfields) + ') VALUES'
            query += ' (' + ','.join(vals) + ')'
        else:
            qvals = [":col{} {}".format(i, quoteId(field)) for i, field in enumerate(fields)]
            qvals += [":extcol{} {}".format(i, quoteId(field)) for i, field in enumerate(extra_fields)]
            pks = ('pixelId', 'diaObjectId')
            onexpr = ["SRC.{col} = DST.{col}".format(col=quoteId(col)) for col in pks]
            setexpr = ["DST.{col} = SRC.{col}".format(col=quoteId(col))
                       for col in fields + extra_fields if col not in pks]
            vals = ["SRC.{col}".format(col=quoteId(col)) for col in fields + extra_fields]
            query = "MERGE INTO {} DST ".format(quoteId(table.name))
            query += "USING (SELECT {} FROM DUAL) SRC ".format(", ".join(qvals))
            query += "ON ({}) ".format(" AND ".join(onexpr))
            query += "WHEN MATCHED THEN UPDATE SET {} ".format(" ,".join(setexpr))
            query += "WHEN NOT MATCHED THEN INSERT "
            query += "({}) VALUES ({})".format(','.join(qfields), ','.join(vals))
        # _LOG.info("query: %s", query)

        values = []
        for rec in objects:
            row = {}
            col = 0
            for field in afw_fields:
                if field not in column_map:
                    continue
                value = rec[field]
                if column_map[field].type == "DATETIME" and not np.isnan(value):
                    # convert seconds into datetime
                    value = datetime.utcfromtimestamp(value)
                elif isinstance(value, geom.Angle):
                    value = str(value.asDegrees())
                elif not np.isfinite(value):
                    value = None
                row["col{}".format(col)] = value
                col += 1
            for i, field in enumerate(extra_fields):
                row["extcol{}".format(i)] = extra_columns[field]
            values.append(row)

        # _LOG.debug("query: %s", query)
        _LOG.info("%s: will store %d records", table.name, len(objects))
        with Timer(table.name + ' insert', self.config.timer):
            res = conn.execute(sql.text(query), values)
        _LOG.debug("inserted %s intervals", res.rowcount)

    def _convertResult(self, res, table_name, catalog=None):
        """Convert result set into output catalog.

        Parameters
        ----------
        res : `sqlalchemy.ResultProxy`
            SQLAlchemy result set returned by query.
        table_name : `str`
            Name of the table.
        catalog : `lsst.afw.table.BaseCatalog`
            If not None then extend existing catalog

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`
             If ``catalog`` is None then new instance is returned, otherwise
             ``catalog`` is updated and returned.
        """
        # make catalog schema
        columns = res.keys()
        schema, col_map = self._schema.getAfwSchema(table_name, columns)
        if catalog is None:
            _LOG.debug("_convertResult: schema: %s", schema)
            _LOG.debug("_convertResult: col_map: %s", col_map)
            catalog = afwTable.SourceCatalog(schema)

        # fill catalog
        for row in res:
            record = catalog.addNew()
            for col, value in row.items():
                # some columns may exist in database but not included in afw schema
                col = col_map.get(col)
                if col is not None:
                    if isinstance(value, datetime):
                        # convert datetime to number of seconds
                        value = int((value - datetime.utcfromtimestamp(0)).total_seconds())
                    elif col.getTypeString() == 'Angle' and value is not None:
                        value = value * geom.degrees
                    if value is not None:
                        record.set(col, value)

        return catalog
