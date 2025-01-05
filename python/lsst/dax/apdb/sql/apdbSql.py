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

from __future__ import annotations

__all__ = ["ApdbSql"]

import datetime
import logging
import urllib.parse
import warnings
from collections.abc import Iterable, Mapping, MutableMapping
from contextlib import closing, suppress
from typing import TYPE_CHECKING, Any, cast

import astropy.time
import numpy as np
import pandas
import sqlalchemy
import sqlalchemy.dialects.postgresql
import sqlalchemy.dialects.sqlite
from lsst.sphgeom import HtmPixelization, LonLat, Region, UnitVector3d
from lsst.utils.db_auth import DbAuth, DbAuthNotFoundError
from lsst.utils.iteration import chunk_iterable
from sqlalchemy import func, sql
from sqlalchemy.pool import NullPool

from .._auth import DB_AUTH_ENVVAR, DB_AUTH_PATH
from ..apdb import Apdb
from ..apdbConfigFreezer import ApdbConfigFreezer
from ..apdbReplica import ReplicaChunk
from ..apdbSchema import ApdbTables
from ..config import ApdbConfig
from ..monitor import MonAgent
from ..schema_model import Table
from ..timer import Timer
from ..versionTuple import IncompatibleVersionError, VersionTuple
from .apdbMetadataSql import ApdbMetadataSql
from .apdbSqlReplica import ApdbSqlReplica
from .apdbSqlSchema import ApdbSqlSchema, ExtraTables
from .config import ApdbSqlConfig

if TYPE_CHECKING:
    import sqlite3

    from ..apdbMetadata import ApdbMetadata

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)

VERSION = VersionTuple(0, 1, 1)
"""Version for the code controlling non-replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""


def _coerce_uint64(df: pandas.DataFrame) -> pandas.DataFrame:
    """Change the type of uint64 columns to int64, and return copy of data
    frame.
    """
    names = [c[0] for c in df.dtypes.items() if c[1] == np.uint64]
    return df.astype({name: np.int64 for name in names})


def _make_midpointMjdTai_start(visit_time: astropy.time.Time, months: int) -> float:
    """Calculate starting point for time-based source search.

    Parameters
    ----------
    visit_time : `astropy.time.Time`
        Time of current visit.
    months : `int`
        Number of months in the sources history.

    Returns
    -------
    time : `float`
        A ``midpointMjdTai`` starting point, MJD time.
    """
    # TODO: Use of MJD must be consistent with the code in ap_association
    # (see DM-31996)
    return visit_time.mjd - months * 30


def _onSqlite3Connect(
    dbapiConnection: sqlite3.Connection, connectionRecord: sqlalchemy.pool._ConnectionRecord
) -> None:
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")


class ApdbSql(Apdb):
    """Implementation of APDB interface based on SQL database.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbSqlConfig` configuration class. For an example of different
    configurations check ``config/`` folder.

    Parameters
    ----------
    config : `ApdbSqlConfig`
        Configuration object.
    """

    metadataSchemaVersionKey = "version:schema"
    """Name of the metadata key to store schema version number."""

    metadataCodeVersionKey = "version:ApdbSql"
    """Name of the metadata key to store code version number."""

    metadataReplicaVersionKey = "version:ApdbSqlReplica"
    """Name of the metadata key to store replica code version number."""

    metadataConfigKey = "config:apdb-sql.json"
    """Name of the metadata key to store code version number."""

    _frozen_parameters = (
        "enable_replica",
        "dia_object_index",
        "pixelization.htm_level",
        "pixelization.htm_index_column",
        "ra_dec_columns",
    )
    """Names of the config parameters to be frozen in metadata table."""

    def __init__(self, config: ApdbSqlConfig):
        self._engine = self._makeEngine(config, create=False)

        sa_metadata = sqlalchemy.MetaData(schema=config.namespace)
        meta_table_name = ApdbTables.metadata.table_name(prefix=config.prefix)
        meta_table: sqlalchemy.schema.Table | None = None
        with suppress(sqlalchemy.exc.NoSuchTableError):
            meta_table = sqlalchemy.schema.Table(meta_table_name, sa_metadata, autoload_with=self._engine)

        self._metadata = ApdbMetadataSql(self._engine, meta_table)

        # Read frozen config from metadata.
        config_json = self._metadata.get(self.metadataConfigKey)
        if config_json is not None:
            # Update config from metadata.
            freezer = ApdbConfigFreezer[ApdbSqlConfig](self._frozen_parameters)
            self.config = freezer.update(config, config_json)
        else:
            self.config = config

        self._schema = ApdbSqlSchema(
            engine=self._engine,
            dia_object_index=self.config.dia_object_index,
            schema_file=self.config.schema_file,
            schema_name=self.config.schema_name,
            prefix=self.config.prefix,
            namespace=self.config.namespace,
            htm_index_column=self.config.pixelization.htm_index_column,
            enable_replica=self.config.enable_replica,
        )

        if self._metadata.table_exists():
            self._versionCheck(self._metadata)

        self.pixelator = HtmPixelization(self.config.pixelization.htm_level)

        _LOG.debug("APDB Configuration:")
        _LOG.debug("    dia_object_index: %s", self.config.dia_object_index)
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    schema prefix: %s", self.config.prefix)

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, tags=tags)

    @classmethod
    def _makeEngine(cls, config: ApdbSqlConfig, *, create: bool) -> sqlalchemy.engine.Engine:
        """Make SQLALchemy engine based on configured parameters.

        Parameters
        ----------
        config : `ApdbSqlConfig`
            Configuration object.
        create : `bool`
            Whether to try to create new database file, only relevant for
            SQLite backend which always creates new files by default.
        """
        # engine is reused between multiple processes, make sure that we don't
        # share connections by disabling pool (by using NullPool class)
        kw: MutableMapping[str, Any] = dict(config.connection_config.extra_parameters)
        conn_args: dict[str, Any] = dict()
        if not config.connection_config.connection_pool:
            kw.update(poolclass=NullPool)
        if config.connection_config.isolation_level is not None:
            kw.update(isolation_level=config.connection_config.isolation_level)
        elif config.db_url.startswith("sqlite"):
            # Use READ_UNCOMMITTED as default value for sqlite.
            kw.update(isolation_level="READ_UNCOMMITTED")
        if config.connection_config.connection_timeout is not None:
            if config.db_url.startswith("sqlite"):
                conn_args.update(timeout=config.connection_config.connection_timeout)
            elif config.db_url.startswith(("postgresql", "mysql")):
                conn_args.update(connect_timeout=int(config.connection_config.connection_timeout))
        kw.update(connect_args=conn_args)
        engine = sqlalchemy.create_engine(cls._connection_url(config.db_url, create=create), **kw)

        if engine.dialect.name == "sqlite":
            # Need to enable foreign keys on every new connection.
            sqlalchemy.event.listen(engine, "connect", _onSqlite3Connect)

        return engine

    @classmethod
    def _connection_url(cls, config_url: str, *, create: bool) -> sqlalchemy.engine.URL | str:
        """Generate a complete URL for database with proper credentials.

        Parameters
        ----------
        config_url : `str`
            Database URL as specified in configuration.
        create : `bool`
            Whether to try to create new database file, only relevant for
            SQLite backend which always creates new files by default.

        Returns
        -------
        connection_url : `sqlalchemy.engine.URL` or `str`
            Connection URL including credentials.
        """
        # Allow 3rd party authentication mechanisms by assuming connection
        # string is correct when we can not recognize (dialect, host, database)
        # matching keys.
        components = urllib.parse.urlparse(config_url)
        if all((components.scheme is not None, components.hostname is not None, components.path is not None)):
            try:
                db_auth = DbAuth(DB_AUTH_PATH, DB_AUTH_ENVVAR)
                config_url = db_auth.getUrl(config_url)
            except DbAuthNotFoundError:
                # Credentials file doesn't exist or no matching credentials,
                # use default auth.
                pass

        # SQLite has a nasty habit creating empty databases when they do not
        # exist, tell it not to do that unless we do need to create it.
        if not create:
            config_url = cls._update_sqlite_url(config_url)

        return config_url

    @classmethod
    def _update_sqlite_url(cls, url_string: str) -> str:
        """If URL refers to sqlite dialect, update it so that the backend does
        not try to create database file if it does not exist already.

        Parameters
        ----------
        url_string : `str`
            Connection string.

        Returns
        -------
        url_string : `str`
            Possibly updated connection string.
        """
        try:
            url = sqlalchemy.make_url(url_string)
        except sqlalchemy.exc.SQLAlchemyError:
            # If parsing fails it means some special format, likely not
            # sqlite so we just return it unchanged.
            return url_string

        if url.get_backend_name() == "sqlite":
            # Massage url so that database name starts with "file:" and
            # option string has "mode=rw&uri=true". Database name
            # should look like a path (:memory: is not supported by
            # Apdb, but someone could still try to use it).
            database = url.database
            if database and not database.startswith((":", "file:")):
                query = dict(url.query, mode="rw", uri="true")
                # If ``database`` is an absolute path then original URL should
                # include four slashes after "sqlite:". Humans are bad at
                # counting things beyond four and sometimes an extra slash gets
                # added unintentionally, which causes sqlite to treat initial
                # element as "authority" and to complain. Strip extra slashes
                # at the start of the path to avoid that (DM-46077).
                if database.startswith("//"):
                    warnings.warn(
                        f"Database URL contains extra leading slashes which will be removed: {url}",
                        stacklevel=3,
                    )
                    database = "/" + database.lstrip("/")
                url = url.set(database=f"file:{database}", query=query)
                url_string = url.render_as_string()

        return url_string

    def _versionCheck(self, metadata: ApdbMetadataSql) -> None:
        """Check schema version compatibility."""

        def _get_version(key: str, default: VersionTuple) -> VersionTuple:
            """Retrieve version number from given metadata key."""
            if metadata.table_exists():
                version_str = metadata.get(key)
                if version_str is None:
                    # Should not happen with existing metadata table.
                    raise RuntimeError(f"Version key {key!r} does not exist in metadata table.")
                return VersionTuple.fromString(version_str)
            return default

        # For old databases where metadata table does not exist we assume that
        # version of both code and schema is 0.1.0.
        initial_version = VersionTuple(0, 1, 0)
        db_schema_version = _get_version(self.metadataSchemaVersionKey, initial_version)
        db_code_version = _get_version(self.metadataCodeVersionKey, initial_version)

        # For now there is no way to make read-only APDB instances, assume that
        # any access can do updates.
        if not self._schema.schemaVersion().checkCompatibility(db_schema_version):
            raise IncompatibleVersionError(
                f"Configured schema version {self._schema.schemaVersion()} "
                f"is not compatible with database version {db_schema_version}"
            )
        if not self.apdbImplementationVersion().checkCompatibility(db_code_version):
            raise IncompatibleVersionError(
                f"Current code version {self.apdbImplementationVersion()} "
                f"is not compatible with database version {db_code_version}"
            )

        # Check replica code version only if replica is enabled.
        if self._schema.has_replica_chunks:
            db_replica_version = _get_version(self.metadataReplicaVersionKey, initial_version)
            code_replica_version = ApdbSqlReplica.apdbReplicaImplementationVersion()
            if not code_replica_version.checkCompatibility(db_replica_version):
                raise IncompatibleVersionError(
                    f"Current replication code version {code_replica_version} "
                    f"is not compatible with database version {db_replica_version}"
                )

    @classmethod
    def apdbImplementationVersion(cls) -> VersionTuple:
        """Return version number for current APDB implementation.

        Returns
        -------
        version : `VersionTuple`
            Version of the code defined in implementation class.
        """
        return VERSION

    @classmethod
    def init_database(
        cls,
        db_url: str,
        *,
        schema_file: str | None = None,
        schema_name: str | None = None,
        read_sources_months: int | None = None,
        read_forced_sources_months: int | None = None,
        enable_replica: bool = False,
        connection_timeout: int | None = None,
        dia_object_index: str | None = None,
        htm_level: int | None = None,
        htm_index_column: str | None = None,
        ra_dec_columns: list[str] | None = None,
        prefix: str | None = None,
        namespace: str | None = None,
        drop: bool = False,
    ) -> ApdbSqlConfig:
        """Initialize new APDB instance and make configuration object for it.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database URL.
        schema_file : `str`, optional
            Location of (YAML) configuration file with APDB schema. If not
            specified then default location will be used.
        schema_name : str | None
            Name of the schema in YAML configuration file. If not specified
            then default name will be used.
        read_sources_months : `int`, optional
            Number of months of history to read from DiaSource.
        read_forced_sources_months : `int`, optional
            Number of months of history to read from DiaForcedSource.
        enable_replica : `bool`
            If True, make additional tables used for replication to PPDB.
        connection_timeout : `int`, optional
            Database connection timeout in seconds.
        dia_object_index : `str`, optional
            Indexing mode for DiaObject table.
        htm_level : `int`, optional
            HTM indexing level.
        htm_index_column : `str`, optional
            Name of a HTM index column for DiaObject and DiaSource tables.
        ra_dec_columns : `list` [`str`], optional
            Names of ra/dec columns in DiaObject table.
        prefix : `str`, optional
            Optional prefix for all table names.
        namespace : `str`, optional
            Name of the database schema for all APDB tables. If not specified
            then default schema is used.
        drop : `bool`, optional
            If `True` then drop existing tables before re-creating the schema.

        Returns
        -------
        config : `ApdbSqlConfig`
            Resulting configuration object for a created APDB instance.
        """
        config = ApdbSqlConfig(db_url=db_url, enable_replica=enable_replica)
        if schema_file is not None:
            config.schema_file = schema_file
        if schema_name is not None:
            config.schema_name = schema_name
        if read_sources_months is not None:
            config.read_sources_months = read_sources_months
        if read_forced_sources_months is not None:
            config.read_forced_sources_months = read_forced_sources_months
        if connection_timeout is not None:
            config.connection_config.connection_timeout = connection_timeout
        if dia_object_index is not None:
            config.dia_object_index = dia_object_index
        if htm_level is not None:
            config.pixelization.htm_level = htm_level
        if htm_index_column is not None:
            config.pixelization.htm_index_column = htm_index_column
        if ra_dec_columns is not None:
            config.ra_dec_columns = ra_dec_columns
        if prefix is not None:
            config.prefix = prefix
        if namespace is not None:
            config.namespace = namespace

        cls._makeSchema(config, drop=drop)

        # SQLite has a nasty habit of creating empty database by default,
        # update URL in config file to disable that behavior.
        config.db_url = cls._update_sqlite_url(config.db_url)

        return config

    def get_replica(self) -> ApdbSqlReplica:
        """Return `ApdbReplica` instance for this database."""
        return ApdbSqlReplica(self._schema, self._engine)

    def tableRowCount(self) -> dict[str, int]:
        """Return dictionary with the table names and row counts.

        Used by ``ap_proto`` to keep track of the size of the database tables.
        Depending on database technology this could be expensive operation.

        Returns
        -------
        row_counts : `dict`
            Dict where key is a table name and value is a row count.
        """
        res = {}
        tables = [ApdbTables.DiaObject, ApdbTables.DiaSource, ApdbTables.DiaForcedSource]
        if self.config.dia_object_index == "last_object_table":
            tables.append(ApdbTables.DiaObjectLast)
        with self._engine.begin() as conn:
            for table in tables:
                sa_table = self._schema.get_table(table)
                stmt = sql.select(func.count()).select_from(sa_table)
                count: int = conn.execute(stmt).scalar_one()
                res[table.name] = count

        return res

    def tableDef(self, table: ApdbTables) -> Table | None:
        # docstring is inherited from a base class
        return self._schema.tableSchemas.get(table)

    @classmethod
    def _makeSchema(cls, config: ApdbConfig, drop: bool = False) -> None:
        # docstring is inherited from a base class

        if not isinstance(config, ApdbSqlConfig):
            raise TypeError(f"Unexpected type of configuration object: {type(config)}")

        engine = cls._makeEngine(config, create=True)

        # Ask schema class to create all tables.
        schema = ApdbSqlSchema(
            engine=engine,
            dia_object_index=config.dia_object_index,
            schema_file=config.schema_file,
            schema_name=config.schema_name,
            prefix=config.prefix,
            namespace=config.namespace,
            htm_index_column=config.pixelization.htm_index_column,
            enable_replica=config.enable_replica,
        )
        schema.makeSchema(drop=drop)

        # Need metadata table to store few items in it, if table exists.
        meta_table: sqlalchemy.schema.Table | None = None
        with suppress(ValueError):
            meta_table = schema.get_table(ApdbTables.metadata)

        apdb_meta = ApdbMetadataSql(engine, meta_table)
        if apdb_meta.table_exists():
            # Fill version numbers, overwrite if they are already there.
            apdb_meta.set(cls.metadataSchemaVersionKey, str(schema.schemaVersion()), force=True)
            apdb_meta.set(cls.metadataCodeVersionKey, str(cls.apdbImplementationVersion()), force=True)
            if config.enable_replica:
                # Only store replica code version if replica is enabled.
                apdb_meta.set(
                    cls.metadataReplicaVersionKey,
                    str(ApdbSqlReplica.apdbReplicaImplementationVersion()),
                    force=True,
                )

            # Store frozen part of a configuration in metadata.
            freezer = ApdbConfigFreezer[ApdbSqlConfig](cls._frozen_parameters)
            apdb_meta.set(cls.metadataConfigKey, freezer.to_json(config), force=True)

    def getDiaObjects(self, region: Region) -> pandas.DataFrame:
        # docstring is inherited from a base class

        # decide what columns we need
        if self.config.dia_object_index == "last_object_table":
            table_enum = ApdbTables.DiaObjectLast
        else:
            table_enum = ApdbTables.DiaObject
        table = self._schema.get_table(table_enum)
        if not self.config.dia_object_columns:
            columns = self._schema.get_apdb_columns(table_enum)
        else:
            columns = [table.c[col] for col in self.config.dia_object_columns]
        query = sql.select(*columns)

        # build selection
        query = query.where(self._filterRegion(table, region))

        # select latest version of objects
        if self.config.dia_object_index != "last_object_table":
            query = query.where(table.c.validityEnd == None)  # noqa: E711

        # _LOG.debug("query: %s", query)

        # execute select
        with self._timer("select_time", tags={"table": "DiaObject"}) as timer:
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
            timer.add_values(row_count=len(objects))
        _LOG.debug("found %s DiaObjects", len(objects))
        return self._fix_result_timestamps(objects)

    def getDiaSources(
        self, region: Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        if self.config.read_sources_months == 0:
            _LOG.debug("Skip DiaSources fetching")
            return None

        if object_ids is None:
            # region-based select
            return self._getDiaSourcesInRegion(region, visit_time)
        else:
            return self._getDiaSourcesByIDs(list(object_ids), visit_time)

    def getDiaForcedSources(
        self, region: Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        if self.config.read_forced_sources_months == 0:
            _LOG.debug("Skip DiaForceSources fetching")
            return None

        if object_ids is None:
            # This implementation does not support region-based selection.
            raise NotImplementedError("Region-based selection is not supported")

        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midpointMjdTai ourselves in store()
        midpointMjdTai_start = _make_midpointMjdTai_start(visit_time, self.config.read_forced_sources_months)
        _LOG.debug("midpointMjdTai_start = %.6f", midpointMjdTai_start)

        with self._timer("select_time", tags={"table": "DiaForcedSource"}) as timer:
            sources = self._getSourcesByIDs(
                ApdbTables.DiaForcedSource, list(object_ids), midpointMjdTai_start
            )
            timer.add_values(row_count=len(sources))

        _LOG.debug("found %s DiaForcedSources", len(sources))
        return sources

    def containsVisitDetector(self, visit: int, detector: int) -> bool:
        # docstring is inherited from a base class
        src_table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaSource)
        frcsrc_table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaForcedSource)
        # Query should load only one leaf page of the index
        query1 = sql.select(src_table.c.visit).filter_by(visit=visit, detector=detector).limit(1)

        with self._engine.begin() as conn:
            result = conn.execute(query1).scalar_one_or_none()
            if result is not None:
                return True
            else:
                # Backup query if an image was processed but had no diaSources
                query2 = sql.select(frcsrc_table.c.visit).filter_by(visit=visit, detector=detector).limit(1)
                result = conn.execute(query2).scalar_one_or_none()
                return result is not None

    def getSSObjects(self) -> pandas.DataFrame:
        # docstring is inherited from a base class

        columns = self._schema.get_apdb_columns(ApdbTables.SSObject)
        query = sql.select(*columns)

        # execute select
        with self._timer("SSObject_select_time", tags={"table": "SSObject"}) as timer:
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
            timer.add_values(row_count=len(objects))
        _LOG.debug("found %s SSObjects", len(objects))
        return self._fix_result_timestamps(objects)

    def store(
        self,
        visit_time: astropy.time.Time,
        objects: pandas.DataFrame,
        sources: pandas.DataFrame | None = None,
        forced_sources: pandas.DataFrame | None = None,
    ) -> None:
        # docstring is inherited from a base class
        objects = self._fix_input_timestamps(objects)
        if sources is not None:
            sources = self._fix_input_timestamps(sources)
        if forced_sources is not None:
            forced_sources = self._fix_input_timestamps(forced_sources)

        # We want to run all inserts in one transaction.
        with self._engine.begin() as connection:
            replica_chunk: ReplicaChunk | None = None
            if self._schema.has_replica_chunks:
                replica_chunk = ReplicaChunk.make_replica_chunk(visit_time, self.config.replica_chunk_seconds)
                self._storeReplicaChunk(replica_chunk, visit_time, connection)

            # fill pixelId column for DiaObjects
            objects = self._add_spatial_index(objects)
            self._storeDiaObjects(objects, visit_time, replica_chunk, connection)

            if sources is not None:
                # fill pixelId column for DiaSources
                sources = self._add_spatial_index(sources)
                self._storeDiaSources(sources, replica_chunk, connection)

            if forced_sources is not None:
                self._storeDiaForcedSources(forced_sources, replica_chunk, connection)

    def storeSSObjects(self, objects: pandas.DataFrame) -> None:
        # docstring is inherited from a base class
        objects = self._fix_input_timestamps(objects)

        idColumn = "ssObjectId"
        table = self._schema.get_table(ApdbTables.SSObject)

        # everything to be done in single transaction
        with self._engine.begin() as conn:
            # Find record IDs that already exist. Some types like np.int64 can
            # cause issues with sqlalchemy, convert them to int.
            ids = sorted(int(oid) for oid in objects[idColumn])

            query = sql.select(table.columns[idColumn], table.columns[idColumn].in_(ids))
            result = conn.execute(query)
            knownIds = set(row.ssObjectId for row in result)

            filter = objects[idColumn].isin(knownIds)
            toUpdate = cast(pandas.DataFrame, objects[filter])
            toInsert = cast(pandas.DataFrame, objects[~filter])

            # insert new records
            if len(toInsert) > 0:
                toInsert.to_sql(table.name, conn, if_exists="append", index=False, schema=table.schema)

            # update existing records
            if len(toUpdate) > 0:
                whereKey = f"{idColumn}_param"
                update = table.update().where(table.columns[idColumn] == sql.bindparam(whereKey))
                toUpdate = toUpdate.rename({idColumn: whereKey}, axis="columns")
                values = toUpdate.to_dict("records")
                result = conn.execute(update, values)

    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        # docstring is inherited from a base class

        table = self._schema.get_table(ApdbTables.DiaSource)
        query = table.update().where(table.columns["diaSourceId"] == sql.bindparam("srcId"))

        with self._engine.begin() as conn:
            # Need to make sure that every ID exists in the database, but
            # executemany may not support rowcount, so iterate and check what
            # is missing.
            missing_ids: list[int] = []
            for key, value in idMap.items():
                params = dict(srcId=key, diaObjectId=0, ssObjectId=value)
                result = conn.execute(query, params)
                if result.rowcount == 0:
                    missing_ids.append(key)
            if missing_ids:
                missing = ",".join(str(item) for item in missing_ids)
                raise ValueError(f"Following DiaSource IDs do not exist in the database: {missing}")

    def dailyJob(self) -> None:
        # docstring is inherited from a base class
        pass

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class

        # Retrieve the DiaObject table.
        table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaObject)

        # Construct the sql statement.
        stmt = sql.select(func.count()).select_from(table).where(table.c.nDiaSources == 1)
        stmt = stmt.where(table.c.validityEnd == None)  # noqa: E711

        # Return the count.
        with self._engine.begin() as conn:
            count = conn.execute(stmt).scalar_one()

        return count

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        if self._metadata is None:
            raise RuntimeError("Database schema was not initialized.")
        return self._metadata

    def _getDiaSourcesInRegion(self, region: Region, visit_time: astropy.time.Time) -> pandas.DataFrame:
        """Return catalog of DiaSource instances from given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        visit_time : `astropy.time.Time`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing DiaSource records.
        """
        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midpointMjdTai ourselves in store()
        midpointMjdTai_start = _make_midpointMjdTai_start(visit_time, self.config.read_sources_months)
        _LOG.debug("midpointMjdTai_start = %.6f", midpointMjdTai_start)

        table = self._schema.get_table(ApdbTables.DiaSource)
        columns = self._schema.get_apdb_columns(ApdbTables.DiaSource)
        query = sql.select(*columns)

        # build selection
        time_filter = table.columns["midpointMjdTai"] > midpointMjdTai_start
        where = sql.expression.and_(self._filterRegion(table, region), time_filter)
        query = query.where(where)

        # execute select
        with self._timer("DiaSource_select_time", tags={"table": "DiaSource"}) as timer:
            with self._engine.begin() as conn:
                sources = pandas.read_sql_query(query, conn)
            timer.add_values(row_counts=len(sources))
        _LOG.debug("found %s DiaSources", len(sources))
        return self._fix_result_timestamps(sources)

    def _getDiaSourcesByIDs(self, object_ids: list[int], visit_time: astropy.time.Time) -> pandas.DataFrame:
        """Return catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        visit_time : `astropy.time.Time`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records.
        """
        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midpointMjdTai ourselves in store()
        midpointMjdTai_start = _make_midpointMjdTai_start(visit_time, self.config.read_sources_months)
        _LOG.debug("midpointMjdTai_start = %.6f", midpointMjdTai_start)

        with self._timer("select_time", tags={"table": "DiaSource"}) as timer:
            sources = self._getSourcesByIDs(ApdbTables.DiaSource, object_ids, midpointMjdTai_start)
            timer.add_values(row_count=len(sources))

        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def _getSourcesByIDs(
        self, table_enum: ApdbTables, object_ids: list[int], midpointMjdTai_start: float
    ) -> pandas.DataFrame:
        """Return catalog of DiaSource or DiaForcedSource instances given set
        of DiaObject IDs.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Database table.
        object_ids :
            Collection of DiaObject IDs
        midpointMjdTai_start : `float`
            Earliest midpointMjdTai to retrieve.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """
        table = self._schema.get_table(table_enum)
        columns = self._schema.get_apdb_columns(table_enum)

        sources: pandas.DataFrame | None = None
        if len(object_ids) <= 0:
            _LOG.debug("ID list is empty, just fetch empty result")
            query = sql.select(*columns).where(sql.literal(False))
            with self._engine.begin() as conn:
                sources = pandas.read_sql_query(query, conn)
        else:
            data_frames: list[pandas.DataFrame] = []
            for ids in chunk_iterable(sorted(object_ids), 1000):
                query = sql.select(*columns)

                # Some types like np.int64 can cause issues with
                # sqlalchemy, convert them to int.
                int_ids = [int(oid) for oid in ids]

                # select by object id
                query = query.where(
                    sql.expression.and_(
                        table.columns["diaObjectId"].in_(int_ids),
                        table.columns["midpointMjdTai"] > midpointMjdTai_start,
                    )
                )

                # execute select
                with self._engine.begin() as conn:
                    data_frames.append(pandas.read_sql_query(query, conn))

            if len(data_frames) == 1:
                sources = data_frames[0]
            else:
                sources = pandas.concat(data_frames)
        assert sources is not None, "Catalog cannot be None"
        return self._fix_result_timestamps(sources)

    def _storeReplicaChunk(
        self,
        replica_chunk: ReplicaChunk,
        visit_time: astropy.time.Time,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        # `visit_time.datetime` returns naive datetime, even though all astropy
        # times are in UTC. Add UTC timezone to timestampt so that database
        # can store a correct value.
        dt = datetime.datetime.fromtimestamp(visit_time.unix_tai, tz=datetime.timezone.utc)

        table = self._schema.get_table(ExtraTables.ApdbReplicaChunks)

        # We need UPSERT which is dialect-specific construct
        values = {"last_update_time": dt, "unique_id": replica_chunk.unique_id}
        row = {"apdb_replica_chunk": replica_chunk.id} | values
        if connection.dialect.name == "sqlite":
            insert_sqlite = sqlalchemy.dialects.sqlite.insert(table)
            insert_sqlite = insert_sqlite.on_conflict_do_update(index_elements=table.primary_key, set_=values)
            connection.execute(insert_sqlite, row)
        elif connection.dialect.name == "postgresql":
            insert_pg = sqlalchemy.dialects.postgresql.dml.insert(table)
            insert_pg = insert_pg.on_conflict_do_update(constraint=table.primary_key, set_=values)
            connection.execute(insert_pg, row)
        else:
            raise TypeError(f"Unsupported dialect {connection.dialect.name} for upsert.")

    def _storeDiaObjects(
        self,
        objs: pandas.DataFrame,
        visit_time: astropy.time.Time,
        replica_chunk: ReplicaChunk | None,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        """Store catalog of DiaObjects from current visit.

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records.
        visit_time : `astropy.time.Time`
            Time of the visit.
        replica_chunk : `ReplicaChunk`
            Insert identifier.
        """
        if len(objs) == 0:
            _LOG.debug("No objects to write to database.")
            return

        # Some types like np.int64 can cause issues with sqlalchemy, convert
        # them to int.
        ids = sorted(int(oid) for oid in objs["diaObjectId"])
        _LOG.debug("first object ID: %d", ids[0])

        # TODO: Need to verify that we are using correct scale here for
        # DATETIME representation (see DM-31996).
        dt = visit_time.datetime

        # everything to be done in single transaction
        if self.config.dia_object_index == "last_object_table":
            # Insert and replace all records in LAST table.
            table = self._schema.get_table(ApdbTables.DiaObjectLast)

            # Drop the previous objects (pandas cannot upsert).
            query = table.delete().where(table.columns["diaObjectId"].in_(ids))

            with self._timer("delete_time", tags={"table": table.name}) as timer:
                res = connection.execute(query)
                timer.add_values(row_count=res.rowcount)
            _LOG.debug("deleted %s objects", res.rowcount)

            # DiaObjectLast is a subset of DiaObject, strip missing columns
            last_column_names = [column.name for column in table.columns]
            last_objs = objs[last_column_names]
            last_objs = _coerce_uint64(last_objs)

            if "lastNonForcedSource" in last_objs.columns:
                # lastNonForcedSource is defined NOT NULL, fill it with visit
                # time just in case.
                last_objs.fillna({"lastNonForcedSource": dt}, inplace=True)
            else:
                extra_column = pandas.Series([dt] * len(objs), name="lastNonForcedSource")
                last_objs.set_index(extra_column.index, inplace=True)
                last_objs = pandas.concat([last_objs, extra_column], axis="columns")

            with self._timer("insert_time", tags={"table": "DiaObjectLast"}) as timer:
                last_objs.to_sql(
                    table.name,
                    connection,
                    if_exists="append",
                    index=False,
                    schema=table.schema,
                )
                timer.add_values(row_count=len(last_objs))
        else:
            # truncate existing validity intervals
            table = self._schema.get_table(ApdbTables.DiaObject)

            update = (
                table.update()
                .values(validityEnd=dt)
                .where(
                    sql.expression.and_(
                        table.columns["diaObjectId"].in_(ids),
                        table.columns["validityEnd"].is_(None),
                    )
                )
            )

            with self._timer("truncate_time", tags={"table": table.name}) as timer:
                res = connection.execute(update)
                timer.add_values(row_count=res.rowcount)
            _LOG.debug("truncated %s intervals", res.rowcount)

        objs = _coerce_uint64(objs)

        # Fill additional columns
        extra_columns: list[pandas.Series] = []
        if "validityStart" in objs.columns:
            objs["validityStart"] = dt
        else:
            extra_columns.append(pandas.Series([dt] * len(objs), name="validityStart"))
        if "validityEnd" in objs.columns:
            objs["validityEnd"] = None
        else:
            extra_columns.append(pandas.Series([None] * len(objs), name="validityEnd"))
        if "lastNonForcedSource" in objs.columns:
            # lastNonForcedSource is defined NOT NULL, fill it with visit time
            # just in case.
            objs.fillna({"lastNonForcedSource": dt}, inplace=True)
        else:
            extra_columns.append(pandas.Series([dt] * len(objs), name="lastNonForcedSource"))
        if extra_columns:
            objs.set_index(extra_columns[0].index, inplace=True)
            objs = pandas.concat([objs] + extra_columns, axis="columns")

        # Insert replica data
        table = self._schema.get_table(ApdbTables.DiaObject)
        replica_data: list[dict] = []
        replica_stmt: Any = None
        replica_table_name = ""
        if replica_chunk is not None:
            pk_names = [column.name for column in table.primary_key]
            replica_data = objs[pk_names].to_dict("records")
            for row in replica_data:
                row["apdb_replica_chunk"] = replica_chunk.id
            replica_table = self._schema.get_table(ExtraTables.DiaObjectChunks)
            replica_table_name = replica_table.name
            replica_stmt = replica_table.insert()

        # insert new versions
        with self._timer("insert_time", tags={"table": table.name}) as timer:
            objs.to_sql(table.name, connection, if_exists="append", index=False, schema=table.schema)
            timer.add_values(row_count=len(objs))
        if replica_stmt is not None:
            with self._timer("insert_time", tags={"table": replica_table_name}) as timer:
                connection.execute(replica_stmt, replica_data)
                timer.add_values(row_count=len(replica_data))

    def _storeDiaSources(
        self,
        sources: pandas.DataFrame,
        replica_chunk: ReplicaChunk | None,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        """Store catalog of DiaSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        """
        table = self._schema.get_table(ApdbTables.DiaSource)

        # Insert replica data
        replica_data: list[dict] = []
        replica_stmt: Any = None
        replica_table_name = ""
        if replica_chunk is not None:
            pk_names = [column.name for column in table.primary_key]
            replica_data = sources[pk_names].to_dict("records")
            for row in replica_data:
                row["apdb_replica_chunk"] = replica_chunk.id
            replica_table = self._schema.get_table(ExtraTables.DiaSourceChunks)
            replica_table_name = replica_table.name
            replica_stmt = replica_table.insert()

        # everything to be done in single transaction
        with self._timer("insert_time", tags={"table": table.name}) as timer:
            sources = _coerce_uint64(sources)
            sources.to_sql(table.name, connection, if_exists="append", index=False, schema=table.schema)
            timer.add_values(row_count=len(sources))
        if replica_stmt is not None:
            with self._timer("replica_insert_time", tags={"table": replica_table_name}) as timer:
                connection.execute(replica_stmt, replica_data)
                timer.add_values(row_count=len(replica_data))

    def _storeDiaForcedSources(
        self,
        sources: pandas.DataFrame,
        replica_chunk: ReplicaChunk | None,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        """Store a set of DiaForcedSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaForcedSource records
        """
        table = self._schema.get_table(ApdbTables.DiaForcedSource)

        # Insert replica data
        replica_data: list[dict] = []
        replica_stmt: Any = None
        replica_table_name = ""
        if replica_chunk is not None:
            pk_names = [column.name for column in table.primary_key]
            replica_data = sources[pk_names].to_dict("records")
            for row in replica_data:
                row["apdb_replica_chunk"] = replica_chunk.id
            replica_table = self._schema.get_table(ExtraTables.DiaForcedSourceChunks)
            replica_table_name = replica_table.name
            replica_stmt = replica_table.insert()

        # everything to be done in single transaction
        with self._timer("insert_time", tags={"table": table.name}) as timer:
            sources = _coerce_uint64(sources)
            sources.to_sql(table.name, connection, if_exists="append", index=False, schema=table.schema)
            timer.add_values(row_count=len(sources))
        if replica_stmt is not None:
            with self._timer("insert_time", tags={"table": replica_table_name}):
                connection.execute(replica_stmt, replica_data)
                timer.add_values(row_count=len(replica_data))

    def _htm_indices(self, region: Region) -> list[tuple[int, int]]:
        """Generate a set of HTM indices covering specified region.

        Parameters
        ----------
        region: `sphgeom.Region`
            Region that needs to be indexed.

        Returns
        -------
        Sequence of ranges, range is a tuple (minHtmID, maxHtmID).
        """
        _LOG.debug("region: %s", region)
        indices = self.pixelator.envelope(region, self.config.pixelization.htm_max_ranges)

        return indices.ranges()

    def _filterRegion(self, table: sqlalchemy.schema.Table, region: Region) -> sql.ColumnElement:
        """Make SQLAlchemy expression for selecting records in a region."""
        htm_index_column = table.columns[self.config.pixelization.htm_index_column]
        exprlist = []
        pixel_ranges = self._htm_indices(region)
        for low, upper in pixel_ranges:
            upper -= 1
            if low == upper:
                exprlist.append(htm_index_column == low)
            else:
                exprlist.append(sql.expression.between(htm_index_column, low, upper))

        return sql.expression.or_(*exprlist)

    def _add_spatial_index(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate spatial index for each record and add it to a DataFrame.

        Parameters
        ----------
        df : `pandas.DataFrame`
            DataFrame which has to contain ra/dec columns, names of these
            columns are defined by configuration ``ra_dec_columns`` field.

        Returns
        -------
        df : `pandas.DataFrame`
            DataFrame with ``pixelId`` column which contains pixel index
            for ra/dec coordinates.

        Notes
        -----
        This overrides any existing column in a DataFrame with the same name
        (pixelId). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        # calculate HTM index for every DiaObject
        htm_index = np.zeros(df.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (ra, dec) in enumerate(zip(df[ra_col], df[dec_col])):
            uv3d = UnitVector3d(LonLat.fromDegrees(ra, dec))
            idx = self.pixelator.index(uv3d)
            htm_index[i] = idx
        df = df.copy()
        df[self.config.pixelization.htm_index_column] = htm_index
        return df

    def _fix_input_timestamps(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Update timestamp columns in input DataFrame to be aware datetime
        type in in UTC.

        AP pipeline generates naive datetime instances, we want them to be
        aware before they go to database. All naive timestamps are assumed to
        be in UTC timezone (they should be TAI).
        """
        # Find all columns with aware non-UTC timestamps and convert to UTC.
        columns = [
            column
            for column, dtype in df.dtypes.items()
            if isinstance(dtype, pandas.DatetimeTZDtype) and dtype.tz is not datetime.timezone.utc
        ]
        for column in columns:
            df[column] = df[column].dt.tz_convert(datetime.timezone.utc)
        # Find all columns with naive timestamps and add UTC timezone.
        columns = [
            column for column, dtype in df.dtypes.items() if pandas.api.types.is_datetime64_dtype(dtype)
        ]
        for column in columns:
            df[column] = df[column].dt.tz_localize(datetime.timezone.utc)
        return df

    def _fix_result_timestamps(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Update timestamp columns to be naive datetime type in returned
        DataFrame.

        AP pipeline code expects DataFrames to contain naive datetime columns,
        while Postgres queries return timezone-aware type. This method converts
        those columns to naive datetime in UTC timezone.
        """
        # Find all columns with aware timestamps.
        columns = [column for column, dtype in df.dtypes.items() if isinstance(dtype, pandas.DatetimeTZDtype)]
        for column in columns:
            # tz_convert(None) will convert to UTC and drop timezone.
            df[column] = df[column].dt.tz_convert(None)
        return df
