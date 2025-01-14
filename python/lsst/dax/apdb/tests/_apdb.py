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

__all__ = ["ApdbSchemaUpdateTest", "ApdbTest", "update_schema_yaml"]

import contextlib
import datetime
import os
import tempfile
import unittest
from abc import ABC, abstractmethod
from collections.abc import Iterator
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

import astropy.time
import pandas
import yaml
from lsst.sphgeom import Angle, Circle, LonLat, Region, UnitVector3d

from .. import (
    Apdb,
    ApdbConfig,
    ApdbReplica,
    ApdbTableData,
    ApdbTables,
    IncompatibleVersionError,
    ReplicaChunk,
    VersionTuple,
)
from .data_factory import makeForcedSourceCatalog, makeObjectCatalog, makeSourceCatalog, makeSSObjectCatalog

if TYPE_CHECKING:
    from ..pixelization import Pixelization

    class TestCaseMixin(unittest.TestCase):
        """Base class for mixin test classes that use TestCase methods."""

else:

    class TestCaseMixin:
        """Do-nothing definition of mixin base class for regular execution."""


def _make_region(xyz: tuple[float, float, float] = (1.0, 1.0, -1.0)) -> Region:
    """Make a region to use in tests"""
    pointing_v = UnitVector3d(*xyz)
    fov = 0.05  # radians
    region = Circle(pointing_v, Angle(fov / 2))
    return region


@contextlib.contextmanager
def update_schema_yaml(
    schema_file: str,
    drop_metadata: bool = False,
    version: str | None = None,
) -> Iterator[str]:
    """Update schema definition and return name of the new schema file.

    Parameters
    ----------
    schema_file : `str`
        Path for the existing YAML file with APDB schema.
    drop_metadata : `bool`
        If `True` then remove metadata table from the list of tables.
    version : `str` or `None`
        If non-empty string then set schema version to this string, if empty
        string then remove schema version from config, if `None` - don't change
        the version in config.

    Yields
    ------
    Path for the updated configuration file.
    """
    with open(schema_file) as yaml_stream:
        schemas_list = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
    # Edit YAML contents.
    for schema in schemas_list:
        # Optionally drop metadata table.
        if drop_metadata:
            schema["tables"] = [table for table in schema["tables"] if table["name"] != "metadata"]
        if version is not None:
            if version == "":
                del schema["version"]
            else:
                schema["version"] = version

    with TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        output_path = os.path.join(tmpdir, "schema.yaml")
        with open(output_path, "w") as yaml_stream:
            yaml.dump_all(schemas_list, stream=yaml_stream)
        yield output_path


class ApdbTest(TestCaseMixin, ABC):
    """Base class for Apdb tests that can be specialized for concrete
    implementation.

    This can only be used as a mixin class for a unittest.TestCase and it
    calls various assert methods.
    """

    time_partition_tables = False
    visit_time = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")

    fsrc_requires_id_list = False
    """Should be set to True if getDiaForcedSources requires object IDs"""

    enable_replica: bool = False
    """Set to true when support for replication is configured"""

    schema_path: str
    """Location of the Felis schema file."""

    timestamp_type_name: str
    """Type name of timestamp columns in DataFrames returned from queries."""

    # number of columns as defined in tests/config/schema.yaml
    table_column_count = {
        ApdbTables.DiaObject: 8,
        ApdbTables.DiaObjectLast: 5,
        ApdbTables.DiaSource: 12,
        ApdbTables.DiaForcedSource: 8,
        ApdbTables.SSObject: 3,
    }

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make database instance and return configuration for it."""
        raise NotImplementedError()

    @abstractmethod
    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        raise NotImplementedError()

    @abstractmethod
    def pixelization(self, config: ApdbConfig) -> Pixelization:
        """Return pixelization used by implementation."""
        raise NotImplementedError()

    def assert_catalog(self, catalog: Any, rows: int, table: ApdbTables) -> None:
        """Validate catalog type and size

        Parameters
        ----------
        catalog : `object`
            Expected type of this is ``pandas.DataFrame``.
        rows : `int`
            Expected number of rows in a catalog.
        table : `ApdbTables`
            APDB table type.
        """
        self.assertIsInstance(catalog, pandas.DataFrame)
        self.assertEqual(catalog.shape[0], rows)
        self.assertEqual(catalog.shape[1], self.table_column_count[table])

    def assert_table_data(self, catalog: Any, rows: int, table: ApdbTables) -> None:
        """Validate catalog type and size

        Parameters
        ----------
        catalog : `object`
            Expected type of this is `ApdbTableData`.
        rows : `int`
            Expected number of rows in a catalog.
        table : `ApdbTables`
            APDB table type.
        extra_columns : `int`
            Count of additional columns expected in ``catalog``.
        """
        self.assertIsInstance(catalog, ApdbTableData)
        n_rows = sum(1 for row in catalog.rows())
        self.assertEqual(n_rows, rows)
        # One extra column for replica chunk id
        self.assertEqual(len(catalog.column_names()), self.table_column_count[table] + 1)

    def test_makeSchema(self) -> None:
        """Test for making APDB schema."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObjectLast))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaForcedSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.metadata))

        # Test from_uri factory method with the same config.
        with tempfile.NamedTemporaryFile() as tmpfile:
            config.save(tmpfile.name)
            apdb = Apdb.from_uri(tmpfile.name)

        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObjectLast))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaForcedSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.metadata))

    def test_empty_gets(self) -> None:
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """
        # use non-zero months for Forced/Source fetching
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time

        res: pandas.DataFrame | None

        # get objects by region
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 0, self.getDiaObjects_table())

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        res = apdb.getDiaSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaSources(region, [1, 2, 3], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaForcedSources(region, [1, 2, 3], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # data_factory's ccdVisitId generation corresponds to (1, 1)
        res = apdb.containsVisitDetector(visit=1, detector=1)
        self.assertFalse(res)

        # get sources by region
        if self.fsrc_requires_id_list:
            with self.assertRaises(NotImplementedError):
                apdb.getDiaForcedSources(region, None, visit_time)
        else:
            res = apdb.getDiaForcedSources(region, None, visit_time)
            self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

    def test_empty_gets_0months(self) -> None:
        """Test for getting data from empty database.

        All get() methods should return empty DataFrame or None.
        """
        # set read_sources_months to 0 so that Forced/Sources are None
        config = self.make_instance(read_sources_months=0, read_forced_sources_months=0)
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time

        res: pandas.DataFrame | None

        # get objects by region
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 0, self.getDiaObjects_table())

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self.assertIs(res, None)

        # get sources by object ID, empty object list
        res = apdb.getDiaSources(region, [], visit_time)
        self.assertIs(res, None)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assertIs(res, None)

        # Database is empty, no images exist.
        res = apdb.containsVisitDetector(visit=1, detector=1)
        self.assertFalse(res)

    def test_storeObjects(self) -> None:
        """Store and retrieve DiaObjects."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time

        # make catalog with Objects
        catalog = makeObjectCatalog(region, 100, visit_time)

        # store catalog
        apdb.store(visit_time, catalog)

        # read it back and check sizes
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, len(catalog), self.getDiaObjects_table())

        # TODO: test apdb.contains with generic implementation from DM-41671

    def test_storeObjects_empty(self) -> None:
        """Test calling storeObject when there are no objects: see DM-43270."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        region = _make_region()
        visit_time = self.visit_time
        # make catalog with no Objects
        catalog = makeObjectCatalog(region, 0, visit_time)

        with self.assertLogs("lsst.dax.apdb", level="DEBUG") as cm:
            apdb.store(visit_time, catalog)
        self.assertIn("No objects", "\n".join(cm.output))

    def test_storeMovingObject(self) -> None:
        """Store and retrieve DiaObject which changes its position."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        pixelization = self.pixelization(config)

        lon_deg, lat_deg = 0.0, 0.0
        lonlat1 = LonLat.fromDegrees(lon_deg - 1.0, lat_deg)
        lonlat2 = LonLat.fromDegrees(lon_deg + 1.0, lat_deg)
        uv1 = UnitVector3d(lonlat1)
        uv2 = UnitVector3d(lonlat2)

        # Check that they fall into different pixels.
        self.assertNotEqual(pixelization.pixel(uv1), pixelization.pixel(uv2))

        # Store one object at two different positions.
        visit_time1 = self.visit_time
        catalog1 = makeObjectCatalog(lonlat1, 1, visit_time1)
        apdb.store(visit_time1, catalog1)

        visit_time2 = visit_time1 + astropy.time.TimeDelta(120.0, format="sec")
        catalog1 = makeObjectCatalog(lonlat2, 1, visit_time2)
        apdb.store(visit_time2, catalog1)

        # Make region covering both points.
        region = Circle(UnitVector3d(LonLat.fromDegrees(lon_deg, lat_deg)), Angle.fromDegrees(1.1))
        self.assertTrue(region.contains(uv1))
        self.assertTrue(region.contains(uv2))

        # Read it back, must return the latest one.
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 1, self.getDiaObjects_table())

    def test_storeSources(self) -> None:
        """Store and retrieve DiaSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100, visit_time)
        oids = list(objects["diaObjectId"])
        sources = makeSourceCatalog(objects, visit_time)

        # save the objects and sources
        apdb.store(visit_time, objects, sources)

        # read it back, no ID filtering
        res = apdb.getDiaSources(region, None, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)

        # read it back and filter by ID
        res = apdb.getDiaSources(region, oids, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)

        # read it back to get schema
        res = apdb.getDiaSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # test if a visit is present
        # data_factory's ccdVisitId generation corresponds to (1, 1)
        res = apdb.containsVisitDetector(visit=1, detector=1)
        self.assertTrue(res)
        # non-existent image
        res = apdb.containsVisitDetector(visit=2, detector=42)
        self.assertFalse(res)

    def test_storeForcedSources(self) -> None:
        """Store and retrieve DiaForcedSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100, visit_time)
        oids = list(objects["diaObjectId"])
        catalog = makeForcedSourceCatalog(objects, visit_time)

        apdb.store(visit_time, objects, forced_sources=catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(region, oids, visit_time)
        self.assert_catalog(res, len(catalog), ApdbTables.DiaForcedSource)

        # read it back to get schema
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # data_factory's ccdVisitId generation corresponds to (1, 1)
        res = apdb.containsVisitDetector(visit=1, detector=1)
        self.assertTrue(res)
        # non-existent image
        res = apdb.containsVisitDetector(visit=2, detector=42)
        self.assertFalse(res)

    def test_timestamps(self) -> None:
        """Check that timestamp return type is as expected."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time

        # have to store Objects first
        time_before = datetime.datetime.now()
        # Cassandra has a millisecond precision, so subtract 1ms to allow for
        # truncated returned values.
        time_before -= datetime.timedelta(milliseconds=1)
        objects = makeObjectCatalog(region, 100, visit_time)
        oids = list(objects["diaObjectId"])
        catalog = makeForcedSourceCatalog(objects, visit_time)
        time_after = datetime.datetime.now()

        apdb.store(visit_time, objects, forced_sources=catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(region, oids, visit_time)
        assert res is not None
        self.assert_catalog(res, len(catalog), ApdbTables.DiaForcedSource)

        self.assertIn("time_processed", res.dtypes)
        dtype = res.dtypes["time_processed"]
        self.assertEqual(dtype.name, self.timestamp_type_name)
        # Verify that returned time is sensible.
        self.assertTrue(all(time_before <= dt <= time_after for dt in res["time_processed"]))

    def test_getChunks(self) -> None:
        """Store and retrieve replica chunks."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        apdb_replica = ApdbReplica.from_config(config)
        visit_time = self.visit_time

        region1 = _make_region((1.0, 1.0, -1.0))
        region2 = _make_region((-1.0, -1.0, -1.0))
        nobj = 100
        objects1 = makeObjectCatalog(region1, nobj, visit_time)
        objects2 = makeObjectCatalog(region2, nobj, visit_time, start_id=nobj * 2)

        # With the default 10 minutes replica chunk window we should have 4
        # records.
        visits = [
            (astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:02:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:11:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:12:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:45:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:46:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-03-01T00:01:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-03-01T00:02:00", format="isot", scale="tai"), objects2),
        ]

        start_id = 0
        for visit_time, objects in visits:
            sources = makeSourceCatalog(objects, visit_time, start_id=start_id)
            fsources = makeForcedSourceCatalog(objects, visit_time, visit=start_id)
            apdb.store(visit_time, objects, sources, fsources)
            start_id += nobj

        replica_chunks = apdb_replica.getReplicaChunks()
        if not self.enable_replica:
            self.assertIsNone(replica_chunks)

            with self.assertRaisesRegex(ValueError, "APDB is not configured for replication"):
                apdb_replica.getDiaObjectsChunks([])

        else:
            assert replica_chunks is not None
            self.assertEqual(len(replica_chunks), 4)

            def _check_chunks(replica_chunks: list[ReplicaChunk], n_records: int | None = None) -> None:
                if n_records is None:
                    n_records = len(replica_chunks) * nobj
                res = apdb_replica.getDiaObjectsChunks(chunk.id for chunk in replica_chunks)
                self.assert_table_data(res, n_records, ApdbTables.DiaObject)
                res = apdb_replica.getDiaSourcesChunks(chunk.id for chunk in replica_chunks)
                self.assert_table_data(res, n_records, ApdbTables.DiaSource)
                res = apdb_replica.getDiaForcedSourcesChunks(chunk.id for chunk in replica_chunks)
                self.assert_table_data(res, n_records, ApdbTables.DiaForcedSource)

            # read it back and check sizes
            _check_chunks(replica_chunks, 800)
            _check_chunks(replica_chunks[1:], 600)
            _check_chunks(replica_chunks[1:-1], 400)
            _check_chunks(replica_chunks[2:3], 200)
            _check_chunks([])

            # try to remove some of those
            deleted_chunks = replica_chunks[:1]
            apdb_replica.deleteReplicaChunks(chunk.id for chunk in deleted_chunks)

            # All queries on deleted ids should return empty set.
            _check_chunks(deleted_chunks, 0)

            replica_chunks = apdb_replica.getReplicaChunks()
            assert replica_chunks is not None
            self.assertEqual(len(replica_chunks), 3)

            _check_chunks(replica_chunks, 600)

    def test_storeSSObjects(self) -> None:
        """Store and retrieve SSObjects."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        # make catalog with SSObjects
        catalog = makeSSObjectCatalog(100, flags=1)

        # store catalog
        apdb.storeSSObjects(catalog)

        # read it back and check sizes
        res = apdb.getSSObjects()
        self.assert_catalog(res, len(catalog), ApdbTables.SSObject)

        # check that override works, make catalog with SSObjects, ID = 51-150
        catalog = makeSSObjectCatalog(100, 51, flags=2)
        apdb.storeSSObjects(catalog)
        res = apdb.getSSObjects()
        self.assert_catalog(res, 150, ApdbTables.SSObject)
        self.assertEqual(len(res[res["flags"] == 1]), 50)
        self.assertEqual(len(res[res["flags"] == 2]), 100)

    def test_reassignObjects(self) -> None:
        """Reassign DiaObjects."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        visit_time = self.visit_time
        objects = makeObjectCatalog(region, 100, visit_time)
        oids = list(objects["diaObjectId"])
        sources = makeSourceCatalog(objects, visit_time)
        apdb.store(visit_time, objects, sources)

        catalog = makeSSObjectCatalog(100)
        apdb.storeSSObjects(catalog)

        # read it back and filter by ID
        res = apdb.getDiaSources(region, oids, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)

        apdb.reassignDiaSources({1: 1, 2: 2, 5: 5})
        res = apdb.getDiaSources(region, oids, visit_time)
        self.assert_catalog(res, len(sources) - 3, ApdbTables.DiaSource)

        with self.assertRaisesRegex(ValueError, r"do not exist.*\D1000"):
            apdb.reassignDiaSources(
                {
                    1000: 1,
                    7: 3,
                }
            )
        self.assert_catalog(res, len(sources) - 3, ApdbTables.DiaSource)

    def test_midpointMjdTai_src(self) -> None:
        """Test for time filtering of DiaSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        # 2021-01-01 plus 360 days is 2021-12-27
        src_time1 = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")
        src_time2 = astropy.time.Time("2021-01-01T00:00:02", format="isot", scale="tai")
        visit_time0 = astropy.time.Time("2021-12-26T23:59:59", format="isot", scale="tai")
        visit_time1 = astropy.time.Time("2021-12-27T00:00:01", format="isot", scale="tai")
        visit_time2 = astropy.time.Time("2021-12-27T00:00:03", format="isot", scale="tai")

        objects = makeObjectCatalog(region, 100, visit_time0)
        oids = list(objects["diaObjectId"])
        sources = makeSourceCatalog(objects, src_time1, 0)
        apdb.store(src_time1, objects, sources)

        sources = makeSourceCatalog(objects, src_time2, 100)
        apdb.store(src_time2, objects, sources)

        # reading at time of last save should read all
        res = apdb.getDiaSources(region, oids, src_time2)
        self.assert_catalog(res, 200, ApdbTables.DiaSource)

        # one second before 12 months
        res = apdb.getDiaSources(region, oids, visit_time0)
        self.assert_catalog(res, 200, ApdbTables.DiaSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaSources(region, oids, visit_time1)
        self.assert_catalog(res, 100, ApdbTables.DiaSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaSources(region, oids, visit_time2)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

    def test_midpointMjdTai_fsrc(self) -> None:
        """Test for time filtering of DiaForcedSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = _make_region()
        src_time1 = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")
        src_time2 = astropy.time.Time("2021-01-01T00:00:02", format="isot", scale="tai")
        visit_time0 = astropy.time.Time("2021-12-26T23:59:59", format="isot", scale="tai")
        visit_time1 = astropy.time.Time("2021-12-27T00:00:01", format="isot", scale="tai")
        visit_time2 = astropy.time.Time("2021-12-27T00:00:03", format="isot", scale="tai")

        objects = makeObjectCatalog(region, 100, visit_time0)
        oids = list(objects["diaObjectId"])
        sources = makeForcedSourceCatalog(objects, src_time1, 1)
        apdb.store(src_time1, objects, forced_sources=sources)

        sources = makeForcedSourceCatalog(objects, src_time2, 2)
        apdb.store(src_time2, objects, forced_sources=sources)

        # reading at time of last save should read all
        res = apdb.getDiaForcedSources(region, oids, src_time2)
        self.assert_catalog(res, 200, ApdbTables.DiaForcedSource)

        # one second before 12 months
        res = apdb.getDiaForcedSources(region, oids, visit_time0)
        self.assert_catalog(res, 200, ApdbTables.DiaForcedSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaForcedSources(region, oids, visit_time1)
        self.assert_catalog(res, 100, ApdbTables.DiaForcedSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaForcedSources(region, oids, visit_time2)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

    def test_metadata(self) -> None:
        """Simple test for writing/reading metadata table"""
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        metadata = apdb.metadata

        # APDB should write two or three metadata items with version numbers
        # and a frozen JSON config.
        self.assertFalse(metadata.empty())
        expected_rows = 4 if self.enable_replica else 3
        self.assertEqual(len(list(metadata.items())), expected_rows)

        metadata.set("meta", "data")
        metadata.set("data", "meta")

        self.assertFalse(metadata.empty())
        self.assertTrue(set(metadata.items()) >= {("meta", "data"), ("data", "meta")})

        with self.assertRaisesRegex(KeyError, "Metadata key 'meta' already exists"):
            metadata.set("meta", "data1")

        metadata.set("meta", "data2", force=True)
        self.assertTrue(set(metadata.items()) >= {("meta", "data2"), ("data", "meta")})

        self.assertTrue(metadata.delete("meta"))
        self.assertIsNone(metadata.get("meta"))
        self.assertFalse(metadata.delete("meta"))

        self.assertEqual(metadata.get("data"), "meta")
        self.assertEqual(metadata.get("meta", "meta"), "meta")

    def test_nometadata(self) -> None:
        """Test case for when metadata table is missing"""
        # We expect that schema includes metadata table, drop it.
        with update_schema_yaml(self.schema_path, drop_metadata=True) as schema_file:
            config = self.make_instance(schema_file=schema_file)
            apdb = Apdb.from_config(config)
            metadata = apdb.metadata

            self.assertTrue(metadata.empty())
            self.assertEqual(list(metadata.items()), [])
            with self.assertRaisesRegex(RuntimeError, "Metadata table does not exist"):
                metadata.set("meta", "data")

            self.assertTrue(metadata.empty())
            self.assertIsNone(metadata.get("meta"))

        # Also check what happens when configured schema has metadata, but
        # database is missing it. Database was initialized inside above context
        # without metadata table, here we use schema config which includes
        # metadata table.
        config.schema_file = self.schema_path
        apdb = Apdb.from_config(config)
        metadata = apdb.metadata
        self.assertTrue(metadata.empty())

    def test_schemaVersionFromYaml(self) -> None:
        """Check version number handling for reading schema from YAML."""
        config = self.make_instance()
        default_schema = config.schema_file
        apdb = Apdb.from_config(config)
        self.assertEqual(apdb._schema.schemaVersion(), VersionTuple(0, 1, 1))  # type: ignore[attr-defined]

        with update_schema_yaml(default_schema, version="") as schema_file:
            config = self.make_instance(schema_file=schema_file)
            apdb = Apdb.from_config(config)
            self.assertEqual(
                apdb._schema.schemaVersion(), VersionTuple(0, 1, 0)  # type: ignore[attr-defined]
            )

        with update_schema_yaml(default_schema, version="99.0.0") as schema_file:
            config = self.make_instance(schema_file=schema_file)
            apdb = Apdb.from_config(config)
            self.assertEqual(
                apdb._schema.schemaVersion(), VersionTuple(99, 0, 0)  # type: ignore[attr-defined]
            )

    def test_config_freeze(self) -> None:
        """Test that some config fields are correctly frozen in database."""
        config = self.make_instance()

        # `enable_replica` is the only parameter that is frozen in all
        # implementations.
        config.enable_replica = not self.enable_replica
        apdb = Apdb.from_config(config)
        frozen_config = apdb.config  # type: ignore[attr-defined]
        self.assertEqual(frozen_config.enable_replica, self.enable_replica)


class ApdbSchemaUpdateTest(TestCaseMixin, ABC):
    """Base class for unit tests that verify how schema changes work."""

    visit_time = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests.

        This method should return configuration that point to the identical
        database instance on each call (i.e. ``db_url`` must be the same,
        which also means for sqlite it has to use on-disk storage).
        """
        raise NotImplementedError()

    def test_schema_add_replica(self) -> None:
        """Check that new code can work with old schema without replica
        tables.
        """
        # Make schema without replica tables.
        config = self.make_instance(enable_replica=False)
        apdb = Apdb.from_config(config)
        apdb_replica = ApdbReplica.from_config(config)

        # Make APDB instance configured for replication.
        config.enable_replica = True
        apdb = Apdb.from_config(config)

        # Try to insert something, should work OK.
        region = _make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100, visit_time)
        sources = makeSourceCatalog(objects, visit_time)
        fsources = makeForcedSourceCatalog(objects, visit_time)
        apdb.store(visit_time, objects, sources, fsources)

        # There should be no replica chunks.
        replica_chunks = apdb_replica.getReplicaChunks()
        self.assertIsNone(replica_chunks)

    def test_schemaVersionCheck(self) -> None:
        """Check version number compatibility."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        self.assertEqual(apdb._schema.schemaVersion(), VersionTuple(0, 1, 1))  # type: ignore[attr-defined]

        # Claim that schema version is now 99.0.0, must raise an exception.
        with update_schema_yaml(config.schema_file, version="99.0.0") as schema_file:
            config.schema_file = schema_file
            with self.assertRaises(IncompatibleVersionError):
                apdb = Apdb.from_config(config)
