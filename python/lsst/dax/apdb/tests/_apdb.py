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

__all__ = ["ApdbTest"]

from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas

from lsst.daf.base import DateTime
from lsst.dax.apdb import ApdbConfig, ApdbTables, make_apdb
from lsst.sphgeom import Angle, Circle, Region, UnitVector3d
from .data_factory import makeObjectCatalog, makeForcedSourceCatalog, makeSourceCatalog


class ApdbTest(ABC):
    """Base class for Apdb tests that can be specialized for concrete
    implementation.

    This can only be used as a mixin class for a unittest.TestCase and it
    calls various assert methods.
    """

    time_partition_tables = False
    visit_time = DateTime("2021-01-01T00:00:00", DateTime.TAI)

    fsrc_requires_id_list = False
    """Should be set to True if getDiaForcedSources requires object IDs"""

    # number of columns as defined in schema YAML files
    n_obj_columns = 91 + 2  # schema + schema-extra
    n_obj_last_columns = 17
    n_src_columns = 107
    n_fsrc_columns = 8

    @abstractmethod
    def make_config(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        raise NotImplementedError()

    @abstractmethod
    def n_columns(self, table: ApdbTables) -> int:
        """Return number of columns for a specified table."""
        raise NotImplementedError()

    def make_region(self) -> Region:
        """Make a region used in tests"""
        pointing_v = UnitVector3d(1., 1., -1.)
        fov = 0.05  # radians
        region = Circle(pointing_v, Angle(fov/2))
        return region

    def assert_catalog(self, catalog: Any, rows: int, table: ApdbTables) -> None:
        """Validate catalog type and size

        Parameters
        ----------
        catalog : `object`
            Expected type of this is ``type``.
        rows : int
            Expected number of rows in a catalog.
        table : `ApdbTables`
            APDB table type.
        """
        self.assertIsInstance(catalog, pandas.DataFrame)  # type: ignore[attr-defined]
        self.assertEqual(catalog.shape[0], rows)  # type: ignore[attr-defined]
        self.assertEqual(catalog.shape[1], self.n_columns(table))  # type: ignore[attr-defined]

    def test_makeSchema(self) -> None:
        """Test for makeing APDB schema."""
        config = self.make_config()
        apdb = make_apdb(config)

        apdb.makeSchema()
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject))  # type: ignore[attr-defined]
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObjectLast))  # type: ignore[attr-defined]
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaSource))  # type: ignore[attr-defined]
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaForcedSource))  # type: ignore[attr-defined]

    def test_empty_gets(self) -> None:
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # use non-zero months for Forced/Source fetching
        config = self.make_config()
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        visit_time = self.visit_time

        res: Optional[pandas.DataFrame]

        # get objects by region
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 0, ApdbTables.DiaObject)

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

        # get sources by region
        if self.fsrc_requires_id_list:
            with self.assertRaises(NotImplementedError):  # type: ignore[attr-defined]
                apdb.getDiaForcedSources(region, None, visit_time)
        else:
            apdb.getDiaForcedSources(region, None, visit_time)
            self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

    def test_empty_gets_0months(self) -> None:
        """Test for getting data from empty database.

        All get() methods should return empty DataFrame or None.
        """

        # set read_sources_months to 0 so that Forced/Sources are None
        config = self.make_config(read_sources_months=0,
                                  read_forced_sources_months=0)
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        visit_time = self.visit_time

        res: Optional[pandas.DataFrame]

        # get objects by region
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 0, ApdbTables.DiaObject)

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self.assertIs(res, None)  # type: ignore[attr-defined]

        # get sources by object ID, empty object list
        res = apdb.getDiaSources(region, [], visit_time)
        self.assertIs(res, None)  # type: ignore[attr-defined]

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assertIs(res, None)  # type: ignore[attr-defined]

    def test_storeObjects(self) -> None:
        """Store and retrieve DiaObjects."""

        # don't care about sources.
        config = self.make_config()
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        visit_time = self.visit_time

        # make catalog with Objects
        catalog = makeObjectCatalog(region, 100)

        # store catalog
        apdb.store(visit_time, catalog)

        # read it back and check sizes
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, len(catalog), ApdbTables.DiaObject)

    def test_storeSources(self) -> None:
        """Store and retrieve DiaSources."""
        config = self.make_config()
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100)
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

    def test_storeForcedSources(self) -> None:
        """Store and retrieve DiaForcedSources."""

        config = self.make_config()
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        catalog = makeForcedSourceCatalog(objects, visit_time)

        apdb.store(visit_time, objects, forced_sources=catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(region, oids, visit_time)
        self.assert_catalog(res, len(catalog), ApdbTables.DiaForcedSource)

        # read it back to get schema
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

    def test_midPointTai_src(self) -> None:
        """Test for time filtering of DiaSources.
        """
        config = self.make_config()
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        # 2021-01-01 plus 360 days is 2021-12-27
        src_time1 = DateTime("2021-01-01T00:00:00", DateTime.TAI)
        src_time2 = DateTime("2021-01-01T00:00:02", DateTime.TAI)
        visit_time0 = DateTime("2021-12-26T23:59:59", DateTime.TAI)
        visit_time1 = DateTime("2021-12-27T00:00:01", DateTime.TAI)
        visit_time2 = DateTime("2021-12-27T00:00:03", DateTime.TAI)

        objects = makeObjectCatalog(region, 100)
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

    def test_midPointTai_fsrc(self) -> None:
        """Test for time filtering of DiaForcedSources.
        """
        config = self.make_config()
        apdb = make_apdb(config)
        apdb.makeSchema()

        region = self.make_region()
        src_time1 = DateTime("2021-01-01T00:00:00", DateTime.TAI)
        src_time2 = DateTime("2021-01-01T00:00:02", DateTime.TAI)
        visit_time0 = DateTime("2021-12-26T23:59:59", DateTime.TAI)
        visit_time1 = DateTime("2021-12-27T00:00:01", DateTime.TAI)
        visit_time2 = DateTime("2021-12-27T00:00:03", DateTime.TAI)

        objects = makeObjectCatalog(region, 100)
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