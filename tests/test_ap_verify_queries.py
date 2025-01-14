# This file is part of dax_apdb.
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

import os
import unittest.mock
from collections.abc import Mapping
from typing import Any

import astropy.time
import lsst.utils.tests
import numpy
import pandas
from lsst.dax.apdb.sql import ApdbSql, ApdbSqlConfig

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


def createTestObjects(
    n_objects: int, id_column_name: str, extra_fields: Mapping[str, Any]
) -> pandas.DataFrame:
    """Create test objects to store in the ApdbSql.

    Parameters
    ----------
    n_objects : `int`
        Number of objects to create.
    id_column_name : `str`
        Name of the ID column.
    extra_fields : `dict`
        A `dict` whose keys are field names and whose values are their types.

    Returns
    -------
    sources : `pandas.DataFrame`
        Tests sources with filled values.
    """
    one_degree = numpy.pi / 180
    data = {
        id_column_name: numpy.arange(n_objects, dtype=numpy.int64),
        "ra": numpy.full(n_objects, one_degree, dtype=numpy.float64),
        "dec": numpy.full(n_objects, one_degree, dtype=numpy.float64),
    }
    for field, type in extra_fields.items():
        data[field] = numpy.ones(n_objects, dtype=type)
    df = pandas.DataFrame(data)
    return df


class TestApVerifyQueries(unittest.TestCase):
    """Tests for ap_verify queries."""

    def setUp(self) -> None:
        # Create DB in memory.
        self.apdbCfg = ApdbSqlConfig(
            db_url="sqlite://",
            schema_file=TEST_SCHEMA,
            dia_object_index="baseline",
            dia_object_columns=[],
        )
        self.apdb = ApdbSql(config=self.apdbCfg)
        self.apdb._schema.makeSchema()

    def tearDown(self) -> None:
        del self.apdb

    def test_count_zero_objects(self) -> None:
        value = self.apdb.countUnassociatedObjects()
        self.assertEqual(value, 0)

    def test_count_objects(self) -> None:
        n_created = 5
        objects = createTestObjects(n_created, "diaObjectId", {"nDiaSources": int})
        objects.at[n_created - 1, "nDiaSources"] = 2

        dateTime = astropy.time.Time(1400000000, format="unix_tai")
        self.apdb.store(dateTime, objects)

        value = self.apdb.countUnassociatedObjects()
        self.assertEqual(n_created - 1, value)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module: Any) -> None:
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
