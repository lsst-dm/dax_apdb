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

"""Unit test for apdb-cli commands."""

import os
import shutil
import tempfile
import unittest

from lsst.dax.apdb import Apdb
from lsst.dax.apdb.cli import apdb_cli

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class CreateSqlTestCase(unittest.TestCase):
    """A test case for apdb-cli create-sql command."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"
        self.config_path = f"{self.tempdir}/apdb-sql.py"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_create_sql(self) -> None:
        """Create SQLite APDB instance and config file for it."""
        args = [
            "create-sql",
            "--schema-file",
            TEST_SCHEMA,
            self.db_url,
            self.config_path,
            "lsst.obs.lsst.LsstCam",
        ]
        apdb_cli.main(args)
        self.assertTrue(os.path.exists(f"{self.tempdir}/apdb.sqlite3"))
        self.assertTrue(os.path.exists(self.config_path))

        # Make Apdb instance from this new config.
        apdb = Apdb.from_uri(self.config_path)
        self.assertEqual(apdb.metadata.get("instrument"), "lsst.obs.lsst.LsstCam")

    def test_create_sql_bad_instrument(self) -> None:
        """Check that bad instrument names raise a useful error.

        Only works if both pipe_base and obs_lsst are available.
        """
        try:
            import lsst.pipe.base  # noqa: F401
        except ModuleNotFoundError:
            raise unittest.SkipTest("pipe_base and obs_lsst must be setup to run this tests.")
        args = [
            "create-sql",
            "--schema-file",
            TEST_SCHEMA,
            self.db_url,
            self.config_path,
            "lsst.obs.lsst.LsStCam",
        ]
        with self.assertRaisesRegex(RuntimeError, "invalid or unknown instrument name"):
            apdb_cli.main(args)


if __name__ == "__main__":
    unittest.main()
