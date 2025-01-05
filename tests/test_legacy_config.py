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

import os
import shutil
import tempfile
import unittest

from lsst.dax.apdb.sql import ApdbSql

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class LegacyConfigTestCase(unittest.TestCase):
    """Test case for using old pex_config files with a new config system.

    This test will be removed when we drop support for legacy configs.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_lecagy_config(self) -> None:
        """Create SQLite APDB instance open it using old pex_config file."""
        new_config = ApdbSql.init_database(self.db_url, schema_file=TEST_SCHEMA)

        # Generate old pex_config file, old config class existed at two
        # different locations.
        config_locations = ["lsst.dax.apdb.apdbSql", "lsst.dax.apdb.sql.apdbSql"]
        for location in config_locations:
            legacy_config_str = (
                f"import {location}\n"
                f'assert type(config) is {location}.ApdbSqlConfig, ""\n'
                f'config.db_url="{new_config.db_url}"\n'
                f'config.schema_file="{TEST_SCHEMA}"\n'
            )
            legacy_config_path = os.path.join(self.tempdir, "legacy_config.py")
            with open(legacy_config_path, "w") as file:
                file.write(legacy_config_str)

            # Make APDB instance using legacy config.
            warning_message = "APDB is instantiated using legacy pex_config format"
            with self.assertWarnsRegex(FutureWarning, warning_message):
                ApdbSql.from_uri(legacy_config_path)


if __name__ == "__main__":
    unittest.main()
