# This file is part of dax_apdb
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

from __future__ import annotations

__all__ = ["create_cassandra"]

import io
import warnings
from typing import Any

import yaml
from lsst.resources import ResourcePath

from ..cassandra import ApdbCassandra, CreateTableOptions


def create_cassandra(
    output_config: str, ra_dec_columns: str | None, table_options: str | None = None, **kwargs: Any
) -> None:
    """Create new APDB instance in Cassandra cluster.

    Parameters
    ----------
    output_config : `str`
        Name of the file to write APDB configuration.
    ra_dec_columns : `str` or `None`
        Comma-separated list of names for ra/dec columns in DiaObject table.
    table_options : `str`, optional
        Path or URI of the YAML file with table options.
    **kwargs
        Keyword arguments passed to `ApdbCassandra.init_database` method.
    """
    options = _read_table_options(table_options)
    ra_dec_list: list[str] | None = None
    if ra_dec_columns:
        ra_dec_list = ra_dec_columns.split(",")
    kwargs["hosts"] = kwargs.pop("host")
    config = ApdbCassandra.init_database(ra_dec_columns=ra_dec_list, table_options=options, **kwargs)
    config.save(output_config)
    if output_config.endswith(".py"):
        warnings.warn(
            "APDB configuration is now saved in YAML format, "
            "output file should use .yaml extension instead of .py."
        )


def _read_table_options(table_options: str | None = None) -> CreateTableOptions | None:
    """Read contents of the table options file."""
    if not table_options:
        return None

    res = ResourcePath(table_options)
    content = res.read()
    stream = io.BytesIO(content)
    if model_data := yaml.load(stream, Loader=yaml.SafeLoader):
        return CreateTableOptions.model_validate(model_data)
    return None
