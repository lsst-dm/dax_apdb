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

from typing import Any

from ..cassandra import ApdbCassandra


def create_cassandra(config_path: str, ra_dec_columns: str | None, **kwargs: Any) -> None:
    """Create new APDB instance in Cassandra cluster.

    Parameters
    ----------
    config_path : `str`
        Name of the file to write APDB configuration.
    ra_dec_columns : `str` or `None`
        Comma-separated list of names for ra/dec columns in DiaObject table.
    **kwargs
        Keyword arguments passed to `ApdbCassandra.init_database` method.
    """
    ra_dec_list: list[str] | None = None
    if ra_dec_columns:
        ra_dec_list = ra_dec_columns.split(",")
    kwargs["hosts"] = kwargs.pop("host")
    config = ApdbCassandra.init_database(ra_dec_columns=ra_dec_list, **kwargs)
    config.save(config_path)