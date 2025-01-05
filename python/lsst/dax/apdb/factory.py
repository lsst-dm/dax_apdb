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

__all__ = ["apdb_type", "config_type_for_name", "make_apdb"]

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .apdb import Apdb
    from .apdbReplica import ApdbReplica
    from .cassandra import ApdbCassandra
    from .config import ApdbConfig
    from .sql import ApdbSql


def apdb_type(config: ApdbConfig) -> type[ApdbSql | ApdbCassandra]:
    """Return Apdb type on Apdb configuration.

    Parameters
    ----------
    config : `ApdbConfig`
        Configuration object, sub-class of ApdbConfig.

    Returns
    -------
    type : `type` [`Apdb`]
        Subclass of `Apdb` class.

    Raises
    ------
    TypeError
        Raised if type of ``config`` does not match any known types.
    """
    from .cassandra import ApdbCassandra, ApdbCassandraConfig
    from .sql import ApdbSql, ApdbSqlConfig

    if type(config) is ApdbSqlConfig:
        return ApdbSql
    elif type(config) is ApdbCassandraConfig:
        return ApdbCassandra
    raise TypeError(f"Unknown type of config object: {type(config)}")


def make_apdb(config: ApdbConfig) -> Apdb:
    """Create Apdb instance based on Apdb configuration.

    Parameters
    ----------
    config : `ApdbConfig`
        Configuration object, sub-class of ApdbConfig.

    Returns
    -------
    apdb : `Apdb`
        Instance of a specific Apdb sub-class.

    Raises
    ------
    TypeError
        Raised if type of ``config`` does not match any known types.
    """
    from .cassandra import ApdbCassandra, ApdbCassandraConfig
    from .sql import ApdbSql, ApdbSqlConfig

    if type(config) is ApdbSqlConfig:
        return ApdbSql(config)
    elif type(config) is ApdbCassandraConfig:
        return ApdbCassandra(config)
    raise TypeError(f"Unknown type of config object: {type(config)}")


def make_apdb_replica(config: ApdbConfig) -> ApdbReplica:
    """Create ApdbReplica instance based on Apdb configuration.

    Parameters
    ----------
    config : `ApdbConfig`
        Configuration object, sub-class of ApdbConfig.

    Returns
    -------
    apdb_replica : `ApdbReplica`
        Instance of a specific ApdbReplica sub-class.

    Raises
    ------
    TypeError
        Raised if type of ``config`` does not match any known types.
    """
    from .cassandra import ApdbCassandra, ApdbCassandraConfig
    from .sql import ApdbSql, ApdbSqlConfig

    if type(config) is ApdbSqlConfig:
        return ApdbSql(config).get_replica()
    elif type(config) is ApdbCassandraConfig:
        return ApdbCassandra(config).get_replica()
    raise TypeError(f"Unknown type of config object: {type(config)}")


def config_type_for_name(type_name: str) -> type[ApdbConfig]:
    """Return ApdbConfig class matching type name.

    Parameters
    ----------
    type_name : `str`
        Short type name of Apdb implementation, for now "sql" and "cassandra"
        are supported.

    Returns
    -------
    type : `type` [`ApdbConfig`]
        Subclass of `ApdbConfig` class.

    Raises
    ------
    TypeError
        Raised if ``type_name`` does not match any known types.
    """
    if type_name == "sql":
        from .sql import ApdbSqlConfig

        return ApdbSqlConfig
    elif type_name == "cassandra":
        from .cassandra import ApdbCassandraConfig

        return ApdbCassandraConfig

    raise TypeError(f"Unknown Apdb implementation type name: {type_name}")
