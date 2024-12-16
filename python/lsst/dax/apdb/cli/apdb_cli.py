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

__all__ = ["main"]

import argparse
from collections.abc import Sequence

from .. import scripts
from . import options
from .logging_cli import LoggingCli


def main(args: Sequence[str] | None = None) -> None:
    """APDB command line tools."""
    parser = argparse.ArgumentParser(description="APDB command line tools")
    log_cli = LoggingCli(parser)

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _create_sql_subcommand(subparsers)
    _create_cassandra_subcommand(subparsers)
    _list_cassandra_subcommand(subparsers)
    _delete_cassandra_subcommand(subparsers)
    _list_index_subcommand(subparsers)
    _metadata_subcommand(subparsers)

    parsed_args = parser.parse_args(args)
    log_cli.process_args(parsed_args)

    kwargs = vars(parsed_args)
    # Strip keywords not understood by scripts.
    method = kwargs.pop("method")
    method(**kwargs)


def _create_sql_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("create-sql", help="Create new APDB instance in SQL database.")
    parser.add_argument("db_url", help="Database URL in SQLAlchemy format for APDB instance.")
    parser.add_argument("output_config", help="Name of the new configuration file for created APDB instance.")
    parser.add_argument(
        "instrument",
        help="Instrument name of the data that will be stored in this APDB instance."
        " This is the fully-qualified name, for example 'lsst.obs.lsst.LsstCam'.",
    )
    options.common_apdb_options(parser)
    options.sql_config_options(parser)
    parser.add_argument(
        "--drop", help="If True then drop existing tables.", default=False, action="store_true"
    )
    parser.set_defaults(method=scripts.create_sql)


def _create_cassandra_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("create-cassandra", help="Create new APDB instance in Cassandra cluster.")
    parser.add_argument("host", help="One or more host names for Cassandra cluster.", nargs="+")
    parser.add_argument(
        "keyspace", help="Cassandra keyspace name for APDB tables, will be created if does not exist."
    )
    parser.add_argument("output_config", help="Name of the new configuration file for created APDB instance.")
    parser.add_argument(
        "instrument",
        help="Instrument name of the data that will be stored in this APDB instance."
        " This is the fully-qualified name, for example 'lsst.obs.lsst.LsstCam'.",
    )
    options.common_apdb_options(parser)
    options.cassandra_config_options(parser)
    parser.add_argument(
        "--drop", help="If True then drop existing tables.", default=False, action="store_true"
    )
    parser.set_defaults(method=scripts.create_cassandra)


def _list_cassandra_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("list-cassandra", help="List APDB instances in Cassandra cluster.")
    parser.add_argument("host", help="One of the host names for Cassandra cluster.")
    parser.add_argument(
        "-v",
        "--verbose",
        help="Provide full list of roles and associated permissions.",
        default=False,
        action="store_true",
    )
    parser.set_defaults(method=scripts.list_cassandra)


def _delete_cassandra_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("delete-cassandra", help="Delete APDB instance from Cassandra cluster.")
    parser.add_argument("host", help="One of the host names for Cassandra cluster.")
    parser.add_argument("keyspace", help="Cassandra keyspace name for APDB tables.")
    parser.add_argument(
        "-y",
        "--confirm",
        help="Assume 'yes' answer for confirmation.",
        default=False,
        action="store_true",
    )
    parser.set_defaults(method=scripts.delete_cassandra)


def _list_index_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("list-index", help="List contents of APDB index file.")
    parser.add_argument(
        "index_path", help="Location of index file, if missing then $DAX_APDB_INDEX_URI is used.", nargs="?"
    )
    parser.set_defaults(method=scripts.list_index)


def _metadata_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("metadata", help="Operations with APDB metadata table.")
    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _metadata_set_subcommand(subparsers)
    _metadata_get_subcommand(subparsers)
    _metadata_show_subcommand(subparsers)
    _metadata_delete_subcommand(subparsers)


def _metadata_show_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("show", help="Show contents of APDB metadata table.")
    parser.add_argument(
        "-j",
        "--json",
        dest="use_json",
        help="Dump metadata in JSON format.",
        default=False,
        action="store_true",
    )
    parser.add_argument("config", help="Path or URI of APDB configuration file.")
    parser.set_defaults(method=scripts.metadata_show)


def _metadata_get_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("get", help="Print value of the metadata item.")
    parser.add_argument("config", help="Path or URI of APDB configuration file.")
    parser.add_argument("key", help="Metadata key, arbitrary string.")
    parser.set_defaults(method=scripts.metadata_get)


def _metadata_set_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("set", help="Add or update metadata item.")
    parser.add_argument(
        "-f",
        "--force",
        help="Force update of the existing key.",
        default=False,
        action="store_true",
    )
    parser.add_argument("config", help="Path or URI of APDB configuration file.")
    parser.add_argument("key", help="Metadata key, arbitrary string.")
    parser.add_argument("value", help="Corresponding metadata value.")
    parser.set_defaults(method=scripts.metadata_set)


def _metadata_delete_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("delete", help="Delete metadata item.")
    parser.add_argument("config", help="Path or URI of APDB configuration file.")
    parser.add_argument("key", help="Metadata key, arbitrary string.")
    parser.set_defaults(method=scripts.metadata_delete)
