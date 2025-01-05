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

__all__ = ["ApdbSqlConfig", "ApdbSqlConnectionConfig", "ApdbSqlPixelizationConfig"]

from typing import Any, ClassVar

from pydantic import BaseModel, Field, field_validator

from ..config import ApdbConfig


class ApdbSqlConnectionConfig(BaseModel):
    """Configuration of connection parameters."""

    isolation_level: str | None = Field(
        default=None,
        description=(
            "Transaction isolation level, if unset then backend-default value "
            "is used, except for SQLite backend where we use READ_UNCOMMITTED. "
            "Some backends may not support every allowed value."
        ),
    )

    connection_pool: bool = Field(
        default=True,
        description=(
            "If False then disable SQLAlchemy connection pool. " "Do not use connection pool when forking."
        ),
    )

    connection_timeout: float | None = Field(
        default=None,
        description=(
            "Maximum time to wait time for database lock to be released before "
            "exiting. Defaults to sqlalchemy defaults if not set."
        ),
    )

    extra_parameters: dict[str, Any] = Field(
        default={}, description="Additional keyword parameters passed to connect() method verbatim."
    )

    @field_validator("isolation_level")
    @classmethod
    def check_isolation_level(cls, v: str) -> str:
        allowed = {None, "READ_COMMITTED", "READ_UNCOMMITTED", "REPEATABLE_READ", "SERIALIZABLE"}
        if v not in allowed:
            raise ValueError(f"Unexpected value for isolation_level: {v}, allowed values: {allowed}")
        return v


class ApdbSqlPixelizationConfig(BaseModel):
    """Pixelization configuration for SQL-based APDB."""

    htm_level: int = Field(
        default=20,
        description="HTM indexing level",
    )

    htm_max_ranges: int = Field(
        default=64,
        description="Max number of ranges in HTM envelope",
    )

    htm_index_column: str = Field(
        default="pixelId",
        description="Name of a HTM index column for DiaObject and DiaSource tables",
    )


class ApdbSqlConfig(ApdbConfig):
    """APDB configuration class for SQL implementation (ApdbSql)."""

    _implementation_type: ClassVar[str] = "sql"

    db_url: str = Field(description="SQLAlchemy database connection URI.")

    namespace: str | None = Field(
        default=None,
        description=(
            "Namespace or schema name for all tables in APDB database. "
            "Presently only works for PostgreSQL backend. If schema with this name does "
            "not exist it will be created when APDB tables are created."
        ),
    )

    connection_config: ApdbSqlConnectionConfig = Field(
        default_factory=ApdbSqlConnectionConfig,
        description="Database connection configuration",
    )

    pixelization: ApdbSqlPixelizationConfig = Field(
        default_factory=ApdbSqlPixelizationConfig,
        description="Configuration for pixelization.",
    )

    dia_object_index: str = Field(
        default="baseline",
        description=(
            'Indexing mode for DiaObject table. Allowed value is one of "baseline", '
            '"pix_id_iov", or "last_object_table".'
        ),
    )

    ra_dec_columns: list[str] = Field(
        default=["ra", "dec"],
        description="Names of ra/dec columns in DiaObject table.",
    )

    dia_object_columns: list[str] = Field(
        default=[],
        description="List of columns to read from DiaObject, by default read all columns",
    )

    prefix: str = Field(
        default="",
        description="Prefix to add to table names and index names.",
    )

    @field_validator("ra_dec_columns")
    @classmethod
    def check_ra_dec(cls, v: list[str]) -> list[str]:
        if len(v) != 2:
            raise ValueError("ra_dec_columns must have exactly two column names")
        return v

    @field_validator("dia_object_index")
    @classmethod
    def check_dia_object_index(cls, v: str) -> str:
        allowed = {"baseline", "pix_id_iov", "last_object_table"}
        if v not in allowed:
            raise ValueError(f"Unexpected value for dia_object_index: {v}, allowed values: {allowed}")
        return v
