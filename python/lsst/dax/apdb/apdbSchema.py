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

"""Module responsible for APDB schema operations.
"""

from __future__ import annotations

__all__ = ["ColumnDef", "IndexDef", "TableDef",
           "make_minimal_dia_object_schema", "make_minimal_dia_source_schema",
           "ApdbSchema"]

from collections import namedtuple
import logging
import os
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type
import yaml

import sqlalchemy
from sqlalchemy import (Column, Index, MetaData, PrimaryKeyConstraint,
                        UniqueConstraint, Table)
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlalchemy.ext.compiler import compiles
import lsst.afw.table as afwTable


_LOG = logging.getLogger(__name__)

# Classes for representing schema

# Column description:
#    name : column name
#    type : name of cat type (INT, FLOAT, etc.)
#    nullable : True or False
#    default : default value for column, can be None
#    description : documentation, can be None or empty
#    unit : string with unit name, can be None
#    ucd : string with ucd, can be None
ColumnDef = namedtuple('ColumnDef', 'name type nullable default description unit ucd')

# Index description:
#    name : index name, can be None or empty
#    type : one of "PRIMARY", "UNIQUE", "INDEX"
#    columns : list of column names in index
IndexDef = namedtuple('IndexDef', 'name type columns')

# Table description:
#    name : table name
#    description : documentation, can be None or empty
#    columns : list of ColumnDef instances
#    indices : list of IndexDef instances, can be empty or None
TableDef = namedtuple('TableDef', 'name description columns indices')


def make_minimal_dia_object_schema() -> afwTable.SourceTable:
    """Define and create the minimal schema required for a DIAObject.

    Returns
    -------
    schema : `lsst.afw.table.Schema`
        Minimal schema for DIAObjects.
    """
    schema = afwTable.SourceTable.makeMinimalSchema()
    schema.addField("pixelId", type='L',
                    doc='Unique spherical pixelization identifier.')
    schema.addField("nDiaSources", type='L')
    return schema


def make_minimal_dia_source_schema() -> afwTable.SourceTable:
    """ Define and create the minimal schema required for a DIASource.

    Returns
    -------
    schema : `lsst.afw.table.Schema`
        Minimal schema for DIASources.
    """
    schema = afwTable.SourceTable.makeMinimalSchema()
    schema.addField("diaObjectId", type='L',
                    doc='Unique identifier of the DIAObject this source is '
                        'associated to.')
    schema.addField("ccdVisitId", type='L',
                    doc='Id of the exposure and ccd this object was detected '
                        'in.')
    schema.addField("psFlux", type='D',
                    doc='Calibrated PSF flux of this source.')
    schema.addField("psFluxErr", type='D',
                    doc='Calibrated PSF flux err of this source.')
    schema.addField("flags", type='L',
                    doc='Quality flags for this DIASource.')
    schema.addField("pixelId", type='L',
                    doc='Unique spherical pixelization identifier.')
    return schema


@compiles(CreateTable, "oracle")
def _add_suffixes_tbl(element: Any, compiler: Any, **kw: Any) -> str:
    """Add all needed suffixed for Oracle CREATE TABLE statement.

    This is a special compilation method for CreateTable clause which
    registers itself with SQLAlchemy using @compiles decotrator. Exact method
    name does not matter. Client can pass a dict to ``info`` keyword argument
    of Table constructor. If the dict has a key "oracle_tablespace" then its
    value is used as tablespace name. If the dict has a key "oracle_iot" with
    true value then IOT table is created. This method generates additional
    clauses for CREATE TABLE statement which specify tablespace name and
    "ORGANIZATION INDEX" for IOT.

    .. seealso:: https://docs.sqlalchemy.org/en/latest/core/compiler.html
    """
    text = compiler.visit_create_table(element, **kw)
    _LOG.debug("text: %r", text)
    oracle_tablespace = element.element.info.get("oracle_tablespace")
    oracle_iot = element.element.info.get("oracle_iot", False)
    _LOG.debug("oracle_tablespace: %r", oracle_tablespace)
    if oracle_iot:
        text += " ORGANIZATION INDEX"
    if oracle_tablespace:
        text += " TABLESPACE " + oracle_tablespace
    _LOG.debug("text: %r", text)
    return text


@compiles(CreateIndex, "oracle")
def _add_suffixes_idx(element: Any, compiler: Any, **kw: Any) -> str:
    """Add all needed suffixed for Oracle CREATE INDEX statement.

    This is a special compilation method for CreateIndex clause which
    registers itself with SQLAlchemy using @compiles decotrator. Exact method
    name does not matter. Client can pass a dict to ``info`` keyword argument
    of Index constructor. If the dict has a key "oracle_tablespace" then its
    value is used as tablespace name. This method generates additional
    clause for CREATE INDEX statement which specifies tablespace name.

    .. seealso:: https://docs.sqlalchemy.org/en/latest/core/compiler.html
    """
    text = compiler.visit_create_index(element, **kw)
    _LOG.debug("text: %r", text)
    oracle_tablespace = element.element.info.get("oracle_tablespace")
    _LOG.debug("oracle_tablespace: %r", oracle_tablespace)
    if oracle_tablespace:
        text += " TABLESPACE " + oracle_tablespace
    _LOG.debug("text: %r", text)
    return text


class ApdbSchema(object):
    """Class for management of APDB schema.

    Attributes
    ----------
    objects : `sqlalchemy.Table`
        DiaObject table instance
    objects_nightly : `sqlalchemy.Table`
        DiaObjectNightly table instance, may be None
    objects_last : `sqlalchemy.Table`
        DiaObjectLast table instance, may be None
    sources : `sqlalchemy.Table`
        DiaSource table instance
    forcedSources : `sqlalchemy.Table`
        DiaForcedSource table instance

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine`
        SQLAlchemy engine instance
    dia_object_index : `str`
        Indexing mode for DiaObject table, see `ApdbConfig.dia_object_index`
        for details.
    dia_object_nightly : `bool`
        If `True` then create per-night DiaObject table as well.
    schema_file : `str`
        Name of the YAML schema file.
    extra_schema_file : `str`, optional
        Name of the YAML schema file with extra column definitions.
    column_map : `str`, optional
        Name of the YAML file with column mappings.
    afw_schemas : `dict`, optional
        Dictionary with table name for a key and `afw.table.Schema`
        for a value. Columns in schema will be added to standard APDB
        schema (only if standard schema does not have matching column).
    prefix : `str`, optional
        Prefix to add to all scheam elements.
    """

    # map afw type names into cat type names
    _afw_type_map = {"I": "INT",
                     "L": "BIGINT",
                     "F": "FLOAT",
                     "D": "DOUBLE",
                     "Angle": "DOUBLE",
                     "String": "CHAR",
                     "Flag": "BOOL"}
    _afw_type_map_reverse = {"INT": "I",
                             "BIGINT": "L",
                             "FLOAT": "F",
                             "DOUBLE": "D",
                             "DATETIME": "L",
                             "CHAR": "String",
                             "BOOL": "Flag"}

    def __init__(self, engine: sqlalchemy.engine.Engine, dia_object_index: str,
                 dia_object_nightly: bool, schema_file: str,
                 extra_schema_file: Optional[str] = None, column_map: Optional[str] = None,
                 afw_schemas: Optional[Mapping[str, afwTable.Schema]] = None,
                 prefix: str = ""):

        self._engine = engine
        self._dia_object_index = dia_object_index
        self._dia_object_nightly = dia_object_nightly
        self._prefix = prefix

        self._metadata = MetaData(self._engine)

        self.objects = None
        self.objects_nightly = None
        self.objects_last = None
        self.sources = None
        self.forcedSources = None

        if column_map:
            column_map = os.path.expandvars(column_map)
            _LOG.debug("Reading column map file %s", column_map)
            with open(column_map) as yaml_stream:
                # maps cat column name to afw column name
                self._column_map = yaml.load(yaml_stream, Loader=yaml.SafeLoader)
                _LOG.debug("column map: %s", self._column_map)
        else:
            _LOG.debug("No column map file is given, initialize to empty")
            self._column_map = {}
        self._column_map_reverse = {}
        for table, cmap in self._column_map.items():
            # maps afw column name to cat column name
            self._column_map_reverse[table] = {v: k for k, v in cmap.items()}
        _LOG.debug("reverse column map: %s", self._column_map_reverse)

        # build complete table schema
        self._schemas = self._buildSchemas(schema_file, extra_schema_file,
                                           afw_schemas)

        # map cat column types to alchemy
        self._type_map = dict(DOUBLE=self._getDoubleType(),
                              FLOAT=sqlalchemy.types.Float,
                              DATETIME=sqlalchemy.types.TIMESTAMP,
                              BIGINT=sqlalchemy.types.BigInteger,
                              INTEGER=sqlalchemy.types.Integer,
                              INT=sqlalchemy.types.Integer,
                              TINYINT=sqlalchemy.types.Integer,
                              BLOB=sqlalchemy.types.LargeBinary,
                              CHAR=sqlalchemy.types.CHAR,
                              BOOL=sqlalchemy.types.Boolean)

        # generate schema for all tables, must be called last
        self._makeTables()

    def _makeTables(self, mysql_engine: str = 'InnoDB', oracle_tablespace: Optional[str] = None,
                    oracle_iot: bool = False) -> None:
        """Generate schema for all tables.

        Parameters
        ----------
        mysql_engine : `str`, optional
            MySQL engine type to use for new tables.
        oracle_tablespace : `str`, optional
            Name of Oracle tablespace, only useful with oracle
        oracle_iot : `bool`, optional
            Make Index-organized DiaObjectLast table.
        """

        info: Dict[str, Any] = dict(oracle_tablespace=oracle_tablespace)

        if self._dia_object_index == 'pix_id_iov':
            # Special PK with HTM column in first position
            constraints = self._tableIndices('DiaObjectIndexHtmFirst', info)
        else:
            constraints = self._tableIndices('DiaObject', info)
        table = Table(self._prefix+'DiaObject', self._metadata,
                      *(self._tableColumns('DiaObject') + constraints),
                      mysql_engine=mysql_engine,
                      info=info)
        self.objects = table

        if self._dia_object_nightly:
            # Same as DiaObject but no index
            table = Table(self._prefix+'DiaObjectNightly', self._metadata,
                          *self._tableColumns('DiaObject'),
                          mysql_engine=mysql_engine,
                          info=info)
            self.objects_nightly = table

        if self._dia_object_index == 'last_object_table':
            # Same as DiaObject but with special index
            info2 = info.copy()
            info2.update(oracle_iot=oracle_iot)
            table = Table(self._prefix+'DiaObjectLast', self._metadata,
                          *(self._tableColumns('DiaObjectLast')
                            + self._tableIndices('DiaObjectLast', info)),
                          mysql_engine=mysql_engine,
                          info=info2)
            self.objects_last = table

        # for all other tables use index definitions in schema
        for table_name in ('DiaSource', 'SSObject', 'DiaForcedSource', 'DiaObject_To_Object_Match'):
            table = Table(self._prefix+table_name, self._metadata,
                          *(self._tableColumns(table_name)
                            + self._tableIndices(table_name, info)),
                          mysql_engine=mysql_engine,
                          info=info)
            if table_name == 'DiaSource':
                self.sources = table
            elif table_name == 'DiaForcedSource':
                self.forcedSources = table

    def makeSchema(self, drop: bool = False, mysql_engine: str = 'InnoDB',
                   oracle_tablespace: Optional[str] = None, oracle_iot: bool = False) -> None:
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`, optional
            If True then drop tables before creating new ones.
        mysql_engine : `str`, optional
            MySQL engine type to use for new tables.
        oracle_tablespace : `str`, optional
            Name of Oracle tablespace, only useful with oracle
        oracle_iot : `bool`, optional
            Make Index-organized DiaObjectLast table.
        """

        # re-make table schema for all needed tables with possibly different options
        _LOG.debug("clear metadata")
        self._metadata.clear()
        _LOG.debug("re-do schema mysql_engine=%r oracle_tablespace=%r",
                   mysql_engine, oracle_tablespace)
        self._makeTables(mysql_engine=mysql_engine, oracle_tablespace=oracle_tablespace,
                         oracle_iot=oracle_iot)

        # create all tables (optionally drop first)
        if drop:
            _LOG.info('dropping all tables')
            self._metadata.drop_all()
        _LOG.info('creating all tables')
        self._metadata.create_all()

    def getAfwSchema(self, table_name: str, columns: Optional[List[str]] = None
                     ) -> Tuple[afwTable.Schema, Mapping[str, Optional[afwTable.Key]]]:
        """Return afw schema for given table.

        Parameters
        ----------
        table_name : `str`
            One of known APDB table names.
        columns : `list` of `str`, optional
            Include only given table columns in schema, by default all columns
            are included.

        Returns
        -------
        schema : `lsst.afw.table.Schema`
        column_map : `dict`
            Mapping of the table/result column names into schema key.
        """

        table = self._schemas[table_name]
        col_map = self._column_map.get(table_name, {})

        # make a schema
        col2afw: Dict[str, Optional[afwTable.Key]] = {}
        schema = afwTable.SourceTable.makeMinimalSchema()
        for column in table.columns:
            if columns and column.name not in columns:
                continue
            afw_col = col_map.get(column.name, column.name)
            if afw_col in schema.getNames():
                # Continue if the column is already in the minimal schema.
                key = schema.find(afw_col).getKey()
            elif column.type in ("DOUBLE", "FLOAT") and column.unit == "deg":
                #
                # NOTE: degree to radian conversion is not supported (yet)
                #
                # angles in afw are radians and have special "Angle" type
                key = schema.addField(afw_col,
                                      type="Angle",
                                      doc=column.description or "",
                                      units="rad")
            elif column.type == "BLOB":
                # No BLOB support for now
                key = None
            else:
                units = column.unit or ""
                # some units in schema are not recognized by afw but we do not care
                if self._afw_type_map_reverse[column.type] == 'String':
                    key = schema.addField(afw_col,
                                          type=self._afw_type_map_reverse[column.type],
                                          doc=column.description or "",
                                          units=units,
                                          parse_strict="silent",
                                          size=10)
                elif units == "deg":
                    key = schema.addField(afw_col,
                                          type='Angle',
                                          doc=column.description or "",
                                          parse_strict="silent")
                else:
                    key = schema.addField(afw_col,
                                          type=self._afw_type_map_reverse[column.type],
                                          doc=column.description or "",
                                          units=units,
                                          parse_strict="silent")
            col2afw[column.name] = key

        return schema, col2afw

    def getAfwColumns(self, table_name: str) -> Mapping[str, ColumnDef]:
        """Returns mapping of afw column names to Column definitions.

        Parameters
        ----------
        table_name : `str`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of afw column names to `ColumnDef` instances.
        """
        table = self._schemas[table_name]
        col_map = self._column_map.get(table_name, {})

        cmap = {}
        for column in table.columns:
            afw_name = col_map.get(column.name, column.name)
            cmap[afw_name] = column
        return cmap

    def getColumnMap(self, table_name: str) -> Mapping[str, ColumnDef]:
        """Returns mapping of column names to Column definitions.

        Parameters
        ----------
        table_name : `str`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of column names to `ColumnDef` instances.
        """
        table = self._schemas[table_name]
        cmap = {column.name: column for column in table.columns}
        return cmap

    def _buildSchemas(self, schema_file: str, extra_schema_file: Optional[str] = None,
                      afw_schemas: Optional[Mapping[str, afwTable.Schema]] = None) -> Mapping[str, TableDef]:
        """Create schema definitions for all tables.

        Reads YAML schemas and builds dictionary containing `TableDef`
        instances for each table.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with standard cat schema.
        extra_schema_file : `str`, optional
            Name of YAML file with extra table information or `None`.
        afw_schemas : `dict`, optional
            Dictionary with table name for a key and `afw.table.Schema`
            for a value. Columns in schema will be added to standard APDB
            schema (only if standard schema does not have matching column).

        Returns
        -------
        schemas : `dict`
            Mapping of table names to `TableDef` instances.
        """

        schema_file = os.path.expandvars(schema_file)
        _LOG.debug("Reading schema file %s", schema_file)
        with open(schema_file) as yaml_stream:
            tables = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
            # index it by table name
        _LOG.debug("Read %d tables from schema", len(tables))

        if extra_schema_file:
            extra_schema_file = os.path.expandvars(extra_schema_file)
            _LOG.debug("Reading extra schema file %s", extra_schema_file)
            with open(extra_schema_file) as yaml_stream:
                extras = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
                # index it by table name
                schemas_extra = {table['table']: table for table in extras}
        else:
            schemas_extra = {}

        # merge extra schema into a regular schema, for now only columns are merged
        for table in tables:
            table_name = table['table']
            if table_name in schemas_extra:
                columns = table['columns']
                extra_columns = schemas_extra[table_name].get('columns', [])
                extra_columns = {col['name']: col for col in extra_columns}
                _LOG.debug("Extra columns for table %s: %s", table_name, extra_columns.keys())
                columns = []
                for col in table['columns']:
                    if col['name'] in extra_columns:
                        columns.append(extra_columns.pop(col['name']))
                    else:
                        columns.append(col)
                # add all remaining extra columns
                table['columns'] = columns + list(extra_columns.values())

                if 'indices' in schemas_extra[table_name]:
                    raise RuntimeError("Extra table definition contains indices, "
                                       "merging is not implemented")

                del schemas_extra[table_name]

        # Pure "extra" table definitions may contain indices
        tables += schemas_extra.values()

        # convert all dicts into named tuples
        schemas = {}
        for table in tables:

            columns = table.get('columns', [])

            table_name = table['table']
            afw_schema = afw_schemas and afw_schemas.get(table_name)
            if afw_schema:
                # use afw schema to create extra columns
                column_names = {col['name'] for col in columns}
                column_names_lower = {col.lower() for col in column_names}
                for _, field in afw_schema:
                    fcolumn = self._field2dict(field, table_name)
                    if fcolumn['name'] not in column_names:
                        # check that there is no column name that only differs in case
                        if fcolumn['name'].lower() in column_names_lower:
                            raise ValueError("afw.table column name case does not match schema column name")
                        columns.append(fcolumn)

            table_columns = []
            for col in columns:
                # For prototype set default to 0 even if columns don't specify it
                if "default" not in col:
                    default = None
                    if col['type'] not in ("BLOB", "DATETIME"):
                        default = 0
                else:
                    default = col["default"]

                column = ColumnDef(name=col['name'],
                                   type=col['type'],
                                   nullable=col.get("nullable"),
                                   default=default,
                                   description=col.get("description"),
                                   unit=col.get("unit"),
                                   ucd=col.get("ucd"))
                table_columns.append(column)

            table_indices = []
            for idx in table.get('indices', []):
                index = IndexDef(name=idx.get('name'),
                                 type=idx.get('type'),
                                 columns=idx.get('columns'))
                table_indices.append(index)

            schemas[table_name] = TableDef(name=table_name,
                                           description=table.get('description'),
                                           columns=table_columns,
                                           indices=table_indices)

        return schemas

    def _tableColumns(self, table_name: str) -> List[Column]:
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `str`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of `Column` objects.
        """

        # get the list of columns in primary key, they are treated somewhat
        # specially below
        table_schema = self._schemas[table_name]
        pkey_columns = set()
        for index in table_schema.indices:
            if index.type == 'PRIMARY':
                pkey_columns = set(index.columns)
                break

        # convert all column dicts into alchemy Columns
        column_defs = []
        for column in table_schema.columns:
            kwargs = dict(nullable=column.nullable)
            if column.default is not None:
                kwargs.update(server_default=str(column.default))
            if column.name in pkey_columns:
                kwargs.update(autoincrement=False)
            ctype = self._type_map[column.type]
            column_defs.append(Column(column.name, ctype, **kwargs))

        return column_defs

    def _field2dict(self, field: afwTable.Field, table_name: str) -> Mapping[str, Any]:
        """Convert afw schema field definition into a dict format.

        Parameters
        ----------
        field : `lsst.afw.table.Field`
            Field in afw table schema.
        table_name : `str`
            Name of the table.

        Returns
        -------
        field_dict : `dict`
            Field attributes for SQL schema:

            - ``name`` : field name (`str`)
            - ``type`` : type name in SQL, e.g. "INT", "FLOAT" (`str`)
            - ``nullable`` : `True` if column can be ``NULL`` (`bool`)
        """
        column = field.getName()
        column = self._column_map_reverse[table_name].get(column, column)
        ctype = self._afw_type_map[field.getTypeString()]
        return dict(name=column, type=ctype, nullable=True)

    def _tableIndices(self, table_name: str, info: Dict) -> List[sqlalchemy.schema.Constraint]:
        """Return set of constraints/indices in a table

        Parameters
        ----------
        table_name : `str`
            Name of the table.
        info : `dict`
            Additional options passed to SQLAlchemy index constructor.

        Returns
        -------
        index_defs : `list`
            List of SQLAlchemy index/constraint objects.
        """

        table_schema = self._schemas[table_name]

        # convert all index dicts into alchemy Columns
        index_defs: List[sqlalchemy.schema.Constraint] = []
        for index in table_schema.indices:
            if index.type == "INDEX":
                index_defs.append(Index(self._prefix+index.name, *index.columns, info=info))
            else:
                kwargs = {}
                if index.name:
                    kwargs['name'] = self._prefix+index.name
                if index.type == "PRIMARY":
                    index_defs.append(PrimaryKeyConstraint(*index.columns, **kwargs))
                elif index.type == "UNIQUE":
                    index_defs.append(UniqueConstraint(*index.columns, **kwargs))

        return index_defs

    def _getDoubleType(self) -> Type:
        """DOUBLE type is database-specific, select one based on dialect.

        Returns
        -------
        type_object : `object`
            Database-specific type definition.
        """
        if self._engine.name == 'mysql':
            from sqlalchemy.dialects.mysql import DOUBLE
            return DOUBLE(asdecimal=False)
        elif self._engine.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
            return DOUBLE_PRECISION
        elif self._engine.name == 'oracle':
            from sqlalchemy.dialects.oracle import DOUBLE_PRECISION
            return DOUBLE_PRECISION
        elif self._engine.name == 'sqlite':
            # all floats in sqlite are 8-byte
            from sqlalchemy.dialects.sqlite import REAL
            return REAL
        else:
            raise TypeError('cannot determine DOUBLE type, unexpected dialect: ' + self._engine.name)
