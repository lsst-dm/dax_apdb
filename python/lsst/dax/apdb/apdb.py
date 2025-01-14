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

__all__ = ["ApdbConfig", "Apdb"]

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

import astropy.time
import pandas
from lsst.resources import ResourcePathExpression
from lsst.sphgeom import Region

from .apdbIndex import ApdbIndex
from .apdbSchema import ApdbTables
from .config import ApdbConfig
from .factory import make_apdb
from .schema_model import Table

if TYPE_CHECKING:
    from .apdbMetadata import ApdbMetadata


class Apdb(ABC):
    """Abstract interface for APDB."""

    @classmethod
    def from_config(cls, config: ApdbConfig) -> Apdb:
        """Create Ppdb instance from configuration object.

        Parameters
        ----------
        config : `ApdbConfig`
            Configuration object, type of this object determines type of the
            Apdb implementation.

        Returns
        -------
        apdb : `apdb`
            Instance of `Apdb` class.
        """
        return make_apdb(config)

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> Apdb:
        """Make Apdb instance from a serialized configuration.

        Parameters
        ----------
        uri : `~lsst.resources.ResourcePathExpression`
            URI or local file path pointing to a file with serialized
            configuration, or a string with a "label:" prefix. In the latter
            case, the configuration will be looked up from an APDB index file
            using the label name that follows the prefix. The APDB index file's
            location is determined by the ``DAX_APDB_INDEX_URI`` environment
            variable.

        Returns
        -------
        apdb : `apdb`
            Instance of `Apdb` class, the type of the returned instance is
            determined by configuration.
        """
        if isinstance(uri, str) and uri.startswith("label:"):
            tag, _, label = uri.partition(":")
            index = ApdbIndex()
            # Try to find YAML format first, and pex_config if YAML is not
            # found. During transitional period we support conversion of
            # pex_config format to a new pydantic format.
            try:
                uri = index.get_apdb_uri(label, "yaml")
            except ValueError as exc:
                try:
                    uri = index.get_apdb_uri(label, "pex_config")
                except ValueError:
                    raise exc from None
        config = ApdbConfig.from_uri(uri)
        return make_apdb(config)

    @abstractmethod
    def tableDef(self, table: ApdbTables) -> Table | None:
        """Return table schema definition for a given table.

        Parameters
        ----------
        table : `ApdbTables`
            One of the known APDB tables.

        Returns
        -------
        tableSchema : `.schema_model.Table` or `None`
            Table schema description, `None` is returned if table is not
            defined by this implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaObjects(self, region: Region) -> pandas.DataFrame:
        """Return catalog of DiaObject instances from a given region.

        This method returns only the last version of each DiaObject,
        and may return only the subset of the DiaObject columns needed
        for AP association. Some
        records in a returned catalog may be outside the specified region, it
        is up to a client to ignore those records or cleanup the catalog before
        futher use.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIAObjects.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing DiaObject records for a region that may be a
            superset of the specified region.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaSources(
        self, region: Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        """Return catalog of DiaSource instances from a given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        object_ids : iterable [ `int` ], optional
            List of DiaObject IDs to further constrain the set of returned
            sources. If `None` then returned sources are not constrained. If
            list is empty then empty catalog is returned with a correct
            schema.
        visit_time : `astropy.time.Time`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0.

        Notes
        -----
        This method returns DiaSource catalog for a region with additional
        filtering based on DiaObject IDs. Only a subset of DiaSource history
        is returned limited by ``read_sources_months`` config parameter, w.r.t.
        ``visit_time``. If ``object_ids`` is empty then an empty catalog is
        always returned with the correct schema (columns/types). If
        ``object_ids`` is `None` then no filtering is performed and some of the
        returned records may be outside the specified region.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaForcedSources(
        self, region: Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        """Return catalog of DiaForcedSource instances from a given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        object_ids : iterable [ `int` ], optional
            List of DiaObject IDs to further constrain the set of returned
            sources. If list is empty then empty catalog is returned with a
            correct schema. If `None` then returned sources are not
            constrained. Some implementations may not support latter case.
        visit_time : `astropy.time.Time`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_forced_sources_months`` configuration parameter is set to 0.

        Raises
        ------
        NotImplementedError
            May be raised by some implementations if ``object_ids`` is `None`.

        Notes
        -----
        This method returns DiaForcedSource catalog for a region with
        additional filtering based on DiaObject IDs. Only a subset of DiaSource
        history is returned limited by ``read_forced_sources_months`` config
        parameter, w.r.t. ``visit_time``. If ``object_ids`` is empty then an
        empty catalog is always returned with the correct schema
        (columns/types). If ``object_ids`` is `None` then no filtering is
        performed and some of the returned records may be outside the specified
        region.
        """
        raise NotImplementedError()

    @abstractmethod
    def containsVisitDetector(self, visit: int, detector: int) -> bool:
        """Test whether any sources for a given visit-detector are present in
        the APDB.

        Parameters
        ----------
        visit, detector : `int`
            The ID of the visit-detector to search for.

        Returns
        -------
        present : `bool`
            `True` if at least one DiaSource or DiaForcedSource record
            may exist for the specified observation, `False` otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def getSSObjects(self) -> pandas.DataFrame:
        """Return catalog of SSObject instances.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing SSObject records, all existing records are
            returned.
        """
        raise NotImplementedError()

    @abstractmethod
    def store(
        self,
        visit_time: astropy.time.Time,
        objects: pandas.DataFrame,
        sources: pandas.DataFrame | None = None,
        forced_sources: pandas.DataFrame | None = None,
    ) -> None:
        """Store all three types of catalogs in the database.

        Parameters
        ----------
        visit_time : `astropy.time.Time`
            Time of the visit.
        objects : `pandas.DataFrame`
            Catalog with DiaObject records.
        sources : `pandas.DataFrame`, optional
            Catalog with DiaSource records.
        forced_sources : `pandas.DataFrame`, optional
            Catalog with DiaForcedSource records.

        Notes
        -----
        This methods takes DataFrame catalogs, their schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from catalog
          - this method knows how to fill interval-related columns of DiaObject
            (validityStart, validityEnd) they do not need to appear in a
            catalog
          - source catalogs have ``diaObjectId`` column associating sources
            with objects

        This operation need not be atomic, but DiaSources and DiaForcedSources
        will not be stored until all DiaObjects are stored.
        """
        raise NotImplementedError()

    @abstractmethod
    def storeSSObjects(self, objects: pandas.DataFrame) -> None:
        """Store or update SSObject catalog.

        Parameters
        ----------
        objects : `pandas.DataFrame`
            Catalog with SSObject records.

        Notes
        -----
        If SSObjects with matching IDs already exist in the database, their
        records will be updated with the information from provided records.
        """
        raise NotImplementedError()

    @abstractmethod
    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        """Associate DiaSources with SSObjects, dis-associating them
        from DiaObjects.

        Parameters
        ----------
        idMap : `Mapping`
            Maps DiaSource IDs to their new SSObject IDs.

        Raises
        ------
        ValueError
            Raised if DiaSource ID does not exist in the database.
        """
        raise NotImplementedError()

    @abstractmethod
    def dailyJob(self) -> None:
        """Implement daily activities like cleanup/vacuum.

        What should be done during daily activities is determined by
        specific implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def countUnassociatedObjects(self) -> int:
        """Return the number of DiaObjects that have only one DiaSource
        associated with them.

        Used as part of ap_verify metrics.

        Returns
        -------
        count : `int`
            Number of DiaObjects with exactly one associated DiaSource.

        Notes
        -----
        This method can be very inefficient or slow in some implementations.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def metadata(self) -> ApdbMetadata:
        """Object controlling access to APDB metadata (`ApdbMetadata`)."""
        raise NotImplementedError()
