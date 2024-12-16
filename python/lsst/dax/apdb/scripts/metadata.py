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

__all__ = ["metadata_delete", "metadata_get", "metadata_set", "metadata_show"]

import json
import sys

from ..apdb import Apdb


def metadata_delete(config: str, key: str) -> None:
    """Delete metadata key.

    Parameters
    ----------
    config : `str`
        Path or URI of APDB configuration file.
    key : `str`
        Metadata key.
    """
    apdb = Apdb.from_uri(config)
    apdb.metadata.delete(key)


def metadata_get(config: str, key: str) -> None:
    """Print value of the metadata item.

    Parameters
    ----------
    config : `str`
        Path or URI of APDB configuration file.
    key : `str`
        Metadata key.
    """
    apdb = Apdb.from_uri(config)
    value = apdb.metadata.get(key)
    if value is None:
        raise KeyError(f"Metadata key {key!r} does not exist.")
    else:
        print(value)


def metadata_set(config: str, key: str, value: str, force: bool) -> None:
    """Add or update metadata item.

    Parameters
    ----------
    config : `str`
        Path or URI of APDB configuration file.
    key : `str`
        Metadata key.
    value : `str`
        Metadata value.
    force : `bool`
        Set to True to allow updates for existing keys.
    """
    apdb = Apdb.from_uri(config)
    apdb.metadata.set(key, value, force=force)


def metadata_show(config: str, use_json: bool) -> None:
    """Show contents of APDB metadata table.

    Parameters
    ----------
    config : `str`
        Path or URI of APDB configuration file.
    use_json : `bool`
        If True dump in JSON format.
    """
    apdb = Apdb.from_uri(config)
    if use_json:
        data = {key: value for key, value in apdb.metadata.items()}
        json.dump(data, sys.stdout, indent=2)
        print()
    else:
        for key, value in apdb.metadata.items():
            print(f"{key}: {value}")


def check_instrument(instrument: str) -> None:
    """Check that a fully-qualified instrument name is valid, if pipe_base is
    available.

    Parameters
    ----------
    instrument : str
        Name of instrument to check for validity.

    Raises
    ------
    RuntimeError
        Raised if the instrument is not known to `~lsst.pipe.base.Instrument`.
    """
    try:
        import lsst.pipe.base

        lsst.pipe.base.Instrument.from_string(instrument)
    except ModuleNotFoundError as e:
        print(f"WARNING: Cannot check instrument string `{instrument}` against the canonical list: {e}")
    except RuntimeError as e:
        raise RuntimeError(f"Not creating APDB: invalid or unknown instrument name `{instrument}`") from e
