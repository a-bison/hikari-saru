"""
Configuration classes.

Please note that these are configuration classes, not data stores.
They're meant to store configuration options that change infrequently,
with a low chance of failure.
"""
import abc
import collections
import collections.abc
import copy
import json
import logging
import pathlib
import re
import shutil
import typing as t
from datetime import datetime

__all__ = (
    "ConfigValueT",
    "ConfigTV",
    "Config",
    "ConfigBackendProtocol",
    "BaseConfig",
    "BaseSubConfig",
    "JsonConfigBackend",
    "NullConfigBackend",
    "InMemoryConfigBackend",
    "JsonConfigDirectory",
    "ConfigException",
    "ConfigPathException",
    "ConfigTemplate",
    "cfg_path_parse",
    "cfg_path_build"
)


logger = logging.getLogger(__name__)

CONFIG_PATH_CHAR = "/"
CONFIG_PATH_SPLIT = re.compile(r"/+")

ConfigValueT = t.Union[
    str,
    int,
    bool,
    float,
    t.MutableSequence['ConfigValueT'],
    t.MutableMapping[str, 'ConfigValueT']
]
"""
An arbitrary configuration value. Encompasses all
possible value types stored in a `Config` implementation.
"""

ConfigTV = t.TypeVar("ConfigTV", bound='Config')
"""
A type variable representing any type of `Config`.
"""


class Config(t.MutableMapping[str, ConfigValueT], abc.ABC):
    """
    The base class for configuration objects. All changes to this object
    are kept in memory until a call to `Config.write` (save
    changes to persistent storage), or `Config.read` (discard
    current changes and re-read from persistent storage).

    May be treated mostly as a normal `typing.MutableMapping` with
    some special considerations.
    """

    @abc.abstractmethod
    def write(self) -> None:
        """
        Write the contents of this configuration to persistent storage.
        """
        ...

    @abc.abstractmethod
    def load(self) -> None:
        """
        Discard in-memory changes and read values from persistent storage.
        """
        ...

    @abc.abstractmethod
    def sub(self, key: str, ensure_exists: bool = True) -> 'Config':
        """
        Return a subconfig view of the passed path.
        """
        ...

    @property
    def root(self) -> t.MutableMapping[str, ConfigValueT]:
        """
        A mutable mapping allowing direct access to this config.
        This returns `self` by default, but implementations may override this.

        In any case, it should not be assumed that the object returned
        by this property supports config behavior. It should be treated
        like a dict.
        """
        return self


class ConfigBackendProtocol(t.Protocol):
    """
    The protocol for configuration backends. Implements the persistence part of
    `BaseConfig` objects.
    """

    def write(self, data: t.Mapping[str, ConfigValueT]) -> None:
        """
        Write data to persistent storage.
        """
        ...

    def read(self) -> t.MutableMapping[str, ConfigValueT]:
        """
        Read data from persistent storage.
        """
        ...


class ConfigException(Exception):
    """
    Raised for generic failures in the configuration system.
    """
    pass


class ConfigPathException(ConfigException):
    """
    Raised on errors following configuration paths.
    """
    pass


class NullConfigBackend(ConfigBackendProtocol):
    """
    A config backend that does nothing. Reading from
    this backend produces an empty dictionary. Mainly
    for testing purposes.
    """

    def write(self, data: t.Mapping[str, ConfigValueT]) -> None: ...

    def read(self) -> t.MutableMapping[str, ConfigValueT]:
        return {}


def cfg_path_parse(path: str) -> t.Sequence[str]:
    """
    Parse a path string into a sequence of config keys.
    Empty strings are ignored, so for example `/foo/bar/` and `foo/bar` are
    the same thing.
    """
    return [item for item in CONFIG_PATH_SPLIT.split(path) if item]


def cfg_path_build(path: t.Sequence[str]) -> str:
    """
    Build a path string out of a sequence of config keys.
    """
    return CONFIG_PATH_CHAR.join(path)


class BaseConfig(Config):
    """
    The standard configuration implementation. Defers the persistence
    part to a passed `ConfigBackendProtocol`, but implements everything
    else.
    """
    def __init__(self, backend: ConfigBackendProtocol):
        self.backend = backend
        self.__data: t.MutableMapping[str, ConfigValueT] = {}

    def write(self) -> None:
        self.backend.write(self.__data)

    def load(self) -> None:
        self.__data = self.backend.read()

    @staticmethod
    def __subdata_path_error(full_path: t.Sequence[str], error_path: t.Sequence[str], msg: str) -> t.NoReturn:
        raise ConfigPathException(
            f"Could not get subconfig at \"{cfg_path_build(full_path)}\": " +
            f"Item at \"{cfg_path_build(error_path)}\" {msg.lower()}"
        )

    def __get_subdata(
        self,
        path: t.Sequence[str],
        create_subdata: bool = False
    ) -> t.MutableMapping[str, ConfigValueT]:
        """
        Traverse the config tree based on `path` and return the corresponding
        internal subconfig, if it exists. If `create_subdata` is true,
        missing tree nodes will be created during the traversal, guaranteeing
        that a subconfig will be returned.

        Raises:
            ConfigPathError: In cases where an item in the path cannot be located,
                or is of invalid type.
        """
        subdata: t.MutableMapping[str, ConfigValueT] = self.__data
        traversed: t.MutableSequence[str] = []

        # traverse path and find the pointed subdata
        for path_item in path:
            if path_item not in subdata:
                if create_subdata:
                    subdata[path_item] = {}
                else:
                    self.__subdata_path_error(path, [*traversed, path_item], "does not exist.")

            # Item exists. Check to see if it's a mapping, so we can continue.
            potential_subdata = subdata[path_item]
            if not isinstance(potential_subdata, collections.abc.MutableMapping):
                self.__subdata_path_error(path, [*traversed, path_item], "is not a mapping.")
            else:
                # Success, so update subdata and traversal tracking
                subdata = potential_subdata
                traversed.append(path_item)

        return subdata

    def __get_subdata_and_key(
        self,
        path: str,
        create_subdata: bool = False
    ) -> t.Tuple[t.MutableMapping[str, ConfigValueT], str]:
        """
        Traverse the config tree based on `path`, and return the corresponding
        subconfig and key. For example, `__get_subdata_and_key("foo/bar/baz")`
        would return the configuration pointed to by `foo/bar`, and the string
        `"baz"`. These may then be further used for operations on the subconfig.
        """
        parsed_path = cfg_path_parse(path)

        if not parsed_path:
            raise ConfigPathException(f"Path \"{path}\" does not reference anything. Empty config names are not allowed.")

        if len(parsed_path) == 1:
            # In the case of one path element, skip the traversal step.
            return self.__data, parsed_path[0]

        *subconfig_path, key = parsed_path
        return self.__get_subdata(subconfig_path, create_subdata), key

    def sub(self, key: str, ensure_exists: bool = True) -> Config:
        # Attempt to get subdata. This will raise any appropriate exceptions
        # should there be a problem with the path.
        self.__get_subdata(cfg_path_parse(key), create_subdata=ensure_exists)
        return BaseSubConfig(self, path=key)

    @property
    def root(self) -> t.MutableMapping[str, ConfigValueT]:
        """
        A mutable mapping allowing direct access to config data.
        """
        return self.__data

    def __setitem__(self, key: str, value: ConfigValueT) -> None:
        subconfig, key = self.__get_subdata_and_key(key, create_subdata=True)
        subconfig[key] = value

    def __delitem__(self, key: str) -> None:
        subconfig, key = self.__get_subdata_and_key(key, create_subdata=False)
        del subconfig[key]

    def __getitem__(self, key: str) -> ConfigValueT:
        subconfig, key = self.__get_subdata_and_key(key, create_subdata=False)
        return subconfig[key]

    def __len__(self) -> int:
        return len(self.__data)

    def __iter__(self) -> t.Iterator[str]:
        yield from self.__data

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str):
            try:
                _ = self[key]
            except (KeyError, ConfigPathException):
                return False

            return True
        else:
            logger.warning("BaseConfig: __contains__ attempt with non-str key")
            return False


class BaseSubConfig(Config):
    """
    A basic subconfig implementation. Note that this is intended to
    be constructed by `BaseConfig`, which does some work to ensure the
    methods defined here are valid.
    """
    def __init__(self, parent: Config, path: str):
        self.parent = parent
        self.path = path

    def write(self) -> None:
        self.parent.write()

    def load(self) -> None:
        self.parent.load()

    def __get_true_path(self, path: str) -> str:
        # Parse path to normalize it. If we get nothing, then a blank
        # string or equivalent was passed in, so error.
        parsed = cfg_path_parse(path)

        if not parsed:
            raise ConfigPathException(f"Path \"{path}\" does not reference anything. Empty config names are not allowed.")

        return cfg_path_build([self.path, *parsed])

    def sub(self, key: str, ensure_exists: bool = True) -> Config:
        return self.parent.sub(self.__get_true_path(key), ensure_exists=ensure_exists)

    @property
    def __safe_dict(self) -> t.MutableMapping[str, ConfigValueT]:
        # Assumption: The path pointed to in our parent is a valid mapping.
        return t.cast(t.MutableMapping[str, ConfigValueT], self.parent[self.path])

    @property
    def root(self) -> t.MutableMapping[str, ConfigValueT]:
        return self.__safe_dict

    def __setitem__(self, key: str, value: ConfigValueT) -> None:
        self.parent[self.__get_true_path(key)] = value

    def __delitem__(self, key: str) -> None:
        del self.parent[self.__get_true_path(key)]

    def __getitem__(self, key: str) -> ConfigValueT:
        return self.parent[self.__get_true_path(key)]

    def __len__(self) -> int:
        return len(self.__safe_dict)

    def __iter__(self) -> t.Iterator[str]:
        yield from self.__safe_dict

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str):
            return self.__get_true_path(key) in self.parent
        else:
            logger.warning("BaseSubConfig: __contains__ attempt with non-str key")
            return False


class InMemoryConfigBackend(ConfigBackendProtocol):
    """
    A configuration backend that stores information in a dictionary.
    All information will be lost on application close. Mainly used
    for testing read/write.
    """
    def __init__(self) -> None:
        self.__data: t.MutableMapping[str, ConfigValueT] = {}

    def write(self, data: t.Mapping[str, ConfigValueT]) -> None:
        self.__data = {}
        self.__data.update(copy.deepcopy(data))

    def read(self) -> t.MutableMapping[str, ConfigValueT]:
        return copy.deepcopy(self.__data)

    @property
    def data(self) -> t.MutableMapping[str, ConfigValueT]:
        return self.__data


class ConfigTemplate:
    """
    A configuration template. Describes all the configuration
    paths that must exist in a `Config` object and what their initial
    values should be.
    """
    def __init__(
        self,
        paths: t.Mapping[str, ConfigValueT],
        rollback_on_failure: bool = False
    ):
        self.template = copy.deepcopy(paths)
        self.rollback_on_failure = rollback_on_failure

    def __rollback(self, config: Config) -> None:
        """
        Roll back a config object. Only applies if self.rollback_on_failure
        is True.
        """
        if self.rollback_on_failure:
            # Load last written data on failure.
            logger.warning("ConfigTemplate: rolling back...")
            config.load()
            logger.warning("ConfigTemplate: rollback success.")

    def apply(self, config: Config) -> None:
        """
        Apply this template to the given config. Does not write anything,
        so on success should be followed up with a call to `Config.write`.
        """
        for path, value in self.template.items():
            if path not in config:
                try:
                    config[path] = value
                except (ConfigPathException, KeyError):
                    logger.error(f"ConfigTemplate: Could not apply value {value} to \"{path}\"")
                    self.__rollback(config)
                    raise


class JsonConfigBackend(ConfigBackendProtocol):
    """
    A configuration backend that stores information in a human-readable
    JSON file.
    """
    def __init__(
        self,
        path: t.Union[str, pathlib.Path],
        check_date: bool = False
    ):
        # We accept str or pathlib.Path, but internally it's always
        # a Path instance.
        if isinstance(path, str):
            self.path = pathlib.Path(path)
        else:
            self.path = path

        self.check_date = check_date
        self.__last_readwrite_date = 0.0

    def __update_last_date(self) -> None:
        self.__last_readwrite_date = round(datetime.now().timestamp(), 4)

    def write(self, data: t.Mapping[str, ConfigValueT]) -> None:
        if self.path.exists() and self.check_date:
            file_timestamp = round(self.path.stat().st_mtime, 4)

            # If file was modified after last load/write,
            # refuse to write.
            if file_timestamp > self.__last_readwrite_date:
                msg = "{} has been modified, config must be reloaded"
                logger.error(
                    f"check_date conflict: {self.path}: "
                    f"{file_timestamp} > {self.__last_readwrite_date} "
                    "(file_timestamp > self.__last_readwrite_date)"
                )
                raise ConfigException(msg.format(self.path))

        with open(self.path, 'w') as f:
            json.dump(data, f, indent=4)

        self.__update_last_date()

    def read(self) -> t.MutableMapping[str, ConfigValueT]:
        if not self.path.exists():
            logger.warning(f"JSON config store at {self.path} does not exist, creating.")
            self.write({})
            return {}

        with open(self.path, 'r') as f:
            data = dict(json.load(f))

        self.__update_last_date()

        return data


class JsonConfigDirectory(t.Mapping[t.Union[int, str], Config]):
    def __init__(
        self,
        path: t.Union[str, pathlib.Path],
        template: t.Union[ConfigTemplate, t.Mapping[str, ConfigTemplate], None] = None
    ):
        self.data: t.MutableMapping[str, Config] = {}
        self.template = template

        # We accept str or pathlib.Path, but internally it's always
        # a Path instance.
        if isinstance(path, str):
            self.path = pathlib.Path(path)
        else:
            self.path = path

    def __apply_template(self, s_cid: str) -> None:
        """
        Attempt to apply the configured template for the named configuration.
        """
        if self.template is None:
            return

        conf = self.data[s_cid]

        if isinstance(self.template, ConfigTemplate):
            # Single mode. Use the same template for everything.
            self.template.apply(conf)
        elif isinstance(self.template, collections.abc.Mapping):
            # Unique mode. Use a different template for each config.
            if s_cid not in self.template:
                # If not present, don't fail, but print warning.
                logger.warning(f"JsonConfigDictionary: unique template: none for {s_cid}")
                return

            template = self.template[s_cid]
            template.apply(conf)
        else:
            raise TypeError(f"Invalid template type {str(type(self.template))}")

    def backup(self, parent_path: pathlib.Path) -> None:
        """
        Back up the entire configuration to a given directory.
        For example, if `parent_path` is "example/config", the resulting
        backup will be created at "example/config/BACKUP_NAME".
        """
        path = self.backup_location(parent_path)

        if path.exists():
            shutil.rmtree(path)

        logger.info(f"JsonConfigDirectory: backup \"{self.path}\" to \"{path}\"")
        shutil.copytree(self.path, path)

    def cfg_location(self, cid: t.Union[int, str]) -> pathlib.Path:
        return self.path / (str(cid) + ".json")

    def backup_location(self, parent_path: pathlib.Path) -> pathlib.Path:
        return parent_path / (self.path.name + ".backup")

    def new_config(self, cid: t.Union[int, str]) -> Config:
        """
        Create a new configuration in this directory and return it.
        Performs no assignment or write operations.
        """
        return BaseConfig(JsonConfigBackend(self.cfg_location(cid)))

    def __read_all(self) -> None:
        """
        Read all configuration files within the directory.
        """
        self.data = {}

        for child in self.path.iterdir():
            try:
                cid = child.stem
            except ValueError:
                continue

            # Create a config object and load it.
            self.data[cid] = self.new_config(cid)
            self.data[cid].load()

            # Apply template on read.
            self.__apply_template(cid)

    def __create_dir(self) -> None:
        """
        Create a new directory. The parent of the set path `self.path` must
        exist.
        """
        try:
            logger.info(f"JsonConfigDirectory: \"{self.path}\": try create")
            self.path.mkdir()
            logger.info(f"JsonConfigDirectory: \"{self.path}\" created")
        except FileNotFoundError:
            logger.error(f"JsonConfigDirectory: \"{self.path}\": one or more parents do not exist")
            raise

    def load(self) -> None:
        """
        Load the config directory, creating a new one if it doesn't exist.
        """
        if self.path.is_dir():
            self.__read_all()
        elif self.path.exists():
            raise FileExistsError(f"JsonConfigDirectory: \"{self.path}\": must be a directory.")
        else:
            # Does not exist, so create new dir
            self.__create_dir()

    def write(self) -> None:
        """
        Write all config files.
        """
        for cfg in self.data.values():
            cfg.write()

    # Get a configuration object.
    def __getitem__(self, cid: t.Union[int, str]) -> Config:
        try:
            return self.data[str(cid)]
        except KeyError:
            logger.error("JsonConfigDirectory: __getitem__(\"{cid}\") miss. Try \"create_config\" first.")
            raise

    # Get the number of configuration objects currently loaded.
    def __len__(self) -> int:
        return len(self.data)

    # Iterate over stored IDs.
    def __iter__(self) -> t.Iterator[str]:
        yield from self.data

    # Test if this config directory contains the given ID
    def __contains__(self, cid: object) -> bool:
        if isinstance(cid, (int, str)):
            return str(cid) in self.data
        else:
            logger.warning("JsonConfigDirectory: __contains__ attempt with non-str/int key")
            return False

    def create_config(self, cid: t.Union[int, str]) -> Config:
        """
        Create a new config, overwriting anything that was there previously.
        Returns the newly created config object.
        """
        s_cid = str(cid)

        self.data[s_cid] = self.new_config(cid)
        self.__apply_template(s_cid)
        self.data[s_cid].write()

        return self.data[s_cid]

    def ensure_exists(self, cid: t.Union[int, str]) -> None:
        """
        Make sure a config exists.
        """
        if cid not in self:
            self.create_config(cid)

