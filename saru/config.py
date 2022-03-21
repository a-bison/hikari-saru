#
# Config classes for dynamic, persistent configuration.
#
import functools
import logging
import json
import pathlib
import re
import shutil
from datetime import datetime

from collections.abc import Mapping, MutableMapping, MutableSequence
from typing import Optional, Union, Protocol, Callable, Any, TypeVar, cast

from saru import util

CONFIG_PATH_SPLIT = re.compile(r"/+")

logger = logging.getLogger(__name__)


ConfigValue = Union[
    str,
    int,
    bool,
    float,
    MutableSequence[Any],
    MutableMapping[str, Any]
]


# NOTE: Each protocol here inherits directly from Protocol even though it's not
# necessary, because mypy seemingly does not treat these classes as Protocols unless
# they do so.

class ConfigBaseProtocol(Protocol):
    opts: MutableMapping[str, ConfigValue]

    def write(self) -> None: ...
    def clear(self) -> None: ...


class ConfigProtocol(ConfigBaseProtocol, Protocol):
    def set(self, key: str, value: ConfigValue) -> None: ...
    def get(self, key: str) -> ConfigValue: ...
    def delete(self, key: str, ignore_keyerror: bool = False) -> None: ...
    def lappend(self, key: str, value: ConfigValue) -> None: ...
    def lremove(self, key: str, value: ConfigValue) -> None: ...
    def get_and_set(self, key: str, f: Callable[[ConfigValue], ConfigValue]) -> None: ...
    def get_and_clear(self) -> MutableMapping[str, ConfigValue]: ...
    def __contains__(self, item: str) -> ConfigValue: ...


class _HasPathSub(Protocol):
    def sub(self, key: str) -> 'PathConfigProtocol': ...
    def path_sub(self, path: str) -> 'PathConfigProtocol': ...


class _ConfigWithPathSub(ConfigProtocol, _HasPathSub, Protocol):
    ...


class PathProtocol(_HasPathSub, Protocol):
    """
    Protocol allowing operations on various config paths.
    """
    def path_get(self, path: str) -> ConfigValue: ...
    def path_set(self, path: str, value: ConfigValue) -> None: ...
    def path_delete(self, path: str) -> None: ...
    def path_lappend(self, path: str, value: ConfigValue) -> None: ...
    def path_lremove(self, path: str, value: ConfigValue) -> None: ...

    def path_sub_exists(self, path: str) -> bool: ...
    def path_create(self, path: str) -> None: ...


class PathConfigProtocol(ConfigProtocol, PathProtocol, Protocol):
    ...


# Mixin for basic configuration functions. Subclasses must implement ConfigBaseProtocol.
class ConfigMixin:
    def lappend(self: ConfigBaseProtocol, key: str, value: ConfigValue) -> None:
        l = self.opts[key]

        if not isinstance(l, MutableSequence):
            raise TypeError("config item must be a list to be appended")

        l.append(value)
        self.write()

    def lremove(self: ConfigBaseProtocol, key: str, value: ConfigValue) -> None:
        l = self.opts[key]

        if not isinstance(l, MutableSequence):
            raise TypeError("config item must be a list to be removed from")

        l.remove(value)
        self.write()

    def set(self: ConfigBaseProtocol, key: str, value: ConfigValue) -> None:
        self.opts[key] = value
        self.write()

    def get(self: ConfigBaseProtocol, key: str) -> ConfigValue:
        return self.opts[key]

    def get_and_set(
        self: ConfigBaseProtocol,
        key: str,
        f: Callable[[ConfigValue], ConfigValue]
    ) -> None:
        self.opts[key] = f(self.opts[key])
        self.write()

    def delete(self: ConfigBaseProtocol, key: str, ignore_keyerror: bool = False) -> None:
        if ignore_keyerror and key not in self.opts:
            return

        del self.opts[key]
        self.write()

    # Clears an entire config, and returns a copy of what was just cleared.
    def get_and_clear(self: ConfigBaseProtocol) -> MutableMapping[str, ConfigValue]:
        cfg = dict(self.opts)
        self.clear()
        self.write()

        return cfg

    def __contains__(self: ConfigBaseProtocol, item: str) -> ConfigValue:
        return item in self.opts


# Enable a config to get sub-configs.
class SubconfigMixin:
    def sub(self: ConfigBaseProtocol, key: str) -> 'PathConfigProtocol':
        if not isinstance(self.opts[key], MutableMapping):
            raise TypeError(f"Cannot get subconfig for non-mapping type (key {key})")

        return cast(PathConfigProtocol, SubConfig(self, key, cast(MutableMapping, self.opts[key])))


def _path_iter(
    cfg: ConfigBaseProtocol,
    path: str
) -> tuple[str, str, bool]:
    if not path:
        raise ValueError("path may not be empty or None")

    head: str
    rest: Union[str, list[str]]

    head, *rest = CONFIG_PATH_SPLIT.split(path, 1)
    if rest:
        rest = rest[0]
    else:
        rest = ""

    return (
        head, rest, isinstance(cfg.opts[head], MutableMapping)
    )


_PathifyT = TypeVar(
    "_PathifyT",
    Callable[[_ConfigWithPathSub, str], Any],
    Callable[[_ConfigWithPathSub, str, ConfigValue], Any],
    Callable[[_HasPathSub, str], Any],
    Callable[[_HasPathSub, str, ConfigValue], Any]
)


def _pathify(
    allow_self_op: bool = True
):
    """
    Use a function that operates on a Config, key, and optionally a ConfigValue,
    and convert it to a function that allows a config path to be used as the key.

    If allow_self_op is False, then if path does not refer to subconfigs, ex:

    class OnlyPathSub:
        def path_sub(path: str) -> PathConfigProtocol:
            ...

        _pathify(allow_self_op=False)
        def path_get(cfg: PathConfigProtocol, key: str) -> ConfigValue:
            return cfg.get(key)

    obj = OnlyPathSub(...)
    obj.path_get("top_level_item")

    ...then, an error will be raised. This mode of operation is meant to allow
    the creation of path functions on collections of configs that have no config
    operations (get/set/lappend/lremove/etc...) of their own. Path functions that
    declare allow_self_op=False MUST be called with at least one subpath, ex:

    obj.path_get("top_level_cfg") #  TypeError
    obj.path_get("top_level_cfg/item") #  OK
    obj.path_sub("top_level_cfg") #  OK
    """
    def deco(
        f: _PathifyT
    ) -> _PathifyT:
        # Haha this is messy and gross
        if allow_self_op:
            @functools.wraps(f)
            def _(cfg: _ConfigWithPathSub, path: str, *args):
                tail: str
                rest: Union[str, list[str]]

                *rest, tail = path.rsplit("/", 1)

                if not rest:
                    return f(cfg, tail, *args)
                else:
                    return f(cfg.path_sub(rest[0]), tail, *args)
        else:
            @functools.wraps(f)
            def _(cfg: _HasPathSub, path: str, *args):
                tail: str
                rest: Union[str, list[str]]

                *rest, tail = path.rsplit("/", 1)

                if not rest:
                    raise TypeError(f"{type(cfg)} has no config operations.")
                else:
                    # Same as above.
                    return f(cfg.path_sub(rest[0]), tail, *args)

        # Ignore mypy error here so we can use varargs for an option value argument.
        return _  # type: ignore

    return deco


class ConfigPathMixin:
    def path_sub(self: _ConfigWithPathSub, path: str) -> PathConfigProtocol:
        if not path:
            raise ValueError("path may not be empty or None")

        head, rest, is_dict = _path_iter(self, path)

        if not rest:
            # Hit end of path, so return the subconfig
            return self.sub(head)
        elif rest and not is_dict:
            # Hit end of config, but we still have path left. Error in this case.
            raise ValueError("configpath too long")
        else:
            # Not and end of path or config, so keep going
            return self.sub(head).path_sub(rest)

    def path_sub_exists(self: _ConfigWithPathSub, path: str) -> bool:
        try:
            self.path_sub(path)
            return True
        except KeyError:
            return False

    def path_create(self: _ConfigWithPathSub, path: str) -> None:
        if not path:
            raise ValueError("path may not be empty or None")

        head, *rest = CONFIG_PATH_SPLIT.split(path, 1)

        # Create if not exist
        if head not in self:
            self.set(head, {})

        # If still more path to consume, continue.
        if rest:
            self.sub(head).path_create(rest[0])

    @_pathify()
    def path_get(self: _ConfigWithPathSub, key: str) -> ConfigValue:
        return self.get(key)

    @_pathify()
    def path_set(self: _ConfigWithPathSub, key: str, value: ConfigValue) -> None:
        self.set(key, value)

    @_pathify()
    def path_delete(self: _ConfigWithPathSub, key: str) -> None:
        self.delete(key)

    @_pathify()
    def path_lappend(self: _ConfigWithPathSub, key: str, value: ConfigValue) -> None:
        self.lappend(key, value)

    @_pathify()
    def path_lremove(self: _ConfigWithPathSub, key: str, value: ConfigValue) -> None:
        self.lremove(key, value)


class SubConfig(ConfigMixin, SubconfigMixin, ConfigPathMixin):
    def __init__(
        self,
        parent: ConfigBaseProtocol,
        name: str,
        cfg: MutableMapping[str, ConfigValue]
    ):
        super().__init__()

        self.parent = parent
        self.opts = cfg
        self.name = name

        self.invalid = False

    # On clear, we create a new dict in the parent and set our reference
    # to the new storage.
    def clear(self) -> None:
        self.parent.opts[self.name] = {}
        self.opts = cast(MutableMapping[str, ConfigValue], self.parent.opts[self.name])

    def write(self) -> None:
        self.parent.write()


class ConfigException(Exception):
    pass


# Simple on-disk persistent configuration for one guild (or anything else that
# only needs one file)
#
# If check_date=True, before writing the config, we check to see if
# it's been modified after we last loaded/wrote the config. If so,
# raise an exception. Use this if you intend to edit the config manually,
# and want to make sure your modifications aren't overwritten.
class JsonConfig(ConfigMixin, SubconfigMixin, ConfigPathMixin):
    def __init__(
        self,
        path: pathlib.Path,
        template: Mapping = None,
        check_date: bool = False
    ):
        super().__init__()

        self.opts = cast(MutableMapping[str, ConfigValue], {})
        self.path = path
        self.template = {} if template is None else template
        self.check_date = check_date
        self.last_readwrite_date = 0.0
        self.init()

    def init(self) -> None:
        if self.path.exists():
            self.load()
        else:
            self.create()

    def __update_last_date(self) -> None:
        self.last_readwrite_date = round(datetime.now().timestamp(), 4)

    def load(self) -> None:
        template = self.template

        with open(self.path, 'r') as f:
            self.opts = dict(json.load(f))

        # On load, force update last date. If the json file modify
        # date has been brought past this by a manual edit, write()
        # will refuse to complete unless load() is called again.
        # (only if self.check_date=True)
        self.__update_last_date()

        if template:
            template_additions = False

            for key, value in self.template.items():
                if key not in self.opts:
                    self.opts[key] = template[key]
                    template_additions = True

            # Do not write unless we make changes here.
            if template_additions:
                self.write()

    def create(self) -> None:
        if self.template is not None:
            self.opts = dict(self.template)

        self.write()

    def clear(self) -> None:
        self.opts = {}

    def write(self) -> None:
        if self.path.exists() and self.check_date:
            file_timestamp = round(self.path.stat().st_mtime, 4)

            # If file was modified after last load/write,
            # refuse to write.
            if file_timestamp > self.last_readwrite_date:
                msg = "{} has been modified, config must be reloaded"
                logger.error(util.longstr_oneline(f"""
                    check_date conflict: {self.path}:
                    {file_timestamp} > {self.last_readwrite_date}
                    (file_timestamp > self.last_readwrite_date)
                """))
                raise ConfigException(msg.format(self.path))

        with open(self.path, 'w') as f:
            json.dump(self.opts, f, indent=4)

        self.__update_last_date()


# Very simple config database consisting of json files on disk.
# Saves a different version of the config depending on the ID.
#
# On disk structure:
# config_root_dir \_ common.json
#                 |_ <id_1>.json
#                 |_ <id_2>.json
#
class JsonConfigDB:
    def __init__(
            self,
            path: pathlib.Path,
            template: Optional[Mapping] = None,
            unique_template: bool = False
    ):
        self.db: MutableMapping[str, JsonConfig] = {}

        self.path = path
        self.template: Mapping = template if template is not None else {}
        self.unique_template = unique_template

        if path.is_dir():
            self.load_db()
        elif path.exists():
            msg = "config {} is not a directory"
            raise FileExistsError(msg.format(str(path)))
        else:  # No file or dir, so create new
            self.create_new_db()

    # Creates a new config DB
    def create_new_db(self) -> None:
        try:
            self.path.mkdir()
        except FileNotFoundError:
            logger.error("Parent directories of config not found.")
            raise

    # Backup the entire configuration to a given path.
    def backup(self, parent_path: pathlib.Path) -> None:
        path = self.backup_loc(parent_path)

        if path.exists():
            shutil.rmtree(path)

        logger.info(f"cfgdb: backup {self.path} to {path}")
        shutil.copytree(self.path, path)

    def cfg_loc(self, cid: Union[int, str]) -> pathlib.Path:
        return self.path / (str(cid) + ".json")

    def backup_loc(self, parent_dir: pathlib.Path) -> pathlib.Path:
        return parent_dir / (self.path.name + ".backup")

    def get_template(self, cid: Union[int, str]) -> Mapping:
        if self.unique_template:
            cid = str(cid)

            if cid in self.template:
                return self.template[cid]
            else:
                return {}
        else:
            return self.template

    # Loads the entire DB from a directory on disk.
    # Note that this will override any configuration currently loaded in
    # memory.
    def load_db(self) -> None:
        self.db = {}

        for child in self.path.iterdir():
            try:
                cid = child.stem
            except ValueError:
                continue

            template = self.get_template(cid)
            self.db[cid] = JsonConfig(self.cfg_loc(cid), template)
            logger.info("Load config: id {}".format(cid))

    def write_db(self) -> None:
        for cfg in self.db.values():
            cfg.write()

    # Gets the config for a single guild. If the config for a guild doesn't
    # exist, create it.
    def get_config(self, cid: Union[int, str]) -> PathConfigProtocol:
        cid = str(cid)

        if cid not in self.db:
            self.create_config(cid)

        return self.db[cid]

    def create_config(self, cid: Union[int, str]) -> None:
        cid = str(cid)
        template = self.get_template(cid)

        self.db[cid] = JsonConfig(self.cfg_loc(cid), template)

    def path_sub(self, path: str) -> 'PathConfigProtocol':
        if not path:
            raise ValueError("path may not be empty or None")

        head, *rest = CONFIG_PATH_SPLIT.split(path, 1)

        cfg: PathConfigProtocol = self.get_config(head)

        if not rest:
            return cfg
        else:
            return cfg.path_sub(rest[0])

    def path_sub_exists(self, path: str) -> bool:
        try:
            self.path_sub(path)
            return True
        except KeyError:
            return False

    def path_create(self, path: str) -> None:
        if not path:
            raise ValueError("path may not be empty or None")

        head, *rest = CONFIG_PATH_SPLIT.split(path, 1)

        # get_config creates if nonexist
        cfg = self.get_config(head)

        if rest:
            cfg.path_create(rest[0])

    # TODO: This is ugly as sin, almost an exact repeat of the path
    #       functions from ConfigPathMixin. Find a better way to do this.
    @_pathify(allow_self_op=False)
    def path_get(cfg: PathConfigProtocol, key: str) -> ConfigValue: # noqa
        return cfg.get(key)

    @_pathify(allow_self_op=False)
    def path_set(cfg: PathConfigProtocol, key: str, value: ConfigValue) -> None: # noqa
        cfg.set(key, value)

    @_pathify(allow_self_op=False)
    def path_delete(cfg: PathConfigProtocol, key: str) -> None: # noqa
        cfg.delete(key)

    @_pathify(allow_self_op=False)
    def path_lappend(cfg: PathConfigProtocol, key: str, value: ConfigValue) -> None: # noqa
        cfg.lappend(key, value)

    @_pathify(allow_self_op=False)
    def path_lremove(cfg: PathConfigProtocol, key: str, value: ConfigValue) -> None: # noqa
        cfg.lremove(key, value)
