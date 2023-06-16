"""
The `cfg` subcommand implementation.
"""

import collections.abc
import json
import logging
import typing as t

import lightbulb

import saru

logger = logging.getLogger(__name__)

CfgOperationCallbackT = t.Callable[[saru.Config, str], None]
CfgValueOperationCallbackT = t.Callable[[saru.Config, str, saru.ConfigValueT], None]

CfgCommandCallbackT = t.Union[
    CfgOperationCallbackT,
    CfgValueOperationCallbackT
]

test_suite: saru.TestSuite[lightbulb.Context] = saru.TestSuite()
saru.add_command_tests(
    test_suite,
    [
        "sr cfg ls g",
        "sr cfg ls c",
        f"sr cfg ls c/{saru.SARU_INTERNAL_CFG}",
        f"sr cfg get c/{saru.SARU_INTERNAL_CFG}/selftest/nonexist",
        f"sr cfg get c/{saru.SARU_INTERNAL_CFG}/nonexist",
        f"sr cfg set c/{saru.SARU_INTERNAL_CFG}/selftest []",
        f"sr cfg append c/{saru.SARU_INTERNAL_CFG}/selftest \"test_value\"",
        f"sr cfg get c/{saru.SARU_INTERNAL_CFG}/selftest",
        f"sr cfg remove c/{saru.SARU_INTERNAL_CFG}/selftest \"test_value\"",
        f"sr cfg delete c/{saru.SARU_INTERNAL_CFG}/selftest"
    ]
)


def path_is_cfg_root(path: str) -> bool:
    """
    Tests if a path is of the form `g` or `c/ROOTNAME`.
    """
    path_parsed = saru.cfg_path_parse(path)

    return (
        (len(path_parsed) == 1) or
        (len(path_parsed) == 2 and path_parsed[0] == "c")
    )


async def handle_cfg_error(ctx: lightbulb.Context, exc: Exception) -> None:
    """
    Post the appropriate response for an exception raised
    during a config operation. If the exception was not handled,
    it will be re-raised.

    Args:
        ctx: Command context.
        exc: The exception to handle.
    """
    if isinstance(exc, KeyError):
        await ctx.respond(f"Key {str(exc)} does not exist.")
        return
    elif isinstance(exc, (saru.ConfigPathException, saru.ConfigException)):
        await ctx.respond(str(exc))
        return
    else:
        raise exc


async def do_cfg_op(
    ctx: lightbulb.Context,
    op: CfgOperationCallbackT,
    force_create: bool
) -> None:
    """
    Performs a config write command, handling errors and
    responding where appropriate. The config path is obtained
    from `ctx.options.path`.

    `op` is passed the subconfig object located at the path's dirname,
    then the basename.
    """
    path: str = ctx.options.path
    s = saru.get(ctx)

    *cfg_path_parsed, item_key = saru.cfg_path_parse(path)
    cfg_path = saru.cfg_path_build(cfg_path_parsed)

    try:
        if path_is_cfg_root(path):
            await ctx.respond("Cannot overwrite a config root.")
        else:
            cfg = s.cfg(cfg_path, ctx, force_create=force_create)
            op(cfg, item_key)
            cfg.write()
            await saru.ack(ctx)
    except Exception as e:
        await handle_cfg_error(ctx, e)


def as_cfg_write_command(
    name: str,
    description: str,
    force_create: bool = False,
    novalue: bool = False
) -> t.Callable[
    [CfgCommandCallbackT],
    lightbulb.CommandLike
]:
    """
    Second order decorator that converts a function into a standard
    config-modfying command.

    This decorator will return a `lightbulb.CommandLike`, which may
    then be modified further and added to a plugin/bot.

    Each command will be of the form
    ```txt
    <prefix><name> <path> json_object
    ```
    For example...
    ```txt
    ?set c/some-cfg/some-value {"any": "valid", "json": "here"}
    ```

    Args:
        name: Command name, supplied to `@lightbulb.command`.
        description: Command description, supplied to `@lightbulb.command`.
        force_create: If `True`, the command will always create the item being operated on.
        novalue: If `True`, the JSON option will not be added to the command.
    """
    def decorator(f: CfgCommandCallbackT) -> lightbulb.CommandLike:
        @lightbulb.option(
            "path",
            f"The path of the item to {name}."
        )
        @lightbulb.command(
            name,
            description
        )
        @lightbulb.implements(lightbulb.PrefixSubCommand)
        async def cfg_write_command(ctx: lightbulb.Context) -> None:
            op: CfgOperationCallbackT

            if novalue:
                # For no value, simple passthrough.
                # Assume that `f` takes no config value.
                op = t.cast(CfgOperationCallbackT, f)
            else:
                try:
                    obj = json.loads(ctx.options.item)
                except json.JSONDecodeError as e:
                    await ctx.respond(f"Could not parse JSON: {e}")
                    return

                # Need to use closure to pass cfg value in
                # Fun type stuff here for checker
                def op(cfg: saru.Config, key: str) -> None:
                    wrapped = t.cast(CfgValueOperationCallbackT, f)
                    wrapped(cfg, key, obj)

            await do_cfg_op(
                ctx,
                op=op,
                force_create=force_create
            )

        # Can't use conditionals with decorator syntax, so need to
        # add option after the fact.
        if not novalue:
            return lightbulb.option(
                "item",
                f"The item to {name}, in JSON format.",
                modifier=lightbulb.OptionModifier.CONSUME_REST
            )(cfg_write_command)

        return cfg_write_command

    return decorator


# Note: child() call is missing intentionally, this subcommand
# is attached to `sr` later through `attach_subcommand`
@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.command(
    "cfg",
    "Arbitrary operations on configuration. Bot owner only.",
    aliases=["conf", "config"]
)
@lightbulb.implements(lightbulb.PrefixSubGroup)
async def cfg(ctx: lightbulb.Context) -> None:
    await ctx.respond(
        "`sr cfg` must be run with a subcommand.\n"
        f"Run `{ctx.prefix}help sr cfg` for help."
    )


@cfg.child()  # type: ignore
@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.option(
    "path",
    "The config path."
)
@lightbulb.command(
    "ls",
    "List all keys in a config path."
)
@lightbulb.implements(lightbulb.PrefixSubCommand)
async def cfg_ls(ctx: lightbulb.Context) -> None:
    path: str = ctx.options.path
    s = saru.get(ctx)

    parsed_path = saru.cfg_path_parse(path)

    items: t.ItemsView[t.Union[str, int], saru.ConfigValueT]

    try:
        if len(parsed_path) == 1 and parsed_path[0] == "c":
            items = s.common_config_directory.items()
        else:
            items = s.cfg(path, ctx).items()
    except Exception as e:
        await handle_cfg_error(ctx, e)
        return

    # Format different types differently, so it's easier to tell
    # what further operations can be taken.
    dicts = []
    seqs = []
    other = []
    for k, v in items:
        if isinstance(v, collections.abc.MutableMapping):
            # goofy looking, will be "{key}"
            dicts.append(f"{{{k}}}")
        elif isinstance(v, collections.abc.MutableSequence):
            seqs.append(f"[{k}]")
        else:
            other.append(str(k))

    out_items = sorted(dicts) + sorted(seqs) + sorted(other)
    await ctx.respond(saru.code(" ".join(out_items)))


@cfg.child()  # type: ignore
@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.option(
    "path",
    "The config path."
)
@lightbulb.command(
    "get",
    "Get a config value."
)
@lightbulb.implements(lightbulb.PrefixSubCommand)
async def cfg_get(ctx: lightbulb.Context) -> None:
    path: str = ctx.options.path
    s = saru.get(ctx)

    *cfg_path_parsed, item_key = saru.cfg_path_parse(path)
    cfg_path = saru.cfg_path_build(cfg_path_parsed)

    item: saru.ConfigValueT = "Should never see this value :)"

    try:
        if path_is_cfg_root(path):
            item = s.cfg(path, ctx).root
        else:
            cfg = s.cfg(cfg_path, ctx)
            item = cfg[item_key]
    except Exception as e:
        await handle_cfg_error(ctx, e)
        return

    await ctx.respond(saru.code(
        json.dumps(item, indent=4)
    ))


@cfg.child()  # type: ignore
@as_cfg_write_command(
    "set",
    "Set a configuration option."
)
def cfg_set(cfg: saru.Config, key: str, value: saru.ConfigValueT) -> None:
    cfg[key] = value


@cfg.child()  # type: ignore
@as_cfg_write_command(
    "delete",
    "Delete a configuration option.",
    novalue=True
)
def cfg_delete(cfg: saru.Config, key: str) -> None:
    del cfg[key]


@cfg.child()  # type: ignore
@as_cfg_write_command(
    "append",
    "Append a configuration object to a sequence."
)
def cfg_append(cfg: saru.Config, key: str, value: saru.ConfigValueT) -> None:
    item = cfg[key]

    if not isinstance(item, collections.abc.MutableSequence):
        raise saru.ConfigException("Can only append to a sequence. Initialize one with `set PATH []`.")
    else:
        item.append(value)


@cfg.child()  # type: ignore
@as_cfg_write_command(
    "remove",
    "Remove a configuration object from a sequence."
)
def cfg_remove(cfg: saru.Config, key: str, value: saru.ConfigValueT) -> None:
    item = cfg[key]

    if not isinstance(item, collections.abc.MutableSequence):
        raise saru.ConfigException("Can only remove from a sequence. Initialize one with `set PATH []`.")
    else:
        item.remove(value)


def attach_subcommand(parent: lightbulb.CommandLike) -> None:
    parent.child(cfg)  # type: ignore
