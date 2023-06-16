"""
A debug extension allowing access to saru internals for debug purposes.
This extension is entirely optional, and offers none of the core services.
See `saru.wrapper.attach` for that.

To use:

```python
bot = lightbulb.BotApp(...)

# Make sure saru is attached before loading the extension.
saru.attach(bot, ...)
bot.load_extension("saru.extension")
```
"""
import abc
import typing as t

import lightbulb

import saru

from . import cfg

plugin = lightbulb.Plugin(
    "saru",
    "hikari-saru debug commands."
)

# Modules that are a part of this extension.
modules: t.MutableSequence['SubCommandProtocol'] = [
    cfg
]


class SubCommandProtocol(t.Protocol):
    def attach_subcommand(self, cmd: lightbulb.CommandLike) -> None: ...

    @property
    @abc.abstractmethod
    def test_suite(self) -> saru.TestSuite: ...


@plugin.command()
@lightbulb.set_help(
    "Run without arguments for a basic info message."
)
@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.command(
    "sr",
    "Commands for saru debug. Bot owner only.",
    aliases=["saru"]
)
@lightbulb.implements(lightbulb.PrefixCommandGroup)
async def sr(ctx: lightbulb.Context) -> None:
    """
    Root saru command. All `saru.extension` functionality is exposed
    through this command group.
    """
    await ctx.respond(
        f"`hikari-saru` version `{saru.__version__}` is attached correctly.\n"
        f"Run `{ctx.prefix}help sr` for help."
    )


@sr.child()  # type: ignore
@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.command(
    "selftest",
    "Perform a test of all built-in saru commands."
)
@lightbulb.implements(lightbulb.PrefixSubCommand)
async def selftest(ctx: lightbulb.Context) -> None:
    await saru.basic_selftest_command(
        ctx,
        *[mod.test_suite for mod in modules]
    )


# Perform subcommand attachment after sr has been defined.
for module in modules:
    module.attach_subcommand(sr)


def load(bot: lightbulb.BotApp) -> None:
    bot.add_plugin(plugin)


def unload(bot: lightbulb.BotApp) -> None:
    bot.remove_plugin(plugin)
