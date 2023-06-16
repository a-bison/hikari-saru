import copy
import json
import numbers
import textwrap
from collections.abc import Mapping
from typing import Optional, Sequence, Union

import hikari
import lightbulb

# Character to use to acknowledge commands. Defaults to a check mark.
__ack_char: str = "\U00002705"


def override_ack_emoji(emoji: str) -> None:
    global __ack_char

    __ack_char = emoji


async def ack(ctx: lightbulb.Context) -> None:
    """React with an emoji for confirmation. By default, this is a checkmark."""
    if isinstance(ctx.event, hikari.MessageCreateEvent):
        await ctx.event.message.add_reaction(__ack_char)
    else:
        raise NotImplementedError(f"ack not implemented for {type(ctx.event)}")


def code(s: str, lang: str = "") -> str:
    # Note: Not using any format()s here, so we can construct format strings
    # using the util.code* funcs
    return "".join([
        "```",
        lang,
        "\n",
        s,
        "\n```"
    ])


def codelns(lns: Sequence[str], lang: str = "") -> str:
    return code("\n".join(lns), lang)


def codejson(j: Mapping) -> str:
    return code(json.dumps(j, indent=4), lang="json")


def longstr_fix(s: str) -> str:
    return textwrap.dedent(s).strip()


def longstr_oneline(s: str) -> str:
    return " ".join(longstr_fix(s).split("\n"))


async def respond_code_or_txt(ctx: lightbulb.Context, text: str) -> None:
    """
    Respond to a message with code. By default, `text` will be enclosed in ``` and posted.
    If that message would be too large, it will be sent as a text file instead.
    """
    msg = code(text)

    if len(msg) <= 2000:
        await ctx.respond(msg)
        return

    # Too big, so send as file instead.
    await ctx.respond(
        "Response too large, sending as a `.txt` file.",
        attachment=hikari.Bytes(text.encode('utf-8'), "response.txt")
    )


async def invoke_prefix_command(
    ctx: lightbulb.PrefixContext,
    command: str
) -> None:
    """
    Programmatically invoke a lightbulb prefix command. This is accomplished by
    copying an existing context object and making tweaks to it.

    Useful for implementing aliases, or for testing.
    """
    invoked_prefix = ctx.prefix
    invoked_with, *args = command.strip().split(maxsplit=1)
    command_obj = ctx.bot.get_prefix_command(invoked_with)

    if command_obj is None:
        raise ValueError(f"Could not find command named \"{invoked_with}\"")

    # Need to override the content of the message event
    new_message = copy.deepcopy(ctx.event.message)
    new_message.content = invoked_prefix + command

    if isinstance(ctx.event, (hikari.GuildMessageCreateEvent, hikari.DMMessageCreateEvent)):
        # Event can either be GuildMessageCreateEvent or DMMessageCreateEvent.
        # Both take the same arguments for construction.
        new_event = type(ctx.event)(
            message=new_message,
            shard=ctx.event.shard  # Use same shard.
        )
    else:
        raise TypeError(f"Cannot invoke a prefix command from {str(type(ctx.event))}")

    new_ctx = lightbulb.PrefixContext(ctx.bot, new_event, command_obj, invoked_with, invoked_prefix)

    new_ctx._parser = (command_obj.parser or lightbulb.utils.Parser)(
        new_ctx,
        args[0] if args else ""
    )

    await command_obj.invoke(new_ctx)


def rangelimit(
    low: Optional[numbers.Real],
    val: numbers.Real,
    high: Optional[numbers.Real],
    valname: Optional[str]
) -> None:
    """Check if val is between low and high. If not, raise a RangeLimitError."""

    if low is None and high is None:
        raise ValueError("low and high cannot both be None")

    if not (__rangecheck_lower(low, val) and __rangecheck_upper(high, val)):
        raise RangeLimitError(low, val, high, valname)


def __rangecheck_lower(
        low: Optional[numbers.Real],
        val: numbers.Real
) -> bool:
    if low is None:
        return True

    return low <= val


def __rangecheck_upper(
        high: Optional[numbers.Real],
        val: numbers.Real
) -> bool:
    if high is None:
        return True

    return val <= high


class RangeLimitError(Exception):
    def __init__(
        self,
        low: Optional[numbers.Real],
        val: numbers.Real,
        high: Optional[numbers.Real],
        valname: Optional[str]
    ):
        self.low = low
        self.val = val
        self.high = high
        self.valname = valname

    def __str__(self) -> str:
        if self.high is None:
            msg = f"`{self.valname}` cannot go below {self.low}"
        elif self.low is None:
            msg = f"`{self.valname}` cannot exceed {self.high}"
        else:
            msg = f"`{self.valname}` must be between {self.low} and {self.high}"

        return msg
