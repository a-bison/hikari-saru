import json

from collections.abc import Mapping
from typing import Sequence

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
