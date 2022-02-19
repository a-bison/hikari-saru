import json

from collections.abc import Mapping
from typing import Sequence

import lightbulb

# Character to use to acknowledge commands. Defaults to a check mark.
__ack_char: str = "\U00002705"


def override_ack_emoji(emoji: str) -> None:
    global __ack_char

    __ack_char = emoji


async def ack(ctx: lightbulb.Context) -> None:
    """React with an emoji for confirmation. By default, this is a checkmark."""
    await ctx.event.message.add_reaction(__ack_char)


def code(s: str) -> str:
    # Note: Not using any format()s here, so we can construct format strings
    # using the util.code* funcs
    return "```\n" + s + "\n```"


def codelns(lns: Sequence[str]) -> str:
    return code("\n".join(lns))


def codejson(j: Mapping) -> str:
    return code(json.dumps(j, indent=4))
