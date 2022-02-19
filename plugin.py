# saru.plugin:
#   A default/reference implementation of a lightbulb extension that uses Saru.

import logging
import lightbulb

logger = logging.getLogger(__name__)

plugin = lightbulb.Plugin("Saru")


def load(bot):
    bot.add_plugin(plugin)


def unload(bot):
    bot.remove_plugin(plugin)