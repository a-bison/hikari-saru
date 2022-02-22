# hikari-saru

Personal toolkit for making discord bots, with a focus on per-guild configuration and task management. Made for
[hikari](https://github.com/hikari-py/hikari) / [lightbulb](https://github.com/tandemdude/hikari-lightbulb). 

This library is still an early early version and has a lot of problems, so I wouldn't recommend using it for anything important.

# Usage

#### Configuration Example

```python
# Import lightbulb and saru
import lightbulb
import saru

# other imports
from pathlib import Path

# Instantiate a bot instance
bot = lightbulb.BotApp(token="your_token_here", prefix="your_prefix_here")
# Attach an instance of the Saru wrapper to BotApp
saru.attach(bot, config_path=Path("your_config_folder"))


# Define a configuration class. Use multiple GuildState classes between
# lightbulb extensions to help keep things contained.
@saru.register(bot)
@saru.config_backed("myconfig")
class MyGuildState(saru.GuildStateBase):
    def write_my_value(self, value: str) -> None:
        self.cfg.set("myvalue", value)

    def get_my_value(self) -> str:
        return self.cfg.get("myvalue")


# Set up some commands to access your config. See lightbulb's usage examples
# for more details on the command system.
@bot.command()
@lightbulb.option("value", "The value to set.")
@lightbulb.command("set-my-value")
@lightbulb.implements(lightbulb.PrefixCommand)
async def set_my_value(ctx: lightbulb.Context) -> None:
    gs = await MyGuildState.get(ctx)
    gs.write_my_value(ctx.options.value)    
    await ctx.respond("Value set.")
    
    # The wrapper Saru class and all its components may also
    # be accessed directly from the bot datastore, if you
    # don't want to use GuildStates.
    my_config = bot.d.saru.cfg(ctx.guild_id).sub("myconfig")
    my_config.set("myvalue")
    await ctx.respond("Value set again.")
    

@bot.command()
@lightbulb.command("get-my-value")
@lightbulb.implements(lightbulb.PrefixCommand)
async def get_my_value(ctx: lightbulb.Context) -> None:
    gs = await MyGuildState.get(ctx)  
    await ctx.respond(f"Your value is: {gs.get_my_value()}")
    

bot.run()

```

#### Task Example

TODO

# Documentation

TODO

# Additional Notes

This library was originally called [monkycord](https://github.com/a-bison/monkycord). Due to discord.py getting
discontinued, and because I wanted to learn something new, I migrated this toolkit to [hikari](https://github.com/hikari-py/hikari).
