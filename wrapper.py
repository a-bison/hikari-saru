import asyncio
import functools
import os
import pathlib
import logging
import time
from types import MappingProxyType
from typing import Optional, Union, Type, Any, TypeVar, Protocol, cast

import hikari
import lightbulb

from . import job
from . import config
from .util import ack

from collections.abc import Mapping, MutableMapping, Iterator, Coroutine

logger = logging.getLogger(__name__)


GuildEntity = Union[int, hikari.Guild, lightbulb.Context]
GuildStateTV = TypeVar('GuildStateTV', bound='GuildStateBase')


# Attach a new instance of Saru to a BotApp.
def attach(
    bot: lightbulb.BotApp,
    *args: Any,
    **kwargs: Any
) -> None:
    saru = Saru(bot, *args, **kwargs)
    bot.d.saru = saru

    # Events
    bot.subscribe(hikari.StartedEvent, saru.on_bot_ready)
    bot.subscribe(hikari.GuildJoinEvent, saru.on_bot_guild_join)
    bot.subscribe(hikari.GuildLeaveEvent, saru.on_bot_guild_leave)


SaruAttachedT = Union[
    lightbulb.BotApp,
    lightbulb.Context
]


def single_dispatch_error(f_name: str, obj: Any) -> Any:
    raise NotImplementedError(f"{__name__}.{f_name}(...) not implemented for {type(obj)}")


# Get the attached instance of Saru from context.
@functools.singledispatch
def get(saru_attached: SaruAttachedT) -> 'Saru':
    return single_dispatch_error("get", saru_attached)


@get.register(lightbulb.Context)
def _(saru_attached: lightbulb.Context) -> 'Saru':
    return saru_attached.bot.d.saru


@get.register(lightbulb.BotApp)
def _(saru_attached: lightbulb.BotApp) -> 'Saru':
    return saru_attached.d.saru


@functools.singledispatch
def guild_id_from_entity(entity: GuildEntity) -> int:
    return single_dispatch_error("get", entity)


@guild_id_from_entity.register(int)
def _(entity: int) -> int:
    return entity


@guild_id_from_entity.register(hikari.Guild)
def _(entity: hikari.Guild) -> int:
    return entity.id


@guild_id_from_entity.register(lightbulb.Context)
def _(entity: lightbulb.Context) -> int:
    if entity is None:
        raise ValueError("this context does not have guild_id")

    return cast(int, entity.guild_id)


class Saru:
    @classmethod
    def get(cls, ctx: lightbulb.Context) -> 'Saru':
        return ctx.bot.d.saru

    """Container class implementing a set of tools with Saru."""
    def __init__(
        self,
        bot: lightbulb.BotApp,
        config_path: pathlib.Path,
        cfgtemplate: Mapping = MappingProxyType({}),
        common_cfgtemplate: Mapping = MappingProxyType({})
    ):
        self.bot = bot
        self.loop = asyncio.get_event_loop()

        self.__cfgtemplate = cfgtemplate

        self.__common_cfgtemplate = dict(common_cfgtemplate)
        self.__common_cfgtemplate["_monky"] = {
            # save the last schedule id, so we don't overlap new schedules
            # with old ones
            "last_schedule_id": 0
        }

        if not config_path.exists():
            os.makedirs(config_path)

        self.config_db = config.JsonConfigDB(
            config_path / "guildcfg",
            template=self.__cfgtemplate
        )
        self.common_config_db = config.JsonConfigDB(
            config_path / "commoncfg",
            template=self.__common_cfgtemplate,
            unique_template=True
        )
        self.job_db = config.JsonConfigDB(
            config_path / "jobdb",
            template={
                "jobs": {},
                "cron": {}
            }
        )
        self.gs_db = GuildStateDB(self.bot)
        self.monkycfg = self.common_config_db.get_config("_monky")

        # Task registry
        self.task_registry = job.TaskRegistry()

        # Job executor/consumer component
        self.jobqueue = job.JobQueue(self.loop)
        self.jobqueue.on_job_submit(self._cfg_job_create)
        self.jobqueue.on_job_stop(self._cfg_job_delete)
        self.jobqueue.on_job_cancel(self._cfg_job_delete)
        self.jobfactory = DiscordJobFactory(self.task_registry, self.bot)
        self.jobtask = None

        # Job scheduler component
        self.jobcron = job.JobCron(self.jobqueue, self.jobfactory)
        self.jobcron.on_create_schedule(self._cfg_sched_create)
        self.jobcron.on_delete_schedule(self._cfg_sched_delete)
        self.cronfactory = DiscordCronFactory(
            self.task_registry,
            cast(int, self.monkycfg.opts["last_schedule_id"]) + 1
        )
        self.crontask = None

        self.is_ready = False

        # Create job consumer and scheduler
        loop = self.loop
        self.jobtask = loop.create_task(self.jobqueue.run())
        self.crontask = loop.create_task(self.jobcron.run())

    # Get the config object for a given job/cron header.
    def get_jobcfg_for_header(self, header: Union[job.JobHeader, job.CronHeader]) -> config.PathConfigProtocol:
        cfg = self.job_db.get_config(header.guild_id)
        return cfg

    # INTERNAL JOB EVENTS

    # When a job is submitted, create an entry in the config DB.
    async def _cfg_job_create(self, header: job.JobHeader) -> None:
        cfg = self.get_jobcfg_for_header(header)
        cfg.sub("jobs").set(str(header.id), header.as_dict())

    # Once a job is done, delete it from the config db.
    async def _cfg_job_delete(self, header: job.JobHeader) -> None:
        cfg = self.get_jobcfg_for_header(header)
        cfg.sub("jobs").delete(str(header.id), ignore_keyerror=True)

    # Add created schedules to the config DB, and increase the
    # last_schedule_id parameter.
    async def _cfg_sched_create(self, header: job.CronHeader) -> None:
        cfg = self.get_jobcfg_for_header(header)
        cfg.sub("cron").set(str(header.id), header.as_dict())

        self.monkycfg.get_and_set(
            "last_schedule_id",
            lambda val: max(cast(int, val), header.id)
        )

    # Remove deleted schedules from the config DB.
    async def _cfg_sched_delete(self, header: job.CronHeader) -> None:
        cfg = self.get_jobcfg_for_header(header)
        cfg.sub("cron").delete(str(header.id))

    # DISCORD LINKS

    # Resume all jobs that never properly finished from the last run.
    # Called from on_ready() to ensure that all discord state is init'd
    # properly
    async def resume_jobs(self):
        for guild_id, cfg in self.job_db.db.items():
            jobs = cfg.sub("jobs").get_and_clear()

            for job_id, job_header in jobs.items():
                await self.resume_job(job_header)

            msg = "Resumed {} unfinished job(s) in guild {}"
            logger.info(msg.format(len(jobs), guild_id))

    # Resume job from a loaded job header dict.
    async def resume_job(self, header):
        job = await self.jobfactory.create_job_from_dict(header)
        await self.jobqueue.submit_job(job)

    # Reschedule all cron entries from cfg
    async def reschedule_all_cron(self):
        for guild_id, cfg in self.job_db.db.items():
            crons = cfg.sub("cron").get_and_clear()

            for sched_id, sched_header in crons.items():
                await self.reschedule_cron(sched_header)

            msg = "Loaded {} schedule(s) in guild {}"
            logger.info(msg.format(len(crons), guild_id))

    async def reschedule_cron(self, header_dict):
        header = await self.cronfactory.create_cronheader_from_dict(header_dict)
        await self.jobcron.create_schedule(header)

    async def join_guilds_offline(self):
        """Create config entries for any guilds that were joined while offline."""
        # TODO Investigate bug in fetch_my_guilds: newest_first appears to repeat guilds?
        async for guild in self.bot.rest.fetch_my_guilds():
            logger.info("In guilds: {}({})".format(guild.name, guild.id))
            _ = self.config_db.get_config(guild.id)
            _ = self.job_db.get_config(guild.id)

        self.config_db.write_db()
        self.job_db.write_db()

    async def on_bot_ready(self, event: hikari.StartedEvent) -> None:
        """Function to call when bot is started and connected. This function MUST be called in order for jobs to
        resume properly."""
        if not self.is_ready:
            await self.reschedule_all_cron()
            await self.resume_jobs()
            await self.join_guilds_offline()

            self.is_ready = True

        logger.info("Saru ready.")

    async def on_bot_guild_join(self, event: hikari.GuildJoinEvent):
        """Optional guild join event handler."""
        g = event.guild
        logger.info(f"Joined new guild: {g.name}({g.id})")

    async def on_bot_guild_leave(self, event: hikari.GuildLeaveEvent):
        """Guild leave event handler. Must be fired in order to avoid corruption of guild state DB."""
        g = event.old_guild
        if g is None:
            logger.info(f"Left guild: {event.guild_id}(cache miss)")
        else:
            logger.info(f"Left guild: {g.name}({g.id}")

        await self.gs_db.delete(event.guild_id)

    # Enqueue a new job. Returns the created job object.
    async def start_job(
        self,
        ctx: lightbulb.Context,
        task_type: Union[str, Type[job.JobTask]],
        properties: Mapping
    ) -> job.Job:

        job = await self.jobfactory.create_job(ctx, task_type, properties)
        await self.jobqueue.submit_job(job)

        return job

    # Schedule a job
    async def schedule_job(
        self,
        ctx: lightbulb.Context,
        task_type: str,
        properties: Mapping,
        cron_str: str
    ) -> job.CronHeader:

        chdr = await self.cronfactory.create_cronheader(
            ctx,
            properties,
            task_type,
            cron_str
        )
        await self.jobcron.create_schedule(chdr)

        return chdr

    # Register a task class.
    def task(self, tsk: Type[job.JobTask]) -> None:
        self.task_registry.register(tsk)

    # Register a guild state class.
    def gstype(self, state_type: Type['GuildStateBase']) -> None:
        self.gs_db.register_cls(state_type)

    # Shortcut to get the guild config for a given command.
    def gcfg(
        self,
        guild_entity: GuildEntity,
        path: Optional[str] = None,
        force_create: bool = False
    ) -> config.PathConfigProtocol:
        """Shortcut to get guild cfg, or a subconfig of one."""

        id = guild_id_from_entity(guild_entity)
        cfg = self.config_db.get_config(id)

        if path is None:
            return cfg
        else:
            if force_create:
                cfg.path_create(path)

            return cfg.path_sub(path)

    def ccfg(self, path: str, force_create: bool = False) -> config.PathConfigProtocol:
        """Shortcut to get common cfg."""
        if force_create:
            self.common_config_db.path_create(path)

        return self.common_config_db.path_sub(path)

    def cfg(
        self,
        path: str,
        guild_entity: Optional[GuildEntity] = None,
        force_create: bool = False
    ) -> config.PathConfigProtocol:
        """Combined shortcut method for getting config objects.

        Provided paths must take one of the following forms:
        c/... - Common config
        g/... - Guild config

        If a g/... path is used, guild_entity must not be None.
        """
        pathtype, *rest = config.CONFIG_PATH_SPLIT.split(path, 1)

        if pathtype == "g":
            if guild_entity is None:
                raise ValueError("guild_entity must not be None for g/... paths")

            if not rest:
                sub_path = None
            else:
                sub_path = rest[0]

            return self.gcfg(guild_entity, sub_path, force_create)
        elif pathtype == "c":
            if not rest:
                raise ValueError("must provide config name for c/... path")
            else:
                sub_path = rest[0]

            return self.ccfg(sub_path, force_create)
        else:
            raise ValueError("first config node must be either g or c")

    # Shortcut to get the guild state for a given discord object.
    # Supports ctx, ints, guilds, and anything else that has a
    # guild property.
    async def gs(self, state_type: Type['GuildStateBase'], guild_entity: GuildEntity):
        return await self.gs_db.get(state_type, guild_entity)


######################################
# JOB INFRASTRUCTURE IMPLEMENTATIONS #
######################################
# Discord-specific aspects of core.job.

# Implementation of discord-specific aspects of constructing job objects.
class DiscordJobFactory(job.JobFactory):
    def __init__(
        self,
        task_registry: job.TaskRegistry,
        bot: lightbulb.BotApp
    ):
        super().__init__(task_registry)
        self.bot = bot

    # Create a new jobheader.
    async def create_jobheader(
        self,
        ctx: lightbulb.Context,
        properties: Mapping,
        task_type: str,
        schedule_id: Optional[int]
    ) -> job.JobHeader:
        if ctx.guild_id is None:
            raise ValueError("ctx must have guild id")

        header = job.JobHeader(
            self.next_id(),
            task_type,
            properties,
            ctx.author.id,
            ctx.guild_id,
            int(time.time()),
            schedule_id
        )

        return header

    # Create a new job.
    async def create_job(
        self,
        ctx: lightbulb.Context,
        task_type: Union[str, Type[job.JobTask]],
        properties: Mapping,
        schedule_id: Optional[int] = None
    ) -> job.Job:

        task_type = self.task_registry.force_str(task_type)
        header = await self.create_jobheader(ctx, properties, task_type, schedule_id)
        j = await self.create_job_from_jobheader(header)
        return j

    # OVERRIDE
    # Discord tasks take some extra constructor parameters, so we need to
    # construct those jobs through the DiscordJobFactory.
    async def create_task(
        self,
        header: job.JobHeader,
        guild: Optional[hikari.Guild] = None
    ) -> job.JobTask:
        if guild is None:
            guild = await self.bot.rest.fetch_guild(header.guild_id)

        task_cls = self.task_registry.get(header.task_type)
        task = task_cls(self.bot, guild)

        return task


# Companion to the JobFactory. No core.job counterpart.
class DiscordCronFactory:
    def __init__(
        self,
        registry: job.TaskRegistry,
        start_id: int = 0
    ):
        self.task_registry = registry
        self.id_counter = job.CountingIdGenerator(start_id)

    async def create_cronheader(
        self,
        ctx: lightbulb.Context,
        properties: Mapping,
        task_type: str,
        cron_str: str
    ) -> job.CronHeader:
        if ctx.guild_id is None:
            raise ValueError("ctx must have guild id")

        header = job.CronHeader(
            self.id_counter.next_id(),
            self.task_registry.force_str(task_type),
            properties,
            ctx.author.id,
            ctx.guild_id,
            cron_str
        )

        return header

    @staticmethod
    async def create_cronheader_from_dict(header_dict: Mapping) -> job.CronHeader:
        return job.CronHeader.from_dict(header_dict)


#################
# DISCORD TASKS #
#################
# An assortment of JobTasks for Discord.

# Task that sends a discord message on a timer to a given channel.
class MessageTask(job.JobTask):
    MAX_MSG_DISPLAY_LEN = 15

    def __init__(self, bot, guild):
        super().__init__()

        self.bot = bot
        self.guild = guild

    async def run(self, header):
        p = header.properties
        channel = self.guild.get_channel(p["channel"])

        for _ in range(p["post_number"]):
            await channel.send(p["message"])
            await asyncio.sleep(p["post_interval"])

    @classmethod
    def task_type(cls):
        return "message"

    @classmethod
    def property_default(cls, properties):
        return {
            "message": "hello",
            "channel": 0,
            "post_interval": 1,  # SECONDS
            "post_number": 1
        }

    def display(self, header):
        p = header.properties
        msg = p["message"]

        if len(msg) > MessageTask.MAX_MSG_DISPLAY_LEN:
            msg = msg[0:MessageTask.MAX_MSG_DISPLAY_LEN] + "..."

        fmt = "message=\"{}\" post_interval={} post_number={}"

        return fmt.format(msg, p["post_interval"], p["post_number"])


################
# GUILD STATES #
################

class GuildStateBase:
    _cfg_path: Optional[str] = None

    @classmethod
    async def get(cls: Type[GuildStateTV], ctx: lightbulb.Context) -> GuildStateTV:
        """Shortcut function for getting a GuildState instance from ctx"""
        if ctx.guild_id is None:
            raise ValueError("ctx must have guild id")

        db = await get(ctx).gs(cls, ctx.guild_id)
        return db

    @classmethod
    def register(cls, bot: lightbulb.BotApp):
        """Shortcut function for registering a GuildState class to Saru."""
        bot.d.saru.gstype(cls)

    @classmethod
    def unregister(cls, bot: lightbulb.BotApp):
        """Shortcut function for unregistering a GuildState class from Saru"""
        gs_db: GuildStateDB = get(bot).gs_db
        gs_db.unregister_cls(cls)

    def __init__(self, bot: lightbulb.BotApp, guild: hikari.Guild):
        self.bot = bot
        self.guild = guild
        self.__cfg: Optional[config.PathConfigProtocol] = None
        self.__cfg_set_from_deco = False

        cfg_path = type(self)._cfg_path
        if cfg_path is not None:
            self.__cfg = get(bot).cfg(cfg_path, guild, force_create=True)
            self.__cfg_set_from_deco = True

    @property
    def cfg(self) -> Optional[config.PathConfigProtocol]:
        """Get the config backing this guild state.

        Will be None unless the @config_backed decorator is used.
        """
        return self.__cfg

    @cfg.setter
    def cfg(self, c: config.PathConfigProtocol):
        """Set the config backing this guild state.

        If @config_backed was used, this may not be changed.
        """
        if self.__cfg_set_from_deco:
            raise AttributeError("cfg cannot be set, was set from decorator")

        self.__cfg = c


def config_backed(config_path: str):
    """Second order decorator that sets up a backing config for a
    GuildState type.
    """
    def deco(gs_type: Type[GuildStateTV]) -> Type[GuildStateTV]:
        gs_type._cfg_path = config_path
        return gs_type

    return deco


def register(bot: lightbulb.BotApp):
    """Second order decorator that calls .register(bot) on the decorated
    type.
    """
    def deco(gs_type: Type[GuildStateTV]) -> Type[GuildStateTV]:
        gs_type.register(bot)
        return gs_type


class GuildStateException(Exception):
    pass


class GuildRequiredException(GuildStateException):
    pass


# Container for guild specific state that doesn't need to be saved between runs.
class GuildStateDB:
    def __init__(self, bot: lightbulb.BotApp):
        self.types: MutableMapping[str, Type[GuildStateBase]] = {}
        self.statedb: MutableMapping[
            str,
            MutableMapping[int, GuildStateBase]
        ] = {}
        self.bot = bot

    @staticmethod
    def typekey(state_type: Type[GuildStateBase]) -> str:
        return state_type.__qualname__

    def register_cls(self, state_type: Type[GuildStateBase]) -> None:
        if not issubclass(state_type, GuildStateBase):
            msg = "GuildState type {} must subclass GuildStateBase"
            raise GuildStateException(msg.format(state_type.__name__))

        k = self.typekey(state_type)
        if k in self.types:
            msg = "GuildState type {} is already registered."
            raise GuildStateException(msg.format(state_type.__name__))

        self.types[k] = state_type
        self.statedb[k] = {}

    def unregister_cls(self, state_type: Type[GuildStateBase]) -> None:
        k = self.typekey(state_type)
        self.__check_typekey(k)

        del self.types[k]
        del self.statedb[k]

    async def _get_guild_and_id(self, guild_entity: Optional[GuildEntity]) -> tuple[hikari.Guild, int]:
        if guild_entity is None:
            raise GuildRequiredException()

        if isinstance(guild_entity, hikari.Guild):
            guild = guild_entity
        elif isinstance(guild_entity, int):
            guild = await self.bot.rest.fetch_guild(guild_entity)
        else:
            raise TypeError("Guild key must be guild, or be an integer.")

        return guild, guild.id

    def __check_typekey(self, k: str) -> None:
        if k not in self.types:
            msg = "GuildState type {} has not been registered."
            raise GuildStateException(msg.format(k))

    # Get guild state dict for a given type, and the type itself.
    def __get_of_type(
        self,
        state_type: Type[GuildStateBase]
    ) -> tuple[Type[GuildStateBase], MutableMapping[int, GuildStateBase]]:

        k = self.typekey(state_type)
        self.__check_typekey(k)

        return self.types[k], self.statedb[k]

    # Get a state instance from the DB. If there's no
    # instance for the given guild, one will be created.
    async def get(self, state_type: Type[GuildStateBase], guild_entity: GuildEntity) -> GuildStateBase:
        guild, guild_id = await self._get_guild_and_id(guild_entity)
        state_type, guild_states = self.__get_of_type(state_type)

        try:
            return guild_states[guild_id]
        except KeyError:
            gs = state_type(self.bot, guild)
            guild_states[guild_id] = gs
            return gs

    # Clear all state associated with the given guild.
    async def delete(self, guild_entity: GuildEntity) -> None:
        _, guild_id = await self._get_guild_and_id(guild_entity)

        for states in self.statedb.values():
            if guild_id in states:
                del states[guild_id]

    # Iterate over all guild states of a given type
    def iter_over_type(self, state_type: Type[GuildStateBase]) -> Iterator[GuildStateBase]:
        state_type, guild_states = self.__get_of_type(state_type)
        yield from guild_states.values()


#####################
# COMMAND UTILITIES #
#####################


class ConfigCallbackProtocol(Protocol):
    __name__: str

    def __call__(
        self,
        ctx: lightbulb.Context,
        cfg: config.ConfigProtocol,
        key: str,
        value: config.ConfigValue
    ) -> Coroutine[Any, Any, None]: ...


def config_command(
    implements: Type[lightbulb.Command] = lightbulb.PrefixCommand,
    type: Optional[Type] = str,
    path: str = "g",
    default_on_non_exist: Any = hikari.UNDEFINED,
    key: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    require_admin: bool = True,
    **command_kwargs: Any
):
    """
    Generates a new configuration command.
    """
    def deco(coro: ConfigCallbackProtocol) -> lightbulb.CommandLike:
        if key is None:
            config_key = coro.__name__
        else:
            config_key = key

        command_name = config_key.replace("_", "-") if not name else name
        command_desc = (
            f"Get/set {config_key.replace('_', ' ').lower()}"
            if not description else description
        )

        @lightbulb.option(
            "value",
            "The value to set.",
            type=type,
            default=None
        )
        @lightbulb.command(
            command_name,
            command_desc,
            **command_kwargs
        )
        @lightbulb.implements(implements)
        async def _(ctx: lightbulb.Context) -> None:
            if ctx.guild_id is None:
                raise ValueError("ctx must have guild id")

            if not isinstance(ctx.event, hikari.MessageCreateEvent):
                raise NotImplementedError("not implemented for non-message ctx")

            if ctx.event.message.member is None:
                raise ValueError("ctx must have a member (invoke in guild only)")

            cfg: config.PathConfigProtocol = get(ctx).cfg(path, ctx.guild_id)
            value = ctx.options.value

            if key not in cfg and default_on_non_exist is not hikari.UNDEFINED:
                logger.warning(f"cfg_command: {key} set to default {default_on_non_exist}")
                cfg.set(key, default_on_non_exist)

            # GET
            if value is None:
                display_name = config_key.replace("_", " ").capitalize()
                value = cfg.get(config_key)
                await ctx.respond(f"{display_name} is {value}.")
                return

            # SET
            if require_admin:
                perms = lightbulb.utils.permissions_for(
                    ctx.event.message.member
                )

                is_admin = (
                    perms & hikari.Permissions.ADMINISTRATOR or
                    ctx.author.id == ctx.get_guild().owner_id
                )

                if perms == hikari.Permissions.NONE:
                    await ctx.respond("Internal error: cache not available")
                    return
                elif not is_admin:
                    await ctx.respond("You must be administrator to set this value.")
                    return

            await coro(ctx, cfg, config_key, value)
            cfg.set(config_key, value)
            await ack(ctx)

        return _

    return deco