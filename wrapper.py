import asyncio
import os
import pathlib
import logging
import time
from typing import Optional, Union, Type, Any

import hikari
import lightbulb

from . import job
from . import config

from collections.abc import Mapping, MutableMapping, Iterator

logger = logging.getLogger(__name__)


GuildEntity = Union[int, hikari.Guild]


class GuildStateBase:
    def __init__(self, bot: lightbulb.BotApp, guild: hikari.Guild):
        self.bot = bot
        self.guild = guild


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


class Saru:
    """Container class implementing a set of tools with Saru."""
    def __init__(
        self,
        bot: lightbulb.BotApp,
        config_path: pathlib.Path,
        cfgtemplate: Mapping = {},
        common_cfgtemplate: Mapping = {}
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
            self.monkycfg.opts["last_schedule_id"] + 1
        )
        self.crontask = None

        self.is_ready = False

        # Create job consumer and scheduler
        loop = self.loop
        self.jobtask = loop.create_task(self.jobqueue.run())
        self.crontask = loop.create_task(self.jobcron.run())

    # Get the config object for a given job/cron header.
    def get_jobcfg_for_header(self, header: Union[job.JobHeader, job.CronHeader]) -> config.JsonConfig:
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
            lambda val: max(val, header.id)
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
        task_type: str,
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
    def gstype(self, state_type: Type[GuildStateBase]) -> None:
        self.gs_db.register_cls(state_type)

    # Shortcut to get the config for a given command.
    # Also supports messages.
    def cfg(self, guild_entity: GuildEntity):
        if isinstance(guild_entity, hikari.Guild):
            id = guild_entity.id
        elif isinstance(guild_entity, int):
            id = guild_entity
        else:
            raise TypeError("GuildEntity must be either guild or integer.")

        cfg = self.config_db.get_config(id)
        return cfg

    # Shortcut to get the guild state for a given discord object.
    # Supports ctx, ints, guilds, and anything else that has a
    # guild property.
    async def gs(self, state_type: Type[GuildStateBase], guild_entity: GuildEntity):
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
        task_type: str,
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
            guild = self.bot.rest.fetch_guild(header.guild_id)

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


class GuildStateException(Exception):
    pass


class GuildRequiredException(GuildStateException):
    pass


# Container for guild specific state that doesn't need to be saved between runs.
# A GuildState may be any type, as long as it needs no constructor arguments.
class GuildStateDB:
    def __init__(self, bot: lightbulb.BotApp):
        self.types = {}
        self.statedb = {}
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
