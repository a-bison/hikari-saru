"""
The `job` subcommand implementation.

This subcommand provides a simple debug interface for managing the
job queue.
"""
import logging
import hikari
import lightbulb
import typing as t

import saru

logger = logging.getLogger(__name__)

test_suite: saru.TestSuite[lightbulb.Context] = saru.TestSuite()
saru.add_command_tests(
    test_suite,
    [

    ]
)


class JobFilterProtocol(t.Protocol):
    def __call__(self, context: lightbulb.Context, arg: t.Optional[str]) -> t.Mapping[int, saru.Job]: ...


def jobfilter_all(ctx: lightbulb.Context, arg: t.Optional[str]) -> t.Mapping[int, saru.Job]:
    return saru.get(ctx).jobqueue.jobs


def jobfilter_header_value(value_name: str, default: t.Callable[[lightbulb.Context], int]) -> JobFilterProtocol:
    """
    Generalized second order job filter. Allows filtering on any numeric value
    contained in the header.
    """
    def _(ctx: lightbulb.Context, arg: t.Optional[str]) -> t.Mapping[int, saru.Job]:
        if arg is None:
            filter_value = default(ctx)
        else:
            try:
                filter_value = int(arg)
            except ValueError:
                raise ValueError(f"Bad argument format for filter \"{ctx.options.filter}\", must be an integer or left blank.")

        return {
            id: job for id, job in saru.get(ctx).jobqueue.jobs.items()
            if getattr(job.header, value_name) == filter_value
        }

    return _


JOBFILTERS: t.Mapping[str, JobFilterProtocol] = {
    "all": jobfilter_all,
    "guild": jobfilter_header_value("guild_id", default=lambda ctx: ctx.guild_id),
    "owner": jobfilter_header_value("owner_id", default=lambda ctx: ctx.user.id)
}


def pretty_print_job(cache: hikari.api.Cache, job: saru.Job) -> str:
    h = job.header
    user = cache.get_user(h.owner_id)
    username = user.username if user is not None else h.owner_id

    s_parts = [
        f"{h.id}:",
        f"guild={h.guild_id}",
        f"owner={username}",
        f"type={h.task_type}",
        f"sched={h.schedule_id}" if h.schedule_id is not None else None,
        job.task.display(job.header)
    ]

    return " ".join([part for part in s_parts if part])


async def send_job_not_found_response(ctx: lightbulb.Context, job: int) -> None:
    jq = saru.get(ctx).jobqueue

    if job in jq.jobs:
        await ctx.respond(f"Job {job} exists, but is not visible to filter \"{ctx.options.filter}\".")
    else:
        await ctx.respond(f"No such job: {job}.")


@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.command(
    "job",
    "Arbitrary operations on jobs. Bot owner only."
)
@lightbulb.implements(lightbulb.PrefixSubGroup)
async def job(ctx: lightbulb.Context) -> None:
    await ctx.respond(
        "`sr job` must be run with a subcommand.\n"
        f"Run `{ctx.prefix}help sr job` for help."
    )


def as_job_cmd(
    name: str,
    desc: str,
    aliases: t.Sequence[str] = (),
    no_id_opt: bool = False,
    id_opt_type: t.Type = int
) -> t.Callable[
    [t.Callable[[lightbulb.Context, t.Mapping[int, saru.Job]], t.Awaitable[None]]],
    lightbulb.CommandLike
]:
    """
    Shorthand second-order decorator for a job command.
    """
    def deco(f: t.Callable[[lightbulb.Context, t.Mapping[int, saru.Job]], t.Awaitable[None]]) -> lightbulb.CommandLike:
        # To allow the use of conditionals @ decorator syntax is not used.
        decos = [
            lightbulb.implements(lightbulb.PrefixSubCommand),
            lightbulb.command(name, desc, aliases=aliases),
            *([] if no_id_opt else [lightbulb.option(
                "id",
                f"The ID of the job to {name}.",
                type=id_opt_type
            )]),
            lightbulb.option(
                "filter",
                "An optional filter to pick which jobs a command may modify. Defaults to \"guild\"",
                type=str,
                default="guild"
            )
        ]

        # Create body.
        async def jobcmd_body(ctx: lightbulb.Context) -> None:
            filter_spec = t.cast(str, ctx.options.filter).lower()

            if "=" in filter_spec:
                filter, arg = filter_spec.split("=", maxsplit=1)
            else:
                filter, arg = filter_spec, None

            if filter not in JOBFILTERS:
                await ctx.respond(f"No such filter \"{filter}\".")
                return

            jobs = JOBFILTERS[filter](ctx, arg)
            await f(ctx, jobs)

        # Apply decorators, creating command.
        tmp: t.Any = jobcmd_body
        for d in decos:
            tmp = d(tmp)

        return t.cast(lightbulb.CommandLike, tmp)

    return deco


@job.child()
@as_job_cmd(
    "list",
    "List enqueued jobs.",
    aliases=["ls"],
    no_id_opt=True
)
async def job_list(ctx: lightbulb.Context, jobs: t.Mapping[int, saru.Job]) -> None:
    joblines = [pretty_print_job(ctx.bot.cache, j) for j in jobs.values()]

    if joblines:
        await ctx.respond(saru.codelns(joblines))
    else:
        jq = saru.get(ctx).jobqueue
        if jq.jobs:
            await ctx.respond("No jobs visible to filter \"{ctx.options.filter}\".")
        else:
            await ctx.respond("No jobs enqueued.")


@job.child()
@as_job_cmd(
    "show",
    "Show a specific job.",
    aliases=["cat"]
)
async def job_show(ctx: lightbulb.Context, jobs: t.Mapping[int, saru.Job]) -> None:
    id: int = ctx.options.id

    if id in jobs:
        # Respond with the job's header structure converted to JSON.
        await ctx.respond(saru.codejson(jobs[id].header.as_dict()))
    else:
        await send_job_not_found_response(ctx, id)


@job.child()
@as_job_cmd(
    "cancel",
    "Cancel a specific job.",
    aliases=["stop"],
    id_opt_type=str  # Need str here to support "all".
)
async def job_cancel(ctx: lightbulb.Context, jobs: t.Mapping[int, saru.Job]) -> None:
    id_str: str = ctx.options.id
    jq = saru.get(ctx).jobqueue

    # Figure out what jobs need canceling.
    if id_str.lower() == "all":
        # Cancel jobs from highest ID first. Otherwise,
        # the currently running job would be canceled repeatedly,
        # causing rapid starting and cancelling of jobs in the queue.
        jobs_to_cancel = sorted(jobs.keys(), reverse=True)
        if not jobs_to_cancel:
            await ctx.respond(f"\"all\" id: No jobs to cancel under filter \"{ctx.options.filter}\"")
            return
    else:
        try:
            id = int(id_str)
        except ValueError:
            await ctx.respond(f"Incorrect ID format. Must be either \"all\" or an integer greater than or equal to 0.")
            return

        if id in jobs:
            jobs_to_cancel = [id]
        else:
            await send_job_not_found_response(ctx, id)
            return

    for job_id in jobs_to_cancel:
        await jq.canceljob(job_id)

    await saru.ack(ctx)


@job.child()
@lightbulb.add_checks(lightbulb.owner_only)
@lightbulb.option(
    "seconds",
    "Number of seconds to block.",
    default=None,
    type=int
)
@lightbulb.command(
    "block",
    "Enqueue a blocker task."
)
@lightbulb.implements(lightbulb.PrefixSubCommand)
async def job_block(ctx: lightbulb.Context) -> None:
    seconds: t.Optional[int] = ctx.options.seconds
    s = saru.get(ctx)
    j = await s.start_job(ctx, saru.BlockerTask, {
        "time": seconds
    })

    await ctx.respond(f"Blocker started. ID: {j.header.id}")


def attach_subcommand(parent: lightbulb.CommandLike) -> None:
    parent.child(job)


def load(bot: lightbulb.BotApp) -> None:
    s = saru.get(bot)

    # Register blocker task for `job block`
    if saru.BlockerTask not in s.task_registry:
        s.task_registry.register(saru.BlockerTask)
