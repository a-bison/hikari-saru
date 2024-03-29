#
# Core implementation of the job system. Includes the main JobQueue and
# JobCron tasks, as well as related tools.
#

import asyncio
import calendar
import collections
import copy
import dataclasses
import functools
import logging
import time
import typing
from abc import ABC, abstractmethod
from collections.abc import Coroutine, MutableMapping
from datetime import MAXYEAR, MINYEAR, datetime
from typing import (Any, Callable, Mapping, Optional, Protocol, Sequence, Type,
                    Union)

logger = logging.getLogger(__name__)


#########
# UTILS #
#########

class CountingIdGenerator:
    def __init__(self, start_count: int = 0):
        self.count = start_count

    def next_id(self) -> int:
        n = self.count
        self.count += 1

        return n


############
# JOB BASE #
############


# Metadata for tracking a job, for scheduling and persistence purposes.
class JobHeader:
    @classmethod
    def from_dict(cls, id: int, d: Mapping) -> 'JobHeader':
        return JobHeader(
            id,  # ignore loaded id
            d["task_type"],
            d["properties"],
            d["owner_id"],
            d["guild_id"],
            d["start_time"],
            d["schedule_id"]
        )

    def __init__(
        self,
        id: int,
        task_type: str,
        properties: Mapping,
        owner_id: int,
        guild_id: int,
        start_time: int,
        schedule_id: Optional[int] = None
    ):
        self.id = id

        # Arguments to the job.
        self.properties = properties

        # Integer ID of the schedule that spawned this job. If no schedule ID,
        # then this will be None/null.
        self.schedule_id = schedule_id

        # Member ID of the discord user that started this job.
        # If this job was started by a schedule, this will reflect the owner of
        # the schedule.
        self.owner_id = owner_id

        # Guild ID of the guild this job was started in
        self.guild_id = guild_id

        # The date the job was started, as a unix timestamp, in UTC time.
        self.start_time = start_time

        # The task type string.
        self.task_type = task_type

        # RUNTIME ATTRIBUTES

        # Is the job cancelled?
        self.cancel = False

        # The results of the job, if any.
        self.results: MutableMapping[str, Any] = {}

    def as_dict(self) -> MutableMapping[str, Any]:
        return {
            "id": self.id,
            "properties": self.properties,
            "schedule_id": self.schedule_id,
            "owner_id": self.owner_id,
            "guild_id": self.guild_id,
            "start_time": self.start_time,
            "task_type": self.task_type
        }


# Task base class. Subclass this to create your own Tasks.
class JobTask(ABC):
    def __init__(self, *_: typing.Any, **__: typing.Any) -> None: ...

    @abstractmethod
    async def run(self, header: JobHeader) -> None:
        raise NotImplementedError("Subclass JobTask to implement specific tasks.")

    # Optional function that returns a default dict of properties to be passed
    # into run() on job execution. If None is returned, no defaults will be
    # set. Optionally, the implementation may use the current properties to
    # select new defaults.
    @classmethod
    def property_default(cls, properties: Mapping) -> Optional[Mapping]:
        return None

    # Class method that returns the string name of this task type.
    @classmethod
    @abstractmethod
    def task_type(cls) -> str:
        return "NONE"

    # Pretty print info about this task. This should only return information
    # included in the header.properties dictionary. Higher level information
    # is the responsibility of the caller.
    def display(self, header: JobHeader) -> str:
        msg = "display() not implemented for task {}"
        logger.warning(msg.format(header.task_type))

        return ""


# Container class for job information. Primary data class for this module.
class Job:
    def __init__(self, header: JobHeader, task: JobTask):
        self.header = header
        self.task = task
        self.complete_event = asyncio.Event()

    # Marks this job as complete, notifies all coroutines waiting on it.
    def mark_complete(self) -> None:
        self.complete_event.set()

    # Wait for this job to finish.
    async def wait(self, timeout: Optional[float] = None) -> Mapping:
        if timeout is None:
            await self.complete_event.wait()
        else:
            await asyncio.wait_for(self.complete_event.wait(), timeout)

        return self.header.results

    @staticmethod
    def force_id(job: Union['Job', int]) -> int:
        if isinstance(job, Job):
            return job.header.id
        else:
            return job


# Simple registry for getting task types.
class TaskRegistry:
    def __init__(self) -> None:
        self.tasks: MutableMapping[str, Type[JobTask]] = {}

    def register(self, cls: Type[JobTask]) -> None:
        if not hasattr(cls, "task_type"):
            raise TypeError("Task class must have task_type classmethod")

        self.tasks[cls.task_type()] = cls

    def unregister(self, cls: Type[JobTask]) -> None:
        if not hasattr(cls, "task_type"):
            raise TypeError("Task class must have task_type classmethod")

        tt = cls.task_type()

        if tt not in self.tasks:
            logger.warning(f"Task {tt} not in task registry. Not unregistering.")
        else:
            del self.tasks[tt]

    def get(self, task: Union[str, Type[JobTask]]) -> Type[JobTask]:
        if isinstance(task, str):
            return self.tasks[task]
        elif isinstance(task, type) and issubclass(task, JobTask):
            return task
        else:
            raise TypeError("Object {} has invalid type".format(str(task)))

    # Forces a passed tasktype object to be a string. Also serves to validate
    # a task_type string.
    def force_str(self, tasktype: Union[str, Type[JobTask]]) -> str:
        return self.get(tasktype).task_type()

    # Tests if a task type is in the task registry.
    def __contains__(self, task_type: Union[Type[JobTask], str]) -> bool:
        task_type_str = task_type if isinstance(task_type, str) else task_type.task_type()
        return task_type_str in self.tasks


# Base JobFactory functionality.
class JobFactory(ABC):
    def __init__(self, task_registry: TaskRegistry):
        self.id_counter = CountingIdGenerator()
        self.task_registry = task_registry

    # Get the next available job ID.
    def next_id(self) -> int:
        return self.id_counter.next_id()

    # Create a jobheader from a schedule entry
    async def create_jobheader_from_cron(self, cron: 'CronHeader') -> JobHeader:
        return cron.as_jobheader(
            self.next_id(),
            int(time.time())
        )

    # Create a jobheader from an existing dictionary
    async def create_jobheader_from_dict(self, header: Mapping) -> JobHeader:
        return JobHeader.from_dict(
            self.next_id(),
            header
        )

    # Create a job using just the header.
    async def create_job_from_jobheader(self, header: JobHeader) -> Job:
        task = await self.create_task(header)
        j = Job(header, task)

        # Update header.properties with declared task defaults, if any.
        defaults = task.property_default(header.properties)
        if defaults:
            new_props = dict(defaults)
            new_props.update(header.properties)
            header.properties = new_props

        return j

    # Create a job from a schedule entry
    async def create_job_from_cron(self, cron: 'CronHeader') -> Job:
        header = await self.create_jobheader_from_cron(cron)
        return await self.create_job_from_jobheader(header)

    # Create a job from an existing dictionary (typically loaded from cfg)
    async def create_job_from_dict(self, header: Mapping) -> Job:
        jobheader = await self.create_jobheader_from_dict(header)
        return await self.create_job_from_jobheader(jobheader)

    # Create a new task using a jobheader.
    @abstractmethod
    async def create_task(self, header: JobHeader) -> JobTask:
        pass


class JobCallback(Protocol):
    async def __call__(self, header: JobHeader) -> None: ...


# A single job queue. Can run one job at a time.
class JobQueue:
    def __init__(self, eventloop: Optional[asyncio.AbstractEventLoop] = None):
        if eventloop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = eventloop

        self.active_job: Optional[Job] = None
        self.active_task: Optional[asyncio.Task] = None
        self.job_queue: asyncio.Queue = asyncio.Queue()

        # Job dict used for display purposes, because asyncio.Queue doesn't
        # support peeking
        self.jobs: MutableMapping[int, Job] = collections.OrderedDict()

        self.job_submit_callback: Optional[JobCallback] = None
        self.job_start_callback: Optional[JobCallback] = None
        self.job_stop_callback: Optional[JobCallback] = None
        self.job_cancel_callback: Optional[JobCallback] = None

    async def submit_job(self, job: Job) -> None:
        if self.job_submit_callback is not None:
            await self.job_submit_callback(job.header)

        await self.job_queue.put(job)
        self.jobs[job.header.id] = job

    def on_job_submit(self, callback: JobCallback) -> None:
        self.job_submit_callback = callback

    def on_job_start(self, callback: JobCallback) -> None:
        self.job_start_callback = callback

    def on_job_stop(self, callback: JobCallback) -> None:
        self.job_stop_callback = callback

    def on_job_cancel(self, callback: JobCallback) -> None:
        self.job_cancel_callback = callback

    def _rm_job(self, job: Optional[Job]) -> None:
        if job is None:
            return

        if job.header.id in self.jobs:
            del self.jobs[job.header.id]

        self.active_job = None
        self.active_task = None

    async def run(self) -> None:
        logger.info("Starting job queue...")

        try:
            while True:
                await self.mainloop()
        except Exception:
            logger.exception("Job queue stopped unexpectedly!")
        finally:
            logger.info("Job queue stoppped.")

    async def mainloop(self) -> None:
        j: Job = await self.job_queue.get()

        if j.header.cancel:
            logger.info("Skipping cancelled job " + str(j.header.id))
            self._rm_job(self.active_job)
            return

        logger.info("Start new job " + str(j.header.as_dict()))
        self.active_job = j

        # Schedule task
        coro = self.active_job.task.run(self.active_job.header)
        task = self.loop.create_task(coro)
        self.active_task = task

        if self.job_start_callback:
            await self.job_start_callback(j.header)

        try:
            await task
        except asyncio.CancelledError:
            logger.warning("Uncaught CancelledError in job " + str(j.header.id))
        except:
            logger.exception("Got exception while running job")
        finally:
            if self.active_job:
                # Notify any listeners on this job that it's done.
                self.active_job.mark_complete()

            self._rm_job(self.active_job)

        if self.job_stop_callback:
            await self.job_stop_callback(j.header)

    def is_job_running(self, job: Union[Job, int]) -> bool:
        """Query if a job is actively running."""

        jobid = Job.force_id(job)

        if not self.active_job or not self.active_task:
            assert not (self.active_job or self.active_task)
            return False

        if self.active_job.header.id == jobid:
            return True

        return False

    async def canceljob(self, job: Union[Job, int]) -> None:
        jobid = Job.force_id(job)

        self.jobs[jobid].header.cancel = True

        if self.is_job_running(jobid):
            assert self.active_task is not None
            self.active_task.cancel()

        if self.job_cancel_callback:
            await self.job_cancel_callback(self.jobs[jobid].header)

        del self.jobs[jobid]


##################
# JOB SCHEDULING #
##################

SCHED_PARSE_POSITIONS = [
    "minute",
    "hour",
    "dayofmonth",
    "month",
    "dayofweek"
]
SCHED_PARSE_LIMITS = [
    (0, 59),
    (0, 23),
    (1, 31),
    (1, 12),
    (0, 6)
]
SCHED_LIMITS = {k: v for k, v in zip(SCHED_PARSE_POSITIONS, SCHED_PARSE_LIMITS)}
SCHED_WD_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6
}
SCHED_MACROS = {
    "!weekly": "* * SUN",
    "!monthly": "1 * *",
    "!yearly": "1 1 *",
    "!daily": "* * *"
}


# Main scheduling data class.
class CronHeader:
    @classmethod
    def from_dict(cls, d: Mapping) -> 'CronHeader':
        return CronHeader(**d)

    def __init__(
        self,
        id: int,
        task_type: str,
        properties: Mapping,
        owner_id: int,
        guild_id: int,
        schedule: str
    ):
        # ID of this schedule. NOTE: Unlike Jobs, whose ID count resets after
        # every startup, schedules always have the same IDs.
        self.id = id

        # Arguments to the jobs created by this schedule.
        self.properties = properties

        # The task type string.
        self.task_type = task_type

        # Member ID of the discord user that owns this schedule.
        self.owner_id = owner_id

        # Guild ID of the guild this schedule was created in.
        self.guild_id = guild_id

        # Schedule string for this schedule. Determines the time that this
        # job will run at. The header stores the schedule string in the exact
        # same format as the unix cron utility.
        #
        # min hour day_of_month month day_of_week
        # note: day_of_week runs from 0-6, Sunday-Saturday.
        #
        # supported operators:
        # * - Signifies all possible values in a field.
        #
        # example:
        # 1 4 * * 0 - run the job at 4:01 am every Sunday.
        self.schedule = schedule

        # RUNTIME VALUES
        # These values are generated at runtime and are never saved.

        # A python datetime object representing the next time this schedule
        # will run. Used by a schedule dispatcher to avoid missing a job fire.
        self.next: Optional[datetime] = None

    def as_dict(self) -> MutableMapping[str, Any]:
        return {
            "id": self.id,
            "properties": self.properties,
            "task_type": self.task_type,
            "owner_id": self.owner_id,
            "guild_id": self.guild_id,
            "schedule": self.schedule
        }

    def as_jobheader(self, id: int, start_time: int) -> JobHeader:
        return JobHeader(
            id,
            self.task_type,
            self.properties,
            self.owner_id,
            self.guild_id,
            start_time,
            self.id
        )

    # Updates self.next to the next run after current datetime. Used by the
    # schedule dispatcher.
    def update_next(self) -> None:
        sched_obj = cron_parse(self.schedule)

        # Make sure to avoid multiple schedule firings, so make carry=1.
        # See cron_next_date() for more details.
        self.next = cron_next_date_as_datetime(sched_obj, carry=1)

    def match(self, **kwargs: Union[str, int, Mapping]) -> bool:
        d = self.as_dict()

        for key, value in kwargs.items():
            if key not in d:
                raise TypeError("Cannot use {} in CronHeader.match".format(
                    key
                ))

            if d[key] != value:
                return False

        return True


class ScheduleParseException(Exception):
    def __init__(self, *args: Any, cronstr: Optional[str] = None):
        super().__init__(*args)

        self.cronstr = cronstr


CronT = MutableMapping[str, Optional[int]]


@dataclasses.dataclass
class _SchedIntermediate:
    minute: Optional[int]
    hour: Optional[int]
    dayofmonth: list[int]
    month: Optional[int]
    dayofweek: Optional[int]
    original_dayofmonth: list[int] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.original_dayofmonth = list(self.dayofmonth)


# TODO: Fix typing here, needs to be more specific.
# Parse a schedule string into a dictionary.
@functools.cache
def cron_parse(schedule_str: str) -> CronT:
    schedule_str = schedule_str.lower()

    # Parse macros first
    for macro, repl in SCHED_MACROS.items():
        schedule_str = schedule_str.replace(macro, repl)

    s_split = schedule_str.lower().split()
    s_dict: CronT = {}

    if len(s_split) < 5:
        raise ScheduleParseException("less than 5 elements", cronstr=schedule_str)
    elif len(s_split) > 5:
        raise ScheduleParseException("more than 5 elements", cronstr=schedule_str)

    for limit, name, elem, i in zip(SCHED_PARSE_LIMITS, SCHED_PARSE_POSITIONS, s_split, range(5)):
        lower, upper = limit

        if elem == "*":
            s_dict[name] = None
            continue

        try:
            result = int(elem)
        except ValueError:
            if name == "dayofweek" and elem.lower() in SCHED_WD_NAMES:
                result = SCHED_WD_NAMES[elem.lower()]
            else:
                msg = "position {}({}): {} is not an integer"
                raise ScheduleParseException(
                    msg.format(i, name, elem),
                    cronstr=schedule_str
                )

        # Verify item range
        if result < lower or result > upper:
            msg = "position {}({}): {} outside bounds {}>{}>{}"
            raise ScheduleParseException(
                msg.format(i, name, elem, lower, elem, upper),
                cronstr=schedule_str
            )

        s_dict[name] = result

    return s_dict


# Conversion functions for weekday formats. The python datetime library
# starts counting weekdays at Mon=0, but cron strings start at Sun=0.
def wd_cron_to_python(wd: int) -> int:
    return (wd + 6) % 7


def wd_python_to_cron(wd: int) -> int:
    return (wd + 1) % 7


# Test whether a schedule should run, based on a timedate object.
def cron_match(schedule_str: str, timedate_obj: datetime) -> bool:
    sd = cron_parse(schedule_str)

    for name, elem in sd.items():
        # Skip *'s
        if elem is None:
            continue

        # If we encounter a field that doesn't match, stop and return False.
        # Therefore, if all specified fields match, we will return True.
        if ((name == "minute"     and elem != timedate_obj.minute) or
            (name == "hour"       and elem != timedate_obj.hour) or
            (name == "dayofmonth" and elem != timedate_obj.day) or
            (name == "month"      and elem != timedate_obj.month) or
            (name == "dayofweek"  and elem != wd_python_to_cron(timedate_obj.weekday()))):
            return False

    return True


# From a cron structure parsed from cron_parse, determine what the next
# date will be the scheduled job will run, based on current date. Returns a dict:
#
# {
#   "minute": ...,
#   "hour":   ...,
#   "day":    ...,
#   "month":  ...,
#   "year":   ...
# }
#
# If carry is supplied, one minute will be added to the from_date. This can
# help avoid multiple schedule firings if the from_date already matches the
# schedule.
def cron_next_date(
    schedule: CronT,
    from_date: Optional[datetime] = None,
    carry: int = 0
) -> Mapping[str, int]:
    # Convert to object form so we can freely modify.
    sched_intermediate: _SchedIntermediate = _SchedIntermediate(
        minute=schedule["minute"],
        hour=schedule["hour"],
        dayofweek=schedule["dayofweek"],
        month=schedule["month"],
        dayofmonth=([] if schedule["dayofmonth"] is None else [schedule["dayofmonth"]])
    )

    if from_date is not None:
        current_date = from_date
    else:
        current_date = datetime.now()

    next_date = {
        "minute": current_date.minute,
        "hour": current_date.hour,
        "dayofmonth": current_date.day,
        "month": current_date.month,
        "year": current_date.year
    }

    next_date["minute"], carry = _next_elem("minute", next_date["minute"],
                                            carry, next_date, sched_intermediate)
    next_date["hour"], carry = _next_elem("hour", next_date["hour"],
                                          carry, next_date, sched_intermediate)

    # Day of month is tricky. If there was a carry, that means we flipped
    # to the next month, and potentially the next year. If that's the case,
    # then we need to do a second round.
    new_day, carry = _cron_next_day(
        sched_intermediate,
        carry,
        day=next_date["dayofmonth"],
        month=next_date["month"],
        year=next_date["year"]
    )

    # Only do another round if dayofweek is present.
    # TODO Evaluate leap year edge case. May need to do another round
    # regardless of dayofweek.
    if carry > 0 and sched_intermediate.dayofweek is not None:
        # We flipped month, so do another round, starting at the first day
        # of the month. la = lookahead
        month_la = next_date["month"]
        year_la = next_date["year"]

        logger.info("cron_next_date(): month overrun, recalc day")

        if month_la == 12:
            month_la = 1
            year_la += 1
            logger.info("cron_next_date(): year overrun")
        else:
            month_la += 1

        new_day, extra_carry = _cron_next_day(
            sched_intermediate, 0,
            day=1,
            month=month_la,
            year=year_la
        )

        # FIXME This will fall over in the case that the day rolls over twice,
        # which could happen if a day falls on a number beyond the new month.
        if extra_carry > 0:
            raise Exception("Could not recalculate dayofmonth")

    # FIXME Possible bug: if month rolls over after previous calculation,
    # days need to be recalculated, but this doesn't happen.
    next_date["dayofmonth"] = new_day
    next_date["month"], carry = _next_elem("month", next_date["month"],
                                           carry, next_date, sched_intermediate)

    # Don't need full elem calculation for year, so just bump it if there
    # was a carry.
    next_date["year"] += carry

    return next_date


def _cron_next_day(
    schedule: _SchedIntermediate,
    carry: int,
    day: int,
    month: int,
    year: int
) -> typing.Tuple[int, int]:
    # If dayofweek is present, fold it into dayofmonth to make things
    # easier to calculate.
    if schedule.dayofweek is not None:
        weekdays = cron_calc_days(
            year,
            month,
            schedule.dayofweek
        )

        # Join, convert to set to remove dupes, and sort.
        schedule.dayofmonth = sorted({*schedule.original_dayofmonth, *weekdays})

    newday, carry = _next_elem(
        "dayofmonth",
        day,
        carry,
        {"year": year, "month": month},
        schedule
    )

    return newday, carry


# Calculate the upper and lower bounds for a given element.
def _limit_elem(elem_name: str, t: Mapping) -> typing.Tuple[int, int]:
    if elem_name == "dayofmonth":
        upper = calendar.monthrange(t["year"], t["month"])[1]
        return 1, upper

    elif elem_name == "year":
        return MINYEAR, MAXYEAR

    else:
        return SCHED_LIMITS[elem_name]


# Calculate the next element
def _next_elem(
    elem_name: str,
    elem: int,
    carry: int,
    t: Mapping,
    schedule: _SchedIntermediate
) -> typing.Tuple[int, int]:
    sched_elem: Union[int, Sequence[int], None] = getattr(schedule, elem_name)
    lower, upper = _limit_elem(elem_name, t)

    new_elem = elem + carry
    new_carry = 0

    logger.info("next_elem(): {}: {}({}) -> {}".format(
        elem_name,
        elem,
        new_elem,
        str(sched_elem)
    ))

    # If our sched element can be anything, don't touch it. Note that
    # the carry has already been taken into account. We just need to check
    # whether the carry made this element roll over.
    if sched_elem is None:
        if new_elem > upper:
            new_elem = lower
            new_carry = 1

        return new_elem, new_carry

    # Otherwise, select the next available schedule slot for this element.
    # If no slot could be selected, select the first one, and carry.
    no_elem_found = False
    if isinstance(sched_elem, int):
        sched_elem = [sched_elem]
    sched_elem = sorted(sched_elem)
    for i in sched_elem:
        if i < new_elem:
            continue
        else:
            new_elem = i
            break
    else:
        no_elem_found = True

    # If we couldn't find the next element, or the new element that WAS
    # selected goes over the given limit, roll back around and carry.
    if new_elem > upper or no_elem_found:
        new_elem = sched_elem[0]
        new_carry = 1

    return new_elem, new_carry


# Calculate all of a given weekday in a given month. Returns
# a list of day numbers within the given month.
@functools.cache
def cron_calc_days(year: int, month: int, wd: int) -> Sequence[int]:
    # Calendar starts at 0 = Monday, goes to 6 = Sunday. Cron format is offset
    # from that, so we need to convert to python range.
    wd = wd_cron_to_python(wd)
    c = calendar.Calendar()
    return [d for d, _wd in c.itermonthdays2(year, month)
            if d != 0 and _wd == wd]


# Convert the output of cron_next_date to a datetime object.
def cron_next_to_datetime(cron_next: Mapping) -> datetime:
    return datetime(
        cron_next["year"],
        cron_next["month"],
        cron_next["dayofmonth"],
        cron_next["hour"],
        cron_next["minute"]
    )


# Get next date as datetime.
def cron_next_date_as_datetime(
    schedule: CronT,
    from_date: Optional[datetime] = None,
    carry: int = 0
) -> datetime:
    return cron_next_to_datetime(cron_next_date(schedule, from_date, carry))


ScheduleCallback = Callable[[CronHeader], Coroutine[Any, Any, None]]


# A scheduler that starts jobs at specific real-world dates.
# Expected to have minute-level accuracy.
class JobCron:
    def __init__(self, jobqueue: JobQueue, jobfactory: JobFactory):
        self.jobqueue = jobqueue
        self.jobfactory = jobfactory

        self.schedule_lock = asyncio.Lock()
        self.schedule: MutableMapping[int, CronHeader] = {}

        self.sched_create_callback: Optional[ScheduleCallback] = None
        self.sched_delete_callback: Optional[ScheduleCallback] = None

    def on_create_schedule(self, callback: ScheduleCallback) -> None:
        self.sched_create_callback = callback

    def on_delete_schedule(self, callback: ScheduleCallback) -> None:
        self.sched_delete_callback = callback

    # Stop a schedule from running.
    async def delete_schedule(self, id: int) -> None:
        async with self.schedule_lock:
            sheader = self.schedule[id]
            sheader.next = None

            if self.sched_delete_callback is not None:
                await self.sched_delete_callback(sheader)

            del self.schedule[id]

    # Replace a schedule entry with a new one.
    async def replace_schedule(self, id: int, sheader: CronHeader) -> None:
        async with self.schedule_lock:
            old_hdr = self.schedule[id]

            new_hdr = sheader
            new_hdr.next = None
            new_hdr.update_next()

            # Call both delete and create callbacks to ensure any
            # external state is updated properly
            if self.sched_delete_callback is not None:
                await self.sched_delete_callback(old_hdr)

            if self.sched_create_callback is not None:
                await self.sched_create_callback(new_hdr)

            self.schedule[id] = new_hdr

    # Schedule a job.
    async def create_schedule(self, sheader: CronHeader) -> None:
        async with self.schedule_lock:
            # Calculate the next run date right away. This also
            # functions to validate the cron str before scheduling.
            sheader.update_next()

            logger.info("New schedule created: " + str(sheader.as_dict()))
            if self.sched_create_callback is not None:
                await self.sched_create_callback(sheader)

            self.schedule[sheader.id] = sheader

    async def run(self) -> None:
        # The background task that starts jobs. Checks if there are new jobs
        # to start roughly once every minute.
        logger.info("Starting job scheduler...")

        try:
            while True:
                await asyncio.sleep(60)

                # Do not allow modifications to the schedule while a schedule
                # check is running.
                async with self.schedule_lock:
                    await self.mainloop()
        except:
            logger.exception("Scheduler stopped unexpectedly!")

    # Single iteration of schedule dispatch.
    async def mainloop(self) -> None:
        for id, sheader in self.schedule.items():
            # If we've gone past the scheduled time, fire the job,
            # regenerate the next time using the cron string.
            if sheader.next and sheader.next < datetime.now():
                sheader.update_next()
                await self._start_scheduled_job(sheader)

    async def _start_scheduled_job(self, cron_header: CronHeader) -> Job:
        job = await self.jobfactory.create_job_from_cron(cron_header)
        msg = "SCHED {}: Firing job type={} {}"
        logger.info(msg.format(
            cron_header.id, job.header.task_type, job.task.display(job.header)
        ))
        await self.jobqueue.submit_job(job)

        return job

    # Run a scheduled job immediately, returning the resulting job.
    async def run_now(self, id: int) -> Job:
        hdr = self.schedule[id]
        return await self._start_scheduled_job(hdr)

    # Returns a copy of the schedule, filtered by the given parameters.
    def sched_filter(self, **kwargs: Union[int, str, Mapping]) -> Mapping[int, CronHeader]:
        return {id: c for id, c in self.schedule.items()
                if c.match(**kwargs)}

    # Returns a copy of the schedule.
    def sched_copy(self) -> Mapping[int, CronHeader]:
        return dict(self.schedule)

    # Reschedule a schedule entry.
    async def reschedule(self, id: int, cronstr: str) -> None:
        hdr = self.schedule[id]
        
        new_hdr = copy.deepcopy(hdr)
        new_hdr.schedule = cronstr

        await self.replace_schedule(id, new_hdr)


##################
# BUILT IN TASKS #
##################

# A task that does nothing but sleep for a given time. Mostly used for
# debugging purposes. For example, you can fill the job queue with BlockerTasks
# that never end to test proper queue and cancellation behavior.
class BlockerTask(JobTask):
    # Ignore any arguments passed in to retain compatibility with all job
    # factories.
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.counter = 0

    @classmethod
    def task_type(cls) -> str:
        return "blocker"

    @classmethod
    def property_default(cls, properties: Mapping) -> Mapping:
        return {
            "time": 60  # seconds. if None, loops forever.
        }

    async def run(self, header: JobHeader) -> None:
        p = header.properties
        time = p["time"]

        if time is None:
            while True: 
                await asyncio.sleep(1)
        else:
            self.counter = time

            while self.counter > 0:
                await asyncio.sleep(1)
                self.counter -= 1

    def display(self, header: JobHeader) -> str:
        if header.properties['time'] is None:
            return "time=infinite"
        else:
            return " ".join([
                f"time={header.properties['time']}",
                f"remaining={self.counter}"
            ])

