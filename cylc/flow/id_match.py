# THIS FILE IS PART OF THE CYLC WORKFLOW ENGINE.
# Copyright (C) NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from fnmatch import fnmatchcase
import fnmatch
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    TYPE_CHECKING,
    Set,
    Tuple,
    # Tuple,
    # Union,
    # overload,
)


from cylc.flow.parsec.util import un_many
from cylc.flow.taskdef import TaskDef
from metomi.isodatetime.exceptions import ISO8601SyntaxError

from cylc.flow import LOG
from cylc.flow.id import IDTokens, Tokens
from cylc.flow.id_cli import contains_fnmatch
from cylc.flow.cycling.loader import get_point

if TYPE_CHECKING:
    # from typing_extensions import Literal

    from cylc.flow.config import WorkflowConfig
    from cylc.flow.task_pool import Pool
    from cylc.flow.task_proxy import TaskProxy
    from cylc.flow.cycling import PointBase


# @overload
# def filter_ids(
#     pool: 'Pool',
#     ids: 'Iterable[str]',
#     *,
#     warn: 'bool' = True,
#     out: 'Literal[IDTokens.Task]' = IDTokens.Task,
#     pattern_match: 'bool' = True,
# ) -> 'Tuple[List[TaskProxy], List[str]]':
#     ...
#
#
# @overload
# def filter_ids(
#     pool: 'Pool',
#     ids: 'Iterable[str]',
#     *,
#     warn: 'bool' = True,
#     out: 'Literal[IDTokens.Cycle]' = IDTokens.Cycle,
#     pattern_match: 'bool' = True,
# ) -> 'Tuple[List[PointBase], List[str]]':
#     ...


# _RET = (
#     'Union['
#     'Tuple[List[TaskProxy], List[str]]'
#     ', '
#     'Tuple[List[PointBase], List[str]]'
#     ']'
# )


def filter_ids(
    pool: 'Pool',
    ids: 'Iterable[str]',
    *,
    warn: 'bool' = True,
    out: 'IDTokens' = IDTokens.Task,
    pattern_match: 'bool' = True,
    # ) -> _RET:
):
    """Filter IDs against a pool of tasks.

    Args:
        pool:
            The pool to match against.
        ids:
            List of IDs to match against the pool.
        out:
            The type of object to match:

            * If IDTokens.Task all matching TaskProxies will be returned.
            * If IDTokens.Cycle all CyclePoints with any matching tasks will
              be returned.
        warn:
            Whether to log a warning if no matching tasks are found in the
            pool.

    TODO:
        Consider using wcmatch which would add support for
        extglobs, namely brace syntax e.g. {foo,bar}.

    """
    if out not in {IDTokens.Cycle, IDTokens.Task}:
        raise ValueError(f'Invalid output format: {out}')

    _cycles: 'List[PointBase]' = []
    _tasks: 'List[TaskProxy]' = []
    _not_matched: 'List[str]' = []

    # enable / disable pattern matching
    match: Callable[[Any, Any], bool]
    if pattern_match:
        match = fnmatchcase
    else:
        match = str.__eq__
        pattern_ids = [
            id_
            for id_ in ids
            if contains_fnmatch(id_)
        ]
        if pattern_ids:
            LOG.warning(f'IDs cannot contain globs: {", ".join(pattern_ids)}')
            ids = [
                id_
                for id_ in ids
                if id_ not in pattern_ids
            ]
            _not_matched.extend(pattern_ids)

    id_tokens_map: Dict[str, Tokens] = {}
    for id_ in ids:
        try:
            id_tokens_map[id_] = Tokens(id_, relative=True)
        except ValueError:
            _not_matched.append(id_)
            LOG.warning(f'Invalid ID: {id_}')

    for id_, tokens in id_tokens_map.items():
        for lowest_token in reversed(IDTokens):
            if tokens.get(lowest_token.value):
                break

        cycles = set()
        tasks = []

        # filter by cycle
        if lowest_token == IDTokens.Cycle:
            cycle = tokens[IDTokens.Cycle.value]
            cycle_sel = tokens.get(IDTokens.Cycle.value + '_sel') or '*'
            for icycle, itasks in pool.items():
                if not itasks:
                    continue
                if not point_match(icycle, cycle, pattern_match):
                    continue
                if cycle_sel == '*':
                    cycles.add(icycle)
                    continue
                for itask in itasks.values():
                    if match(itask.state.status, cycle_sel):
                        cycles.add(icycle)
                        break

        # filter by task
        elif lowest_token == IDTokens.Task:   # noqa SIM106
            cycle = tokens[IDTokens.Cycle.value]
            cycle_sel_raw = tokens.get(IDTokens.Cycle.value + '_sel')
            cycle_sel = cycle_sel_raw or '*'
            task = tokens[IDTokens.Task.value]
            task_sel_raw = tokens.get(IDTokens.Task.value + '_sel')
            task_sel = task_sel_raw or '*'
            for icycle, itasks in pool.items():
                if not point_match(icycle, cycle, pattern_match):
                    continue
                for itask in itasks.values():
                    if (
                        # check cycle selector
                        (
                            (
                                # disable cycle_sel if not defined if
                                # pattern matching is turned off
                                pattern_match is False
                                and cycle_sel_raw is None
                            )
                            or match(itask.state.status, cycle_sel)
                        )
                        # check namespace name
                        and itask.name_match(task, match_func=match)
                        # check task selector
                        and (
                            (
                                # disable task_sel if not defined if
                                # pattern matching is turned off
                                pattern_match is False
                                and task_sel_raw is None
                            )
                            or match(itask.state.status, task_sel)
                        )
                    ):
                        tasks.append(itask)

        else:
            raise NotImplementedError

        if not (cycles or tasks):
            _not_matched.append(id_)
            if warn:
                LOG.warning(f"No active tasks matching: {id_}")
        else:
            _cycles.extend(list(cycles))
            _tasks.extend(tasks)

    ret: List[Any] = []
    if out == IDTokens.Cycle:
        _cycles.extend({
            itask.point
            for itask in _tasks
        })
        ret = _cycles
    elif out == IDTokens.Task:
        for icycle in _cycles:
            if icycle in pool:
                _tasks.extend(pool[icycle].values())
        ret = _tasks
    return ret, _not_matched


def point_match(
    point: 'PointBase', value: str, pattern_match: bool = True
) -> bool:
    """Return whether a cycle point matches a string/pattern.

    Args:
        point: Cycle point to compare against.
        value: String/pattern to test.
        pattern_match: Whether to allow glob patterns in the value.
    """
    try:
        return point == get_point(value)
    except (ValueError, ISO8601SyntaxError):
        # Could be glob pattern
        if pattern_match:
            return fnmatchcase(str(point), value)
        return False


def id_match(
    config: 'WorkflowConfig',
    pool: 'Pool',
    ids: Set[Tokens],
):
    unmatched: Set[Tokens] = set()

    pattern_ids = {
        id_
        for id_ in ids
        if contains_fnmatch(id_.relative_id_with_selectors)
    }
    plain_ids = ids - pattern_ids

    pattern_ids, _unmatched = _expand_globs(config, pool, pattern_ids)
    print(f'expand_globs: {pattern_ids}')
    unmatched.update(_unmatched)

    plain_ids, _unmatched = _expand_plain_ids(config, plain_ids)
    unmatched.update(_unmatched)

    return {*pattern_ids, *plain_ids}, unmatched


def _expand_globs(
    config: 'WorkflowConfig',
    pool: 'Pool',
    ids: Set[Tokens],
) -> Tuple[Set[Tokens], Set[Tokens]]:
    all_active_tasks = {
        itask.tokens.task
        for itasks in pool.values()
        for itask in itasks.values()
    }
    all_cycles = {str(icycle) for icycle in pool}
    all_namespaces = config.get_namespace_list('all namespaces')
    unmatched: Set[Tokens] = set()

    ret: Set[Tokens] = set()
    for id_ in ids:
        _cycles = _fnmatchcase_glob(id_['cycle'], all_cycles)

        _namespace = id_.get('task', '*') or 'root'
        _tasks = {
            task
            for namespace in _fnmatchcase_glob(_namespace, all_namespaces)
            for task in config.runtime['descendants'].get(namespace, [])
            if task not in config.runtime['descendants']
        }

        if not _cycles or not _tasks:
            unmatched.add(id_)
        else:
            matched = {
                id_.duplicate(cycle=_cycle, task=_task)
                # for _cycle, _task in zip(_cycles, _tasks)
                for _cycle in _cycles
                for _task in _tasks
            }.intersection(all_active_tasks)
            if matched:
                ret = ret.union(matched)
                print(f'# hit {id_} => {ret}')
            else:
                unmatched.add(id_)
                print(f'# miss {id_}')
            
    return ret, unmatched


def _expand_plain_ids(
    config: 'WorkflowConfig',
    ids: Set[Tokens],
) -> Tuple[Set[Tokens], Set[Tokens]]:
    all_namespaces = config.get_namespace_list('all namespaces')
    unmatched: Set[Tokens] = set()

    ret: Set[Tokens] = set()
    for id_ in ids:
        _namespace = id_.get('task', '*') or 'root'
        _tasks = {
            task
            for namespace in _fnmatchcase_glob(_namespace, all_namespaces)
            for task in config.runtime['descendants'].get(namespace, [])
            if task not in config.runtime['descendants']
        }
        # if 'B' in _tasks:
        #     breakpoint()

        if not _tasks:
            unmatched.add(id_)
        else:
            ret.update({
                id_.duplicate(task=_task)
                for _task in _tasks
            })
            
    return ret, unmatched


def _fnmatchcase_glob(pattern, values):
    return {
        value
        for value in values
        if fnmatchcase(value, pattern)
    }


# DAMMIT: only task SELECTORS are n=0 bound, task globs should expand
