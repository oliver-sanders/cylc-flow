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

from copy import deepcopy
from fnmatch import fnmatchcase
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Set,
    Tuple,
    # Tuple,
    # Union,
    # overload,
)

from metomi.isodatetime.exceptions import ISO8601SyntaxError

from cylc.flow import LOG
from cylc.flow.cycling.loader import get_point
from cylc.flow.id import IDTokens, Tokens
from cylc.flow.id_cli import contains_fnmatch


if TYPE_CHECKING:
    # from typing_extensions import Literal

    from cylc.flow.config import WorkflowConfig
    from cylc.flow.cycling import PointBase
    from cylc.flow.task_pool import Pool
    from cylc.flow.task_proxy import TaskProxy


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
) -> Tuple[Set[Tokens], Set[Tokens]]:
    """New Cylc 8.6.0 task matching interface.

    Args:
        config: The workflow config.
        pool: The task pool (used to determine active cycles/tasks).
        ids: The provided IDs to match.

    Returns:
        (matched, unmatched)

    """
    unmatched: Set[Tokens] = set()

    # separate IDs targeting active tasks ONLY from the remainder
    active_only_ids = {
        id_
        for id_ in ids
        if id_.get('task_sel') or id_.get('cycle_sel')
    }
    plain_ids = ids - active_only_ids

    # match active-only IDs
    active_only_ids, _unmatched = _match(
        config, pool, active_only_ids, only_match_active=True
    )
    unmatched.update(_unmatched)

    # match IDs
    plain_ids, _unmatched = _match(config, pool, plain_ids)
    unmatched.update(_unmatched)

    return {*active_only_ids, *plain_ids}, unmatched


def _match(
    config: 'WorkflowConfig',
    pool: 'Pool',
    ids: Set[Tokens],
    only_match_active: bool = False,
) -> Tuple[Set[Tokens], Set[Tokens]]:
    # mapping of family name to all contained tasks
    family_lookup: Dict[str, Set[str]] = _get_family_lookup(config)

    # set of all active task IDs (including their statuses as task selectors)
    all_active_tasks: Set[Tokens] = {
        Tokens(
            cycle=itask.tokens['cycle'],
            task=itask.tokens['task'],
            task_sel=itask.state.status,
        )
        for itasks in pool.values()
        for itask in itasks.values()
    }

    # set of all active cycles
    all_cycles: Set[str] = {str(icycle) for icycle in pool}

    # set of all possible namespaces (tasks + families)
    all_namespaces: Dict[str, Any] = config.get_namespace_list('all namespaces')

    # results
    unmatched: Set[Tokens] = set()
    matched: Set[Tokens] = set()

    for id_ in ids:
        # match cycles
        if contains_fnmatch(id_['cycle']):
            _cycles = _fnmatchcase_glob(id_['cycle'], all_cycles)
        else:
            _cycles = {id_['cycle']}

        # match tasks
        _namespace = id_.get('task', '*') or 'root'
        _tasks = {
            task
            for namespace in _fnmatchcase_glob(_namespace, all_namespaces)
            for task in family_lookup.get(namespace, {namespace})
        }

        # expand matched IDs
        _matched = {
            Tokens(
                cycle=_cycle,
                task=_task,
                task_sel=id_.get('task_sel') or id_.get('cycle_sel'),
            )
            for _cycle in _cycles
            for _task in _tasks
        }

        if only_match_active:
            # filter against active tasks
            _matched = _matched.intersection(all_active_tasks)
        else:
            # filter for on-sequence task instances
            for id__ in list(_matched):
                try:
                    taskdef = config.taskdefs[id__['task']]
                    if not taskdef.is_valid_point(get_point(id__['cycle'])):
                        _matched.remove(id__)
                except (ValueError, KeyError):
                    _matched.remove(id__)

        if _matched:
            matched = matched.union(_matched)
        else:
            unmatched.add(id_)

    return matched, unmatched


def _fnmatchcase_glob(pattern: str, values: Iterable[str]) -> Set[str]:
    """Convenience function for globing over a list of values.

    This uses the "fnmatchcase" function which is shell glob like.

    Args:
        Pattern: The glob.
        Values: The things to evaluate the glob over.

    Examples:
        >>> sorted(_fnmatchcase_glob('*', {'a', 'b', 'c'}))
        ['a', 'b', 'c']

        >>> sorted(_fnmatchcase_glob('a*', {'a1', 'a2', 'b1'}))
        ['a1', 'a2']

    """
    return {
        value
        for value in values
        if fnmatchcase(value, pattern)
    }


def _get_family_lookup(config: 'WorkflowConfig') -> Dict[str, Set[str]]:
    """Return a dict mapping families to all contained tasks.

    This recursively expands families avoiding the need to do so later.
    """
    lookup = deepcopy(config.runtime['descendants'])

    def _iter():
        ret = False
        for family, namespaces in lookup.items():
            for namespace in list(namespaces):
                if namespace in config.runtime['descendants']:
                    ret = True
                    namespaces.remove(namespace)
                    namespaces.update(config.runtime['descendants'][namespace])
        return ret

    while _iter():
        pass

    return lookup
