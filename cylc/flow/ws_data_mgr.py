#!/usr/bin/env python3

# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2019 NIWA & British Crown (Met Office) & Contributors.
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
"""Manage the workflow data store.

The data store is generated here, in a Workflow Service (WS), and synced to the
User Interface Server (UIS) via protobuf messages. Used as resolving data with
GraphQL, both in the WS and UIS, it is then provisioned to the CLI and GUI.

This data store is comprised of Protobuf message objects (data elements),
which are used as data containers for their respective type.

Static data elements are generated on workflow start/restart/reload, which
includes workflow, task, and family definition objects.

Updates are triggered by changes in the task pool;
migrations of task instances from runahead to live pool, and
changes in task state (itask.state.is_updated).
- Graph edges are generated for new cycle points in the pool
- Ghost nodes, task and family cycle point instances containing static data,
  are generated from the source & target of edges and pointwise respectively.
- Cycle points are removed/pruned if they are not in the pool and not
  the source or target cycle point of the current edge set. The removed include
  edge, task, and family cycle point items and an update of the family,
  workflow, and manager aggregate attributes.

Data elements include a "stamp" field, which is a timestamped ID for use
in assessing changes in the data store, for comparisons of a store sync.

Packaging methods are included for dissemination of protobuf messages.

"""

from time import time
from copy import copy

from cylc.flow.cycling.loader import get_point
from cylc.flow.task_id import TaskID
from cylc.flow.suite_status import get_suite_status
from cylc.flow.task_state_prop import extract_group_state
from cylc.flow.wallclock import (
    TIME_ZONE_LOCAL_INFO, TIME_ZONE_UTC_INFO, get_utc_mode)
from cylc.flow.task_job_logs import JOB_LOG_OPTS
from cylc.flow import __version__ as CYLC_VERSION
from cylc.flow.ws_messages_pb2 import (
    PbFamily, PbFamilyProxy, PbTask, PbTaskProxy, PbWorkflow,
    PbEdge, PbEdges, PbEntireWorkflow)


ID_DELIM = '|'


def task_mean_elapsed_time(tdef):
    """Calculate task mean elapsed time."""
    if tdef.elapsed_times:
        return sum(tdef.elapsed_times) / len(tdef.elapsed_times)
    elif tdef.rtconfig['job']['execution time limit']:
        return tdef.rtconfig['job']['execution time limit']


class WsDataMgr(object):
    """Manage the workflow data store.

    Attributes:
        .all_states (list):
            All task proxy states in the store.
        .ancestors (dict):
            Local store of config.get_first_parent_ancestors()
        .cycle_states (dict):
            List of task states for point key.
        .descendants (dict):
            Local store of config.get_first_parent_descendants()
        .edge_points (dict):
            Source point keys of target points lists.
        .edges (dict):
            cylc.flow.ws_messages_pb2.PbEdge by internal ID.
        .families (dict):
            cylc.flow.ws_messages_pb2.PbFamily by name (internal ID).
        .family_proxies (dict):
            cylc.flow.ws_messages_pb2.PbFamilyProxy by internal ID.
        .graph (cylc.flow.ws_messages_pb2.PbEdges):
            Graph message holding egdes meta data.
        .max_point (cylc.flow.cycling.PointBase):
            Maximum cycle point in the pool.
        .min_point (cylc.flow.cycling.PointBase):
            Minimum cycle point in the pool.
        .parents (dict):
            Local store of config.get_parent_lists()
        .pool_points (set):
            Cycle point objects in the task pool.
        .schd (cylc.flow.scheduler.Scheduler):
            Workflow scheduler object.
        .state_count_cycles (dict):
            State totals (dict) per cycle point key.
        .tasks (dict):
            cylc.flow.ws_messages_pb2.PbTask by name (internal ID).
        .task_proxies (dict):
            cylc.flow.ws_messages_pb2.PbTaskProxy by internal ID.
        .workflow_id (str):
            ID of the workflow service containing owner and name.
        .workflow (cylc.flow.ws_messages_pb2.PbWorkflow)
            Message containing the global information of the workflow.

    Arguments:
        schd (cylc.flow.scheduler.Scheduler):
            Workflow scheduler instance.
    """

    # Memory optimization - constrain possible attributes to this list.
    __slots__ = [
        'all_states',
        'ancestors',
        'cycle_states',
        'descendants',
        'edge_points',
        'edges',
        'families',
        'family_proxies',
        'graph',
        'max_point',
        'min_point',
        'parents',
        'pool_points',
        'schd',
        'state_count_cycles',
        'task_proxies',
        'tasks',
        'workflow_id',
        'workflow',
    ]

    def __init__(self, schd):
        self.schd = schd
        self.ancestors = {}
        self.descendants = {}
        self.parents = {}
        self.pool_points = set()
        self.max_point = None
        self.min_point = None
        self.edge_points = {}
        self.all_states = []
        self.cycle_states = {}
        self.state_count_cycles = {}
        # Managed data types
        self.tasks = {}
        self.task_proxies = {}
        self.families = {}
        self.family_proxies = {}
        self.edges = {}
        self.graph = PbEdges()
        self.workflow_id = f'{self.schd.owner}{ID_DELIM}{self.schd.suite}'
        self.workflow = PbWorkflow()

    def generate_definition_elements(self):
        """Generate static definition data elements.

        Populates the tasks, families, and workflow elements
        with data from and/or derived from the workflow definition.

        """
        config = self.schd.config
        update_time = time()
        tasks = {}
        families = {}
        workflow = PbWorkflow(
            stamp=f'{self.workflow_id}@{update_time}',
            id=self.workflow_id,
        )

        ancestors = config.get_first_parent_ancestors()
        descendants = config.get_first_parent_descendants()
        parents = config.get_parent_lists()

        # Create task definition elements.
        for name, tdef in config.taskdefs.items():
            t_id = f'{self.workflow_id}{ID_DELIM}{name}'
            t_check = f'{t_id}@{update_time}'
            task = PbTask(
                stamp=t_check,
                id=t_id,
                name=name,
                depth=len(ancestors[name]) - 1,
            )
            task.namespace[:] = tdef.namespace_hierarchy
            for key, val in dict(tdef.describe()).items():
                if key in ['title', 'description', 'url']:
                    setattr(task.meta, key, val)
                else:
                    task.meta.user_defined.append(f'{key}={val}')
            elapsed_time = task_mean_elapsed_time(tdef)
            if elapsed_time:
                task.mean_elapsed_time = elapsed_time
            tasks[t_id] = task

        # Created family definition elements.
        for name in ancestors.keys():
            if name in config.taskdefs.keys():
                continue
            f_id = f'{self.workflow_id}{ID_DELIM}{name}'
            f_check = f'{f_id}@{update_time}'
            family = PbFamily(
                stamp=f_check,
                id=f_id,
                name=name,
                depth=len(ancestors[name]) - 1,
            )
            famcfg = config.cfg['runtime'][name]
            for key, val in famcfg.get('meta', {}).items():
                if key in ['title', 'description', 'url']:
                    setattr(family.meta, key, val)
                else:
                    family.meta.user_defined.append(f'{key}={val}')
            family.parents.extend(
                [f'{self.workflow_id}{ID_DELIM}{p_name}'
                    for p_name in parents[name]])
            families[f_id] = family

        for name, parent_list in parents.items():
            if not parent_list:
                continue
            fam = parent_list[0]
            f_id = f'{self.workflow_id}{ID_DELIM}{fam}'
            if f_id in families:
                ch_id = f'{self.workflow_id}{ID_DELIM}{name}'
                if name in config.taskdefs:
                    families[f_id].child_tasks.append(ch_id)
                else:
                    families[f_id].child_families.append(ch_id)

        # Populate static fields of workflow
        workflow.api_version = self.schd.server.API
        workflow.cylc_version = CYLC_VERSION
        workflow.name = self.schd.suite
        workflow.owner = self.schd.owner
        workflow.host = self.schd.host
        workflow.port = self.schd.port
        for key, val in config.cfg['meta'].items():
            if key in ['title', 'description', 'URL']:
                setattr(workflow.meta, key, val)
            else:
                workflow.meta.user_defined.append(f'{key}={val}')
        workflow.tree_depth = max(
            [len(val) for key, val in ancestors.items()]) - 1

        if get_utc_mode():
            time_zone_info = TIME_ZONE_UTC_INFO
        else:
            time_zone_info = TIME_ZONE_LOCAL_INFO
        for key, val in time_zone_info.items():
            setattr(workflow.time_zone_info, key, val)

        workflow.last_updated = update_time
        workflow.run_mode = config.run_mode()
        workflow.cycling_mode = config.cfg['scheduling']['cycling mode']
        workflow.workflow_log_dir = self.schd.suite_log_dir
        workflow.job_log_names.extend(list(JOB_LOG_OPTS.values()))
        workflow.ns_defn_order.extend(config.ns_defn_order)

        workflow.tasks.extend(list(tasks))
        workflow.families.extend(list(families))

        # replace the originals (atomic update, for access from other threads).
        self.ancestors = ancestors
        self.descendants = descendants
        self.parents = parents
        self.tasks = tasks
        self.families = families
        self.workflow = workflow

    def generate_ghost_task(self, task_id):
        """Create task-point element populated with static data.

        Args:
            task_id (str):
                valid TaskID string.

        Returns:

            object: cylc.flow.ws_messages_pb2.PbTaskProxy
                Populated task proxy data element.

        """
        update_time = time()

        name, point_string = TaskID.split(task_id)
        self.cycle_states.setdefault(point_string, {})[name] = None
        t_id = f'{self.workflow_id}{ID_DELIM}{name}'
        tp_id = f'{self.workflow_id}{ID_DELIM}{point_string}{ID_DELIM}{name}'
        tp_check = f'{tp_id}@{update_time}'
        taskdef = self.tasks[t_id]
        tproxy = PbTaskProxy(
            stamp=tp_check,
            id=tp_id,
            task=taskdef.id,
            cycle_point=point_string,
            depth=taskdef.depth,
            name=name,
        )
        tproxy.namespace[:] = taskdef.namespace
        tproxy.parents[:] = [
            f'{self.workflow_id}{ID_DELIM}{point_string}{ID_DELIM}{p_name}'
            for p_name in self.parents[name]]
        p1_name = self.parents[name][0]
        tproxy.first_parent = (
            f'{self.workflow_id}{ID_DELIM}{point_string}{ID_DELIM}{p1_name}')
        return tproxy

    def generate_ghost_families(self, family_proxies=None, cycle_points=None):
        """Generate the family-point elements from tasks in cycle points.

        Args:
            task_id (str):
                valid TaskID string.

        Returns:

            list: [cylc.flow.ws_messages_pb2.PbFamilyProxy]
                list of populated family proxy data elements.

        """
        update_time = time()
        if family_proxies is None:
            family_proxies = {}
        fam_proxy_ids = {}
        for point_string, tasks in self.cycle_states.items():
            # construct family tree based on the
            # first-parent single-inheritance tree
            if not cycle_points or point_string not in cycle_points:
                continue
            cycle_first_parents = set()

            for key in tasks:
                for parent in self.ancestors.get(key, []):
                    if parent == key:
                        continue
                    cycle_first_parents.add(parent)

            for fam in cycle_first_parents:
                f_id = f'{self.workflow_id}{ID_DELIM}{fam}'
                if f_id not in self.families:
                    continue
                fp_id = (
                    f'{self.workflow_id}{ID_DELIM}'
                    f'{point_string}{ID_DELIM}{fam}')
                fp_check = f'{fp_id}@{update_time}'
                fproxy = PbFamilyProxy(
                    stamp=fp_check,
                    id=fp_id,
                    cycle_point=point_string,
                    name=fam,
                    family=f'{self.workflow_id}{ID_DELIM}{fam}',
                    depth=self.families[f_id].depth,
                )
                for child_name in self.descendants[fam]:
                    ch_id = (
                        f'{self.workflow_id}{ID_DELIM}'
                        f'{point_string}{ID_DELIM}{child_name}'
                    )
                    if self.parents[child_name][0] == fam:
                        if child_name in cycle_first_parents:
                            fproxy.child_families.append(ch_id)
                        elif child_name in self.schd.config.taskdefs:
                            fproxy.child_tasks.append(ch_id)
                if self.parents[fam]:
                    fproxy.parents.extend(
                        [f'{self.workflow_id}{ID_DELIM}'
                         f'{point_string}{ID_DELIM}{p_name}'
                            for p_name in self.parents[fam]])
                    p1_name = self.parents[fam][0]
                    fproxy.first_parent = (
                        f'{self.workflow_id}{ID_DELIM}'
                        f'{point_string}{ID_DELIM}{p1_name}')
                family_proxies[fp_id] = fproxy
                fam_proxy_ids.setdefault(f_id, []).append(fp_id)
        self.family_proxies = family_proxies
        for f_id, fp_ids in fam_proxy_ids.items():
            self.families[f_id].proxies[:] = fp_ids

    def generate_graph_elements(self, edges=None,
                                task_proxies=None, family_proxies=None,
                                start_point=None, stop_point=None):
        """Generate edges and [ghost] nodes (family and task proxy elements).

        Args:
            edges (dict, optional):
                ID-PbEdge key-value mapping.
            task_proxies (dict, optional):
                ID-PbTaskProxy key-value mapping.
            family_proxies (dict, optional):
                ID-PbFamilyProxy key-value mapping.
            start_point (cylc.flow.cycling.PointBase):
                Edge generation start point.
            stop_point (cylc.flow.cycling.PointBase):
                Edge generation stop point.

        """
        if not self.pool_points:
            return
        config = self.schd.config
        graph = PbEdges()
        if edges is None:
            edges = {}
        if task_proxies is None:
            task_proxies = {}
        if family_proxies is None:
            family_proxies = {}
        if start_point is None:
            start_point = min(self.pool_points)
        if stop_point is None:
            stop_point = max(self.pool_points)

        # Used for generating family [ghost] nodes
        new_points = set()

        # Generate ungrouped edges
        for edge in config.get_graph_edges(start_point, stop_point):
            # Reference or create edge source & target nodes/proxies
            s_node = edge[0]
            t_node = edge[1]
            if s_node is None:
                continue
            # Is the source cycle point in the task pool?
            s_name, s_point = TaskID.split(s_node)
            s_point_cls = get_point(s_point)
            s_pool_point = False
            s_valid = TaskID.is_valid_id(s_node)
            if s_valid:
                s_pool_point = s_point_cls in self.pool_points
            # Is the target cycle point in the task pool?
            t_pool_point = False
            t_valid = t_node and TaskID.is_valid_id(t_node)
            if t_valid:
                t_name, t_point = TaskID.split(t_node)
                t_point_cls = get_point(t_point)
                t_pool_point = get_point(t_point) in self.pool_points
            # Proceed if either source or target cycle points
            # are in the task pool.
            if not s_pool_point and not t_pool_point:
                continue
            # Initiate edge element.
            e_id = s_node + ID_DELIM + (t_node or 'NoTargetNode')
            edges[e_id] = PbEdge(
                id=f'{self.workflow_id}{ID_DELIM}{e_id}',
                suicide=edge[3],
                cond=edge[4],
            )
            # Evaluate whether source/target is a task or xtrigger
            # then add/create the corresponding id and items.
            # TODO: if xtrigger is suite_state create remote ID
            source_id = (
                f'{self.workflow_id}{ID_DELIM}{s_point}{ID_DELIM}{s_name}')
            edges[e_id].source = source_id
            if s_valid:
                s_task_id = f'{self.workflow_id}{ID_DELIM}{s_name}'
                new_points.add(s_point)
                # Add source points for pruning.
                self.edge_points.setdefault(s_point_cls, set())
                if source_id not in task_proxies:
                    task_proxies[source_id] = self.generate_ghost_task(s_node)
                if source_id not in self.tasks[s_task_id].proxies:
                    self.tasks[s_task_id].proxies.append(source_id)
            # At present targets can't be xtriggers.
            if t_valid:
                target_id = (
                    f'{self.workflow_id}{ID_DELIM}{t_point}{ID_DELIM}{t_name}')
                t_task_id = f'{self.workflow_id}{ID_DELIM}{t_name}'
                new_points.add(t_point)
                # Add target points to associated source points for pruning.
                self.edge_points.setdefault(s_point_cls, set())
                self.edge_points[s_point_cls].add(t_point_cls)
                if target_id not in task_proxies:
                    task_proxies[target_id] = self.generate_ghost_task(t_node)
                if target_id not in self.tasks[t_task_id].proxies:
                    self.tasks[t_task_id].proxies.append(target_id)
                edges[e_id].target = target_id

        # If no edges were created (i.e. no source/target met criteria).
        if not edges:
            return
        graph.edges.extend([e.id for e in edges.values()])
        graph.leaves.extend(config.leaves)
        graph.feet.extend(config.feet)
        for key, info in config.suite_polling_tasks.items():
            graph.workflow_polling_tasks.add(
                local_proxy=key,
                workflow=info[0],
                remote_proxy=info[1],
                req_state=info[2],
                graph_string=info[3],
            )

        self.generate_ghost_families(family_proxies, new_points)
        # Replace the originals (atomic update, for access from other threads).
        self.task_proxies = task_proxies
        self.edges = edges
        self.graph = graph

    def prune_points(self, point_strings):
        """Remove old nodes and edges by cycle point.

        Args:
            point_strings (list):
                Iterable of valid cycle point strings.

        """
        if not point_strings:
            return
        node_ids = set()
        tasks_proxies = {}
        for tp_id, tproxy in list(self.task_proxies.items()):
            if tproxy.cycle_point in point_strings:
                node_ids.add(tp_id)
                tasks_proxies.setdefault(tproxy.task, set()).add(tp_id)
                del self.task_proxies[tp_id]
        for t_id, tp_ids in tasks_proxies.items():
            self.tasks[t_id].proxies[:] = list(
                set(self.tasks[t_id].proxies) - tp_ids)

        for t_id in set(self.schd.job_pool.task_jobs) - set(self.task_proxies):
            self.schd.job_pool.remove_task_jobs(t_id)

        families_proxies = {}
        for fp_id, fproxy in list(self.family_proxies.items()):
            if fproxy.cycle_point in point_strings:
                families_proxies.setdefault(fproxy.family, set()).add(fp_id)
                del self.family_proxies[fp_id]
        for f_id, fp_ids in families_proxies.items():
            self.families[f_id].proxies[:] = list(
                set(self.families[f_id].proxies) - fp_ids)

        g_eids = set()
        for e_id, edge in list(self.edges.items()):
            if edge.source in node_ids or edge.target in node_ids:
                del self.edges[e_id]
                continue
            g_eids.add(edge.id)
        self.graph.edges[:] = list(g_eids)

        for point_string in point_strings:
            self.cycle_states.pop(point_string, None)
            self.state_count_cycles.pop(point_string, None)
        # Re-Compute state_counts (total, and per cycle).
        all_states = []
        state_count_cycles = {}
        for point_string, c_task_states in self.cycle_states.items():
            count = {}
            for state in c_task_states.values():
                if state is None:
                    continue
                try:
                    count[state] += 1
                except KeyError:
                    count[state] = 1
                all_states.append(state)
            state_count_cycles[point_string] = count
        self.all_states = all_states
        self.state_count_cycles = state_count_cycles

    def initiate_data_model(self, reload=False):
        """Initiate or Update data model on start/restart/reload.

        Args:
            reload (bool, optional):
                Reset data-store before regenerating.

        """
        # Reset attributes/data-store on reload:
        if reload:
            self.__init__(self.schd)
        # Static elements
        self.generate_definition_elements()
        self.increment_graph_elements()

    def increment_graph_elements(self):
        """Generate and/or prune graph elements if needed.

        Use the task pool and edge source/target cycle points to find
        new points to generate edges and/or old points to prune data-store.

        """
        # Gather task pool cycle points.
        old_pool_points = self.pool_points.copy()
        self.pool_points = set(list(self.schd.pool.pool))
        # No action if pool is not yet initiated.
        if not self.pool_points:
            return
        # Increment edges:
        # - Initially for each cycle point in the pool.
        # - For each new cycle point thereafter.
        # Using difference and pointwise allows for historical
        # task insertion (in gaps).
        new_points = self.pool_points.difference(old_pool_points)
        if new_points:
            for point in new_points:
                # All family & task cycle instances are generated and
                # populated with static data as 'ghost nodes'.
                self.generate_graph_elements(
                    self.edges, self.task_proxies, self.family_proxies,
                    point, point)
            self.min_point = min(self.pool_points)
            self.max_point = max(self.pool_points)
        # Prune data store by cycle point where said point is:
        # - Not in the set of pool points.
        # - Not a source or target cycle point in the set of edges.
        # This ensures a buffer of sources and targets in front and behind the
        # task pool, while accommodating exceptions such as ICP dependencies.
        # TODO: Turn nodes back to ghost if not in pool? (for suicide)
        prune_points = set()
        for s_point, t_points in list(self.edge_points.items()):
            if (s_point not in self.pool_points and
                    t_points.isdisjoint(self.pool_points)):
                prune_points.add(str(s_point))
                prune_points.union((str(t_p) for t_p in t_points))
                del self.edge_points[s_point]
                continue
            t_diffs = t_points.difference(self.pool_points)
            if t_diffs:
                prune_points.union((str(t_p) for t_p in t_diffs))
                self.edge_points[s_point].difference_update(t_diffs)
        # Action pruning if any eligible cycle points are found.
        if prune_points:
            self.prune_points(prune_points)
        if new_points or prune_points:
            # Pruned and/or additional elements require
            # state/status recalculation, and ID ref updates.
            self.update_workflow()

    def update_task_proxies(self, updated_tasks=None):
        """Update dynamic fields of task nodes/proxies.

        Args:
            updated_tasks (list): [cylc.flow.task_proxy.TaskProxy]
                Update task-node from corresponding given list of
                task proxy objects from the workflow task pool.

        """
        if not updated_tasks:
            return
        update_time = time()
        task_defs = {}

        # update task instance
        for itask in updated_tasks:
            name, point_string = TaskID.split(itask.identity)
            tp_id = (
                f'{self.workflow_id}{ID_DELIM}{point_string}{ID_DELIM}{name}')
            if tp_id not in self.task_proxies:
                continue
            self.cycle_states.setdefault(point_string, {})[name] = (
                itask.state.status)
            # Gather task definitions for elapsed time recalculation.
            if name not in task_defs:
                task_defs[name] = itask.tdef
            # Create new message and copy existing message content.
            tproxy = PbTaskProxy()
            # to avoid modification while being read.
            tproxy.CopyFrom(self.task_proxies[tp_id])
            tproxy.stamp = f'{tp_id}@{update_time}'
            tproxy.state = itask.state.status
            tproxy.job_submits = itask.submit_num
            tproxy.spawned = itask.has_spawned
            tproxy.latest_message = itask.summary['latest_message']
            tproxy.jobs[:] = self.schd.job_pool.task_jobs.get(tp_id, [])
            tproxy.broadcasts[:] = [
                f'{key}={val}' for key, val in
                self.schd.task_events_mgr.broadcast_mgr.get_broadcast(
                    itask.identity).items()]
            prereq_list = []
            for prereq in itask.state.prerequisites:
                # Protobuf messages populated within
                prereq_obj = prereq.api_dump(self.workflow_id)
                if prereq_obj:
                    prereq_list.append(prereq_obj)
            # Unlike the following list comprehension repeated message
            # fields cannot be directly assigned, so is cleared first.
            del tproxy.prerequisites[:]
            tproxy.prerequisites.extend(prereq_list)
            tproxy.outputs[:] = [
                f'{trigger}={is_completed}'
                for trigger, _, is_completed in itask.state.outputs.get_all()
            ]
            # Replace the original
            # (atomic update, for access from other threads).
            self.task_proxies[tp_id] = tproxy

        # Recalculate effected task def elements elapsed time.
        for name, tdef in task_defs.items():
            elapsed_time = task_mean_elapsed_time(tdef)
            if elapsed_time:
                t_id = f'{self.workflow_id}{ID_DELIM}{name}'
                self.tasks[t_id].stamp = f'{t_id}@{update_time}'
                self.tasks[t_id].mean_elapsed_time = elapsed_time

    def update_family_proxies(self, cycle_points=None):
        """Update state of family proxies.

        Args:
            cycle_points (list):
                Update family-node state from given list of
                valid cycle point strings.

        """
        if cycle_points is None:
            cycle_points = self.cycle_states.keys()
        if not cycle_points:
            return
        update_time = time()

        # Compute state_counts (total, and per cycle).
        all_states = []
        state_count_cycles = {}

        for point_string in cycle_points:
            # For each cycle point, construct a family state tree
            # based on the first-parent single-inheritance tree
            c_task_states = self.cycle_states.get(point_string, None)
            if c_task_states is None:
                continue
            c_fam_task_states = {}
            count = {}

            for key in c_task_states:
                state = c_task_states[key]
                if state is None:
                    continue
                try:
                    count[state] += 1
                except KeyError:
                    count[state] = 1

                all_states.append(state)
                for parent in self.ancestors.get(key, []):
                    if parent == key:
                        continue
                    c_fam_task_states.setdefault(parent, set())
                    c_fam_task_states[parent].add(state)

            state_count_cycles[point_string] = count

            for fam, child_states in c_fam_task_states.items():
                state = extract_group_state(child_states)
                fp_id = (
                    f'{self.workflow_id}{ID_DELIM}'
                    f'{point_string}{ID_DELIM}{fam}')
                if state is None or fp_id not in self.family_proxies:
                    continue
                # Since two fields strings are reassigned,
                # it should be safe without copy.
                fproxy = self.family_proxies[fp_id]
                fproxy.stamp = f'{fp_id}@{update_time}'
                fproxy.state = state

        self.all_states = all_states
        self.state_count_cycles = state_count_cycles

    def update_workflow_statuses(self):
        """Update workflow element status and state totals."""
        # Create new message and copy existing message content
        update_time = time()
        workflow = PbWorkflow()
        workflow.CopyFrom(self.workflow)
        workflow.stamp = f'{self.workflow_id}@{update_time}'
        workflow.last_updated = update_time

        state_count_totals = {}
        for _, count in list(self.state_count_cycles.items()):
            for state, state_count in count.items():
                state_count_totals.setdefault(state, 0)
                state_count_totals[state] += state_count
        for state, count in state_count_totals.items():
            setattr(workflow.state_totals,
                    copy(state).replace('-', '_'), count)

        workflow.last_updated = update_time
        workflow.states.extend(list(set(self.all_states)).sort())
        workflow.reloading = self.schd.pool.do_reload

        # Construct a workflow status string for use by monitoring clients.
        workflow.status, workflow.status_msg = map(
            str, get_suite_status(self.schd))

        # Return workflow element for additional manipulation.
        return workflow

    def update_workflow(self):
        """Update and populate dynamic fields of workflow element."""
        workflow = self.update_workflow_statuses()

        for key, value in (
                ('oldest_cycle_point', self.min_point),
                ('newest_cycle_point', self.max_point),
                ('newest_runahead_cycle_point',
                    self.schd.pool.get_max_point_runahead())):
            if value:
                setattr(workflow, key, str(value))
            else:
                setattr(workflow, key, '')

        workflow.task_proxies[:] = list(self.task_proxies)
        workflow.family_proxies[:] = list(self.family_proxies)
        workflow.ClearField('edges')
        workflow.edges.CopyFrom(self.graph)

        # Replace the original (atomic update, for access from other threads).
        self.workflow = workflow

    def update_dynamic_elements(self, updated_nodes=None):
        """Update data elements containing dynamic/live fields."""
        # If no tasks are given update all
        if updated_nodes is None:
            updated_nodes = self.schd.pool.get_all_tasks()
        elif not updated_nodes:
            return
        self.update_task_proxies(updated_nodes)
        self.update_family_proxies(set(str(t.point) for t in updated_nodes))
        self.workflow = self.update_workflow_statuses()

    # Message collation and dissemination methods:
    def get_entire_workflow(self):
        """Gather data elements into single Protobuf message."""
        workflow_msg = PbEntireWorkflow()
        workflow_msg.workflow.CopyFrom(self.workflow)
        workflow_msg.tasks.extend(list(self.tasks.values()))
        workflow_msg.task_proxies.extend(list(self.task_proxies.values()))
        workflow_msg.jobs.extend(list(self.schd.job_pool.pool.values()))
        workflow_msg.families.extend(list(self.families.values()))
        workflow_msg.family_proxies.extend(list(self.family_proxies.values()))
        workflow_msg.edges.extend(list(self.edges.values()))

        return workflow_msg