from cylc.flow import commands
from functools import partial
from cylc.flow.id import Tokens
from cylc.flow.scheduler import Scheduler
from cylc.flow.id_match import id_match
from typing import List, Set, Tuple


async def test_id_match(flow, scheduler, start):
    id_ = flow({
        'scheduling': {
            'cycling mode': 'integer',
            'initial cycle point': '1',
            'final cycle point': '2',
            'graph': {
               'P1': '''
                    a1 => b1 => c1
                    a2 => b2 => c2

                    b1[-P1] => b1
                    b2[-P1] => b2
                ''',
            },
        },
        'runtime': {
            'a1, a2': {'inherit': 'A'},
            'A': {},
            'b1, b2': {'inherit': 'B'},
            'B': {},
            'c1, c2': {},
        }
    })
    schd: Scheduler = scheduler(id_)

    def match(*ids: str) -> Tuple[Set[str], Set[str]]:
        matched, unmatched = id_match(
            schd.config,
            schd.pool.active_tasks,
            {Tokens(id_, relative=True) for id_ in ids},
        )
        return {id_.relative_id for id_ in matched}, {
            id_.relative_id_with_selectors for id_ in unmatched
        }

    async with start(schd):
        await commands.run_cmd(commands.set_prereqs_and_outputs(schd, ['1/a2'], ['1'], ['succeeded'], None))
        await commands.run_cmd(commands.set_prereqs_and_outputs(schd, ['1/b2'], ['1'], ['failed'], None))

        # task pool state:
        # * cycle 1
        #   * n=0 a1 waiting
        #   * n=1 b1 waiting
        #   * n=2 c1 waiting
        #   * n=1 a2 succeeded
        #   * n=0 b2 failed
        #   * n=1 c2 waiting
        # * cycle 2
        #   * n=0 a1 waiting
        #   * n=1 b1 waiting
        #   * n=2 c1 waiting
        #   * n=0 a2 waiting
        #   * n=1 b2 waiting
        #   * n=2 c2 waiting

        # check the n=0 window matches expecations before proceeding
        assert {itask.tokens.relative_id for itask in schd.pool.get_tasks()} == {'1/a1', '1/b2', '2/a1', '2/a2'}

        # test active task selection
        assert (
            match('*:waiting')
            == match('*/root:waiting')
            == match('*/*:waiting')
            == match('*/A:waiting')
            == match('*/a*:waiting')
            == match('1/a1:waiting', '2/a1:waiting', '2/a2:waiting')
            == ({'1/a1', '2/a1', '2/a2'}, set())
        )

        assert (
            match('*:failed')
            == match('*/root:failed')
            == match('*/*:failed')
            == match('*/B:failed')
            == match('*/b*:failed')
            == match('1/b2:failed')
            == ({'1/b2'}, set())
        )

        assert (
            match('1/b1:failed', '1/b2:failed')
            == match('1/B:failed', '1/b1:failed')
            == match('*:failed', '1/B:failed', '1/b1:failed')
            == ({'1/b2'}, {'1/b1:failed'})
        )

        # test regular task selection
        assert (
            match('*')
            == match('*/*')
            == match('*/root')
            == match('*/A', '*/B', '*/c1', '*/c2')
            == match('*/a*', '*/b*', '*/c*')
            == (
                {
                    '1/a1',
                    '1/a2',
                    '1/b1',
                    '1/b2',
                    '1/c1',
                    '1/c2',
                    '2/a1',
                    '2/a2',
                    '2/b1',
                    '2/b2',
                    '2/c1',
                    '2/c2',
                },
                set(),
            )
        )

        assert (
            match('1/A')
            == match('1/a*')
            == match('1/a1', '1/a2')
            == ({'1/a1', '1/a2'}, set())
        )
