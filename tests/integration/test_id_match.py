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
    schd = scheduler(id_)
    async with start(schd):
        schd: Scheduler
        # match = partial(id_match, schd.config, schd.pool.active_tasks)

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

        assert {itask.tokens.relative_id for itask in schd.pool.get_tasks()} == {'1/a1', '1/b2', '2/a1', '2/a2'}


        def match(*ids: str) -> Tuple[Set[str], Set[str]]:
            matched, unmatched = id_match(
                schd.config,
                schd.pool.active_tasks,
                {Tokens(id_, relative=True) for id_ in ids},
            )
            return {id_.relative_id for id_ in matched}, {
                id_.relative_id for id_ in unmatched
            }

        # test patterns
        assert (
            match('*/*')
            == match('*/root')
            == match('*')
            == (
                {'1/a1', '1/b2', '2/a1', '2/a2'},
                set(),
            )
        )

        assert match('1/*') == ({'1/a1', '1/b2'}, set())


        # test plain ids
        assert match('1/A') == ({'1/a1', '1/a2'}, set())
        assert match('1/A', '1/B', '1/C') == ({'1/a1', '1/a2', '1/b1', '1/b2'}, {'1/C'})
        assert match('1/root') == ({'1/a1', '1/a2', '1/b1', '1/b2', '1/c1', '1/c2'}, set())
