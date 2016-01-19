import argparse
import collections
import csv
import datetime
import functools
import sqlite3
import urllib.parse
from pprint import pprint

import dateutil.parser
import ipdb
import numpy
import pagecountssearch
import pathlib
import pymysql
import scipy.interpolate
import utils

now = datetime.datetime.now

InputRecord = collections.namedtuple(
    'InputRecord',
    [
        'project',
        'page_id',
        'page_title',
        'identifier_type',
        'identifier_id',
        'start_date',
        'end_date',
    ],
)

OutputRecord = collections.namedtuple(
    'OutputRecord',
    [
        'project',
        'page_id',
        'page_title',
        'identifier_type',
        'identifier_id',
        'start_date',
        'end_date',
        'views',
    ],
)

PageMove = collections.namedtuple(
    'PageMove',
    'timestamp from_ to',
)

PagePeriod = collections.namedtuple(
    'PagePeriod',
    'page start end',
)

TimeSpan = collections.namedtuple(
    'TimeSpan',
    'start end',
)


@functools.lru_cache(10000)
def parse_timestamp(timestamp: str):
    return dateutil.parser.parse(timestamp)


def parse_record(raw_record):
    project, page_id, page_title, identifier_type, identifier_id, start_date, end_date = raw_record

    page_id = int(page_id)
    if not end_date:
        end_date = None
    else:
        end_date = parse_timestamp(end_date)

    if not start_date:
        start_date = None
    else:
        start_date = start_date = parse_timestamp(start_date)

    return InputRecord(
        project,
        page_id,
        page_title,
        identifier_type,
        identifier_id,
        start_date,
        end_date,
    )

# def parse_move_record(record):
#     timestamp, from_, to = record
#
#     to = to.rstrip('\n')
#     timestamp = dateutil.parser.parse(timestamp)
#
#     return MoveRecord(timestamp, from_, to)

class ViewsCounter:
    def __init__(
            self,
            finder,
            start_period=None,
            end_period=None,
            granularity=datetime.timedelta(hours=1)):
        self.finder = finder
        self.granularity = granularity
        self.period = TimeSpan(start_period, end_period)

    # Adjust the lru_cache with respect to the average number of redirect.
    # According to the 20150901 dump, there average number of in-redirect
    # per page is 2,78 and standard deviation is is 8,31.
    # You can check it with the following query:
    # select avg(res.rd_sum), variance(res.rd_sum)
    # from (
    # 	select rd_title, count(rd_from) as rd_sum
    #     from redirect
    #     where rd_namespace = 0
    #     group by rd_namespace, rd_title
    # ) as res
    @functools.lru_cache(50)
    def interp_fn(self, project, page):
        granularity = self.granularity
        page = wikify_title(page)

        tic = now()
        print('Searching for ', project, page)
        result = self.finder.search(project, page)
        toc = now()
        print('Search took:', toc - tic)

        if len(result) == 0:
            print("Warning: stats not found")
            return (lambda val: 0), 0, 0

        print('Computing interpolation function for ', project, page)
        tic = now()
        views_list = [(ts, views) for ts, views, _ in result]

        timestamps = [t for t, _ in views_list]
        views = [v for _, v in views_list]

        first, last = timestamps[0], timestamps[-1]
        start = first - granularity
        end = last + granularity

        start_unix = int(start.timestamp())
        end_unix = int(end.timestamp())
        granularity_unix = int(granularity.total_seconds())

        xs = list(range(start_unix, end_unix, granularity_unix))
        ys = list(0 for _ in range(len(xs)))

        for timestamp, views in views_list:
            timestamp_unix = int(timestamp.timestamp())
            index = xs.index(timestamp_unix)
            ys[index] = views

        # print('xs', xs)
        # print('ys', ys)

        scipy_interp = scipy.interpolate.interp1d(
            xs,
            numpy.cumsum(ys),
            assume_sorted=True,
        )

        toc = now()
        print('Interp took:', toc - tic)

        def interp(x):
            if isinstance(x, datetime.datetime):
                x = to_unix_timestamp(x)

            if x < xs[0]:
                return 0.0
            if x > xs[-1]:
                return xs[-1]
            else:
                return scipy_interp(x)

        return interp, xs[0], xs[-1]

    def count(self, project, page, start_date, end_date):
        # Avoid useless computation and I/O
        page = wikify_title(page)
        if not timespan_intersects(
                self.period,
                TimeSpan(start_date, end_date),
                ):
            return 0

        interp, min_, max_ = self.interp_fn(project, page)

        if end_date is None:
            upper = max_
        else:
            upper = interp(end_date)

        if start_date is None:
            lower = min_
        else:
            lower = interp(start_date)

        return upper - lower


def to_unix_timestamp(datetime):
    return int(datetime.timestamp())


def timespan_intersects(*timespans):
    start, end = float('-infinity'), float('+infinity')
    for timespan in timespans:
        if timespan.start is not None:
            start = max(
                start,
                to_unix_timestamp(timespan.start),
            )
        if timespan.end is not None:
            end = min(
                end,
                to_unix_timestamp(timespan.end),
            )

        if start > end:
            return False

    return True


def timestamp_max(*ts):
    without_nones = filter(lambda x: x is not None, ts)
    return max(without_nones, default=None)


def timestamp_min(*ts):
    without_nones = filter(lambda x: x is not None, ts)
    return min(without_nones, default=None)


def test_timespan_intersects():
    forever = TimeSpan(None, None)
    from_2011 = TimeSpan(
        None,
        parse_timestamp('20110101'),
    )
    from_2011_to_2012 = TimeSpan(
        parse_timestamp('20110101'),
        parse_timestamp('20120101'),
    )
    from_2013_to_2015 = TimeSpan(
        parse_timestamp('20130101'),
        parse_timestamp('20150101'),
    )
    from_2014_to_2016 = TimeSpan(
        parse_timestamp('20140101'),
        parse_timestamp('20160101'),
    )
    assert timespan_intersects(forever, from_2013_to_2015)
    assert timespan_intersects(from_2013_to_2015, from_2014_to_2016)
    assert not timespan_intersects(from_2011_to_2012, from_2014_to_2016)

    assert timespan_intersects(from_2011, from_2011_to_2012)
    assert timespan_intersects(forever, from_2011)

    assert timespan_intersects(from_2013_to_2015, from_2013_to_2015)
    assert timespan_intersects(from_2011, from_2011)
    assert timespan_intersects(forever, forever)

# def test_extract_views():
#     utc = datetime.timezone.utc
#     print(extract_views(
#         [TimeSpan(
#             start=datetime.datetime(2014, 1, 1, 0, tzinfo=utc),
#             end=datetime.datetime(2014, 1, 1, 3, 30, tzinfo=utc),
#         )],
#         [
#             (datetime.datetime(2014, 1, 1, 0, tzinfo=utc), 10),
#             (datetime.datetime(2014, 1, 1, 1, tzinfo=utc), 20),
#             (datetime.datetime(2014, 1, 1, 2, tzinfo=utc), 5),
#             (datetime.datetime(2014, 1, 1, 4, tzinfo=utc), 15),
#         ])
#     )


# def periods_from_moves(current_page_title, moves):
#     descending_moves = sorted(moves, key=lambda m: m.timestamp, reverse=True)
#
#     page_periods = [
#         PagePeriod(current_page_title, None, None),
#     ]
#
#     for timestamp, from_, to in descending_moves:
#         last = page_periods[-1]
#
#         page_periods[-1] = PagePeriod(last.page, timestamp, last.end)
#
#         page_periods.append(
#             PagePeriod(from_, None, timestamp)
#         )
#     return page_periods

# def test_periods_from_moves():
#     moves = [
#         PageMove(
#             parse_timestamp('2014-11-26 15:28:23+00:00'),
#             'Spanish conquest of Chiapas',
#             'Spanish arrival to Chiapas',
#         ),
#         PageMove(
#             parse_timestamp('2014-11-26 15:35:21+00:00'),
#             'Spanish arrival to Chiapas',
#             'Spanish conquest of Chiapas',
#         ),
#         PageMove(
#             parse_timestamp('2014-11-26 15:40:59+00:00'),
#             'Spanish conquest of Chiapas',
#             'Spanish arrival to Chiapas',
#         ),
#         PageMove(
#             parse_timestamp('2014-11-26 16:31:13+00:00'),
#             'Spanish arrival to Chiapas',
#             'Spanish conquest of Chiapas',
#         ),
#     ]
#
#     assert periods_from_moves('Spanish conquest of Chiapas', moves) == [
#         PagePeriod(
#             'Spanish conquest of Chiapas',
#             parse_timestamp('2014-11-26 16:31:13+00:00'),
#             None,
#         ),
#         PagePeriod(
#             'Spanish arrival to Chiapas',
#             parse_timestamp('2014-11-26 15:40:59+00:00'),
#             parse_timestamp('2014-11-26 16:31:13+00:00'),
#         ),
#         PagePeriod(
#             'Spanish conquest of Chiapas',
#             parse_timestamp('2014-11-26 15:35:21+00:00'),
#             parse_timestamp('2014-11-26 15:40:59+00:00'),
#         ),
#         PagePeriod(
#             'Spanish arrival to Chiapas',
#             parse_timestamp('2014-11-26 15:28:23+00:00'),
#             parse_timestamp('2014-11-26 15:35:21+00:00'),
#         ),
#         PagePeriod(
#             'Spanish conquest of Chiapas',
#             None,
#             parse_timestamp('2014-11-26 15:28:23+00:00'),
#         ),
#     ]
#
#     assert periods_from_moves('Spanish conquest of Chiapas', []) == [
#         PagePeriod('Spanish conquest of Chiapas', None, None),
#     ]

# @functools.lru_cache(1000)
# def get_page_periods(conn, project, page_title):
#     moves = list(get_moves(conn, project, page_title))
#     moves.sort(key=lambda r: r.timestamp)
#
#     print('Moves:')
#     pprint(moves)
#
#     periods = list(periods_from_moves(page_title, moves))
#     print('Page periods:')
#     pprint(periods)
#     return periods

def get_redirects_for(cursor, page_title):
    page_title = wikify_title(page_title)
    with cursor:
        cursor.execute('''
            select rd_from
            from redirect
            where rd_namespace = 0 and rd_title = %s
            ''', (page_title,))
        rows = cursor.fetchall()
        ids = [row[0] for row in rows]
        return ids


def get_page_title(cursor, page_id):
    with cursor:
        cursor.execute('''
            select page_title
            from page
            where page_id = %s
            ''', (page_id,))
        row = cursor.fetchone()
        title = row[0]
        if not isinstance(title, str):
            title = title.decode('utf-8', errors='replace')
        return title


def counts_for_page(
        connection: sqlite3.Connection,
        views_counter: ViewsCounter,
        project: str,
        page_id: int,
        page_title: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime):

    print('Looking for counts for', project, page_title, start_date, end_date)

    redirects_ids = get_redirects_for(
        connection.cursor(),
        page_title=page_title,
    )
    redirects_titles = [
        get_page_title(connection.cursor(), page_id)
        for page_id in redirects_ids
    ]

    sum_ = sum(
        views_counter.count(project, page, start_date, end_date)
        for page in redirects_titles + [page_title]
    )
    print('Sum:', sum_)

    return sum_


def wikify_title(page_title):
    return page_title.replace(' ', '_')


def add_utc_if_naive(timestamp: datetime.datetime):
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

    return timestamp


def parse_cmdline_date(timestamp: str):
    return add_utc_if_naive(
        dateutil.parser.parse(timestamp)
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_files',
        nargs='+',
        type=pathlib.Path,
    )
    parser.add_argument(
        'counts_dataset_dir',
        type=pathlib.Path,
    )
    parser.add_argument(
        '--counts-period-start',
        required=True,
        type=parse_cmdline_date,
    )
    parser.add_argument(
        '--counts-period-end',
        required=True,
        type=parse_cmdline_date,
    )
    parser.add_argument(
        'db_url',
        type=urllib.parse.urlparse,
        help='Database connection URL',
    )
    parser.add_argument(
        'output_dir',
        type=pathlib.Path,
    )
    return parser.parse_args()


# def get_moves(connection, project, page_title):
#     c = connection
#     query = '''
#     select distinct * from moves where project = ? and page_title_to = ?
#     '''
#
#     to_search = {page_title}
#     done = set()
#     result = []
#     with c:
#         while to_search - done:
#             curr_title = to_search.pop()
#
#             rows = c.execute(query, (project, curr_title)).fetchall()
#
#             for row in rows:
#                 print('row:', tuple(row))
#                 result.append(PageMove(
#                     timestamp=row['timestamp'],
#                     from_=row['page_title_from'],
#                     to=row['page_title_to'],
#                 ))
#                 to_search.add(row['page_title_from'])
#             done.add(curr_title)
#
#     result.sort(key=lambda r: r.timestamp)
#     return result

# def get_moves(connection, project, page_title):
#     return get_moves_rec(connection, project, page_title, up_to=None, delete_shield=(page_title, ))
#
# def get_moves_rec(connection, project, page_title, up_to=None, delete_shield=None):
#     c = connection
#     query_moves = '''
#     select distinct * from moves where project = ? and page_title_to = ?
#     '''
#
#     query_deletions = '''
#     select distinct * from deletions where project = ? and page_title = ?
#     '''
#     result = set()
#     if up_to is not None:
#         rows = c.execute('''
#             select distinct *
#             from moves
#             where project = ? and page_title_to = ? and timestamp <= ?
#             ''', (project, page_title, up_to)).fetchall()
#     else:
#         rows = c.execute('''
#             select distinct *
#             from moves
#             where project = ? and page_title_to = ?
#             ''', (project, page_title)).fetchall()
#
#     if not rows:
#         return []
#
#     for row in rows:
#         if row['page_title_from'] in delete_shield:
#             is_deleted = False
#         else:
#             is_deleted = len(c.execute('''
#                 select distinct *
#                 from deletions
#                 where project = ? and page_title = ?
#                 ''', (project, row['page_title_from'])).fetchall()) >= 1
#
#         submoves = get_moves_rec(
#             connection,
#             project,
#             row['page_title_from'],
#             row['timestamp'],
#             delete_shield=delete_shield,
#         )
#         if not submoves and is_deleted:
#             print('Page ', row['page_title_from'], 'has been deleted. skip')
#             continue
#
#         result.add(PageMove(
#             timestamp=row['timestamp'],
#             from_=row['page_title_from'],
#             to=row['page_title_to'],
#         ))
#
#         result.update(submoves)
#     return result

def main():
    args = parse_args()
    print(args)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # sqlite_conn = sqlite3.connect(
    #     str(args.moves_sqlite),
    #     detect_types=sqlite3.PARSE_DECLTYPES,
    # )
    # sqlite_conn.row_factory = sqlite3.Row
    # ipdb.set_trace()  ######### Break Point ###########
    #
    # r=get_page_periods(moves_conn, 'en', '\'Abd al-Rahman I')
    # periods = get_page_periods(moves_conn, 'en', 'Spanish conquest of Chiapas')
    db_url = args.db_url
    db_vars = dict(
        host=db_url.hostname,
        port=db_url.port or 3306,
        user=db_url.username,
        password=db_url.password or '',
        database=db_url.path.rpartition('/')[-1]
    )
    print(db_vars)
    db_conn = pymysql.connect(
        **db_vars
    )

    counts_finder = pagecountssearch.Finder(args.counts_dataset_dir)
    views_counter = ViewsCounter(
        counts_finder,
        start_period=args.counts_period_start,
        end_period=args.counts_period_end,
        granularity=datetime.timedelta(hours=1),
    )

    for input_file_path in args.input_files:
        input_file = utils.open_compressed_file(input_file_path)
        basename = input_file_path.name

        output_file_path = args.output_dir/basename
        output_file = output_file_path.open('wt', encoding='utf-8')
        with input_file, output_file:
            raw_records = csv.reader(input_file)

            input_records = (parse_record(r) for r in raw_records)

            output_records = (
                OutputRecord(
                    *r,
                    counts_for_page(
                        db_conn,
                        views_counter,
                        r.project,
                        r.page_id,
                        r.page_title,
                        r.start_date,
                        r.end_date,
                    ),
                )
                for r in input_records
            )

            writer = csv.writer(output_file)

            for output_record in output_records:
                writer.writerow(output_record)


if __name__ == '__main__':
    main()
