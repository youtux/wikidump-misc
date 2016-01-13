import ipdb
import pathlib
import csv
import collections
import dateutil.parser
import functools
import argparse
import datetime
import scipy.interpolate
import numpy

import pagecountssearch

import utils

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
    start_date = parse_timestamp(start_date)
    if end_date is not None:
        end_date = parse_timestamp(end_date)

    return InputRecord(
        project,
        page_id,
        page_title,
        identifier_type,
        identifier_id,
        start_date,
        end_date,
    )

class ViewsCounter:
    def __init__(self, finder, granularity=datetime.timedelta(hours=1)):
        self.finder = finder
        self.granularity = granularity

    @functools.lru_cache(10000)
    def interp_fn(self, project, page):
        print('Computing interpolation function for ', project, page)
        granularity = self.granularity

        result = self.finder.search(project, page)

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

        print('xs', xs)
        print('ys', ys)

        scipy_interp = scipy.interpolate.interp1d(
            xs,
            numpy.cumsum(ys),
            assume_sorted=True,
        )

        def interp(x):
            if isinstance(x, datetime):
                x = to_unix_timestamp(x)

            if x < xs[0]:
                return 0.0
            if x > xs[-1]:
                return xs[-1]
            else:
                return scipy_interp(x)

        return interp

    def count(self, project, page, start_date, end_date):
        interp = self.interp_fn(project, page)

        return interp(end_date) - interp(start_date)


def to_unix_timestamp(datetime):
    return int(datetime.timestamp())

def extract_views(time_spans, views_list, granularity=datetime.timedelta(hours=1)):
    timestamps = [t for t, _ in views_list]
    views = [v for _, v in views_list]

    first, last = timestamps[0], timestamps[-1]
    start = first - granularity
    end = last + granularity


    start_unix = int(start.timestamp())
    end_unix = int(end.timestamp())
    granularity_unix = int(granularity.total_seconds())

    x = list(range(start_unix, end_unix, granularity_unix))
    y = list(0 for _ in range(len(x)))

    for timestamp, views in views_list:
        timestamp_unix = int(timestamp.timestamp())
        index = x.index(timestamp_unix)
        y[index] = views

    print('x', x)
    print('y', y)

    interp_f = scipy.interpolate.interp1d(x, numpy.cumsum(y), assume_sorted=True)

    count = 0
    for start, end in time_spans:
        assert start <= end
        start_unix, end_unix = to_unix_timestamp(start), to_unix_timestamp(end)
        start_unix = max(start_unix, x[0])
        start_unix = min(start_unix, x[-1])

        end_unix = min(end_unix, x[-1])
        end_unix = max(end_unix, x[0])

        count += interp_f(end_unix) - interp_f(start_unix)
    return count


def test_extract_views():
    utc = datetime.timezone.utc
    print(extract_views(
        [TimeSpan(
            start=datetime.datetime(2014, 1, 1, 0, tzinfo=utc),
            end=datetime.datetime(2014, 1, 1, 3, 30, tzinfo=utc),
        )],
        [
            (datetime.datetime(2014, 1, 1, 0, tzinfo=utc), 10),
            (datetime.datetime(2014, 1, 1, 1, tzinfo=utc), 20),
            (datetime.datetime(2014, 1, 1, 2, tzinfo=utc), 5),
            (datetime.datetime(2014, 1, 1, 4, tzinfo=utc), 15),
        ])
    )


def counts_for_record(record, views_counter: ViewsCounter):
    page = wikify_title(record.page_title)

    return views_counter.count(
        record.project,
        page,
        record.start_date,
        record.end_date,
    )


def wikify_title(page_title):
    return page_title.replace(' ', '_')

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
        'output_dir',
        type=pathlib.Path,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print(args)

    counts_finder = pagecountssearch.Finder(args.counts_dataset_dir)
    views_counter = ViewsCounter(
        counts_finder,
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
                OutputRecord(*r, counts_for_record(r, views_counter))
                for r in input_records
            )

            writer = csv.writer(output_file)

            for output_record in output_records:
                writer.writerow(output_record)


if __name__ == '__main__':
    main()