import csv
import argparse
import pathlib
import sqlite3
import functools
import dateutil.parser
import itertools
import collections

from utils import *

insert_tpl = '''
INSERT INTO identifiers_history VALUES (?, ?, ?, ?, ?, ?)
'''

timestamp_parser_cached = functools.lru_cache(100000)(dateutil.parser.parse)

InputRecord = collections.namedtuple(
    'InputRecord',
    'identifier_type identifier_id action timestamp project page_id revision_id',
)

OutputRecord = collections.namedtuple(
    'OutputRecord',
    "project page_id identifier_type identifier_id start_date end_date",
)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_files',
        nargs='+',
        type=pathlib.Path,
        help='''Input csv files. They must be ordered by ([project], page, timestamp)''',
    )
    parser.add_argument(
        'sqlite_file',
    )
    parser.add_argument(
        '--create-tables',
        action='store_true',
        default=False,
        required=False,
    )
    return parser.parse_args()


def create_tables_and_indexes(connection):
    with connection:
        connection.executescript('''
-- Table: identifiers_history
CREATE TABLE IF NOT EXISTS identifiers_history (
    project TEXT NOT NULL,
    page_id INTEGER NOT NULL,
    identifier_type TEXT NOT NULL,
    identifier_id TEXT NOT NULL,
    start_date DATETIME,
    end_date DATETIME,
    PRIMARY KEY (project, page_id, identifier_type, identifier_id, start_date, end_date)
);

-- Index: timestamp_asc
-- CREATE INDEX IF NOT EXISTS timestamp_asc ON identifiers_history (timestamp ASC);

-- Index: identifier_asc
CREATE INDEX IF NOT EXISTS identifier_asc ON identifiers_history (
    identifier_type ASC,
    identifier_id ASC
);

-- Index: page_revision_asc
CREATE INDEX IF NOT EXISTS project_page_revision_asc ON identifiers_history (
    project ASC,
    page_id ASC
);
    ''')

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

def parse_record(r, default_project=None):
    # no project given
    if len(r) == 6:
        identifier_type, identifier_id, action, timestamp, page_id, revision_id = r
        project = default_project
    else:
        identifier_type, identifier_id, action, timestamp, project, page_id, revision_id = r

    timestamp = timestamp_parser_cached(timestamp)
    return InputRecord(identifier_type, identifier_id, action, timestamp, project, page_id, revision_id)

def merge_records(records):
    for _, group in itertools.groupby(records, key=lambda r: (r.project, r.page_id)):
        key_by_project_page_id_ts = lambda r: (r.project, r.page_id, r.identifier_type, r.identifier_id, r.timestamp)
        group_per_page = sorted(group, key=key_by_project_page_id_ts)

        key_by_project_page_id = lambda r: (r.project, r.page_id, r.identifier_type, r.identifier_id)
        for key, group in itertools.groupby(group_per_page,  key=key_by_project_page_id):
            for r1, r2 in grouper(group, 2, fillvalue=InputRecord(None, None, 'removed', None, None, None, None)):
                assert r1.action != r2.action
                assert r1.revision_id != r2.revision_id

                yield OutputRecord(
                    r1.project,
                    r1.page_id,
                    r1.identifier_type,
                    r1.identifier_id,
                    r1.timestamp,
                    r2.timestamp,
                )

def main():
    args = parse_args()

    conn = sqlite3.connect(args.sqlite_file)
    # db = peewee.SqliteDatabase(args.sqlite_file, autocommit=False)

    # models.database_proxy.initialize(db)

    if args.create_tables:
        print('Creating tables and indexes')
        create_tables_and_indexes(conn)
        # models.create_tables()

    for file_path in args.input_files:
        print('Reading', file_path, '...')
        input_file = open_compressed_file(file_path)
        with input_file, conn:
            csvreader = csv.reader(input_file)
            input_records = (parse_record(r, 'en') for r in csvreader)

            # for r in merge_records(input_records):
            #     print(r)

            conn.executemany(insert_tpl, merge_records(input_records))


if __name__ == '__main__':
    main()
