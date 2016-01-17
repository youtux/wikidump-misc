import ipdb
import argparse
import collections
import sqlite3
import pathlib
import functools
import dateutil.parser
import csv

import utils

Record = collections.namedtuple(
    'Record',
    'timestamp from_ to')


@functools.lru_cache(10000)
def parse_timestamp(timestamp: str):
    return dateutil.parser.parse(timestamp)


def parse_record(record):
    timestamp, from_, to = record
    from_ = from_.rstrip('\n')
    to = to.rstrip('\n')

    timestamp = parse_timestamp(timestamp)

    return Record(timestamp, from_, to)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_file',
        type=pathlib.Path,
    )
    parser.add_argument(
        '--project',
        required=True,
        help='Project name',
    )
    parser.add_argument(
        'sqlite_file',
        help='Output sqlite file name',
    )
    parser.add_argument(
        '--create-indexes',
        action='store_true',
        help='''Create indexes for fast access. This will cause the file to grow. Like a lot.''',
    )
    return parser.parse_args()

def create_tables(connection):
    with connection:
        connection.executescript('''
-- Table: identifiers_history
CREATE TABLE IF NOT EXISTS moves (
    timestamp DATETIME NOT NULL,
    project TEXT NOT NULL,
    page_title_from TEXT NOT NULL,
    page_title_to TEXT NOT NULL
    --PRIMARY KEY (timestamp, project, page_title_from, page_title_to)
);
    ''')

def create_indexes(connection):
    with connection:
        connection.executescript('''
CREATE INDEX IF NOT EXISTS timestamp_asc ON moves (timestamp ASC);

CREATE INDEX IF NOT EXISTS project_page_title_to ON move (
    project ASC,
    page_title_to ASC
);
''')

def main():
    args = parse_args()

    input_file = utils.open_compressed_file(args.input_file)
    conn = sqlite3.connect(str(args.sqlite_file))

    create_tables(conn)
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = MEMORY')

    print('Inserting data...')
    with input_file, conn:
        reader = csv.reader(input_file)
        assert next(reader) == ['timestamp', 'from', 'to']
        records = (parse_record(r) for r in reader)

        db_records = (
            (r.timestamp, args.project, r.from_, r.to)
            for r in records
        )

        conn.executemany(
            'INSERT INTO moves VALUES (?, ?, ?, ?)',
            db_records,
        )

    print('Creating indexes...')
    if args.create_indexes:
        create_indexes()


if __name__ == '__main__':
    main()
