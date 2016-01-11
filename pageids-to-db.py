import csv
import argparse
import pathlib
import sqlite3
import functools
import dateutil.parser

from utils import *

insert_tpl = '''
INSERT INTO Page VALUES (?, ?, ?)
'''

timestamp_parser_cached = functools.lru_cache(100000)(dateutil.parser.parse)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_files',
        nargs='+',
        type=pathlib.Path,
    )
    parser.add_argument(
        'sqlite_file',
    )
    parser.add_argument(
        '--create-tables', '-c',
        action='store_true',
        default=False,
        required=False,
    )
    return parser.parse_args()


def create_tables_and_indexes(connection):
    with connection:
        connection.executescript('''
-- Table: Page
CREATE TABLE IF NOT EXISTS Page (
    "project" TEXT NOT NULL,
    "id" INTEGER NOT NULL,
    "title" TEXT NOT NULL,
    PRIMARY KEY ("project", "id", "title")
);

-- Index: timestamp_asc
CREATE INDEX IF NOT EXISTS title_asc ON Page (
    "project" ASC,
    "title" ASC
);

    ''')

def parse_record(r, default_project=None):
    # no project given
    if len(r) == 2:
        page_id, page_title = r
        project = default_project
    else:
        project, page_id, page_title = r

    return project, page_id, page_title

def main():
    args = parse_args()

    conn = sqlite3.connect(args.sqlite_file)

    if args.create_tables:
        print('Creating tables and indexes')
        create_tables_and_indexes(conn)

    for file_path in args.input_files:
        print('Reading', file_path, '...')
        input_file = open_compressed_file(file_path)
        with input_file, conn:
            csvreader = csv.reader(input_file)
            records = (parse_record(r, 'en') for r in csvreader)

            conn.executemany(insert_tpl, records)


if __name__ == '__main__':
    main()
