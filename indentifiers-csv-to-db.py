import csv
import argparse
import pathlib
import sqlite3
import subprocess
import io
import gzip
import functools
import dateutil.parser

insert_tpl = '''
INSERT INTO identifiers_history VALUES (?, ?, ?, ?, ?, ?, ?)
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
        '--create-tables',
        action='store_true',
        default=False,
        required=False,
    )
    return parser.parse_args()


def open_file(file_path):
    mode = 'rt'
    encoding = 'utf-8'
    if file_path.suffix == '.7z':
        f = subprocess.Popen(
            ['7z', 'e', '-so', str(file_path)],
            stdout=subprocess.PIPE,
        )
        w = io.TextIOWrapper(f.stdout, encoding=encoding)
        return w
    elif file_path.suffix == '.gz':
        return gzip.open(str(file_path), mode, encoding=encoding)
    else:
        return file_path.open(mode, encoding=encoding)

def create_tables_and_indexes(connection):
    with connection:
        connection.executescript('''
-- Table: identifiers_history
CREATE TABLE IF NOT EXISTS identifiers_history (
    identifier_type TEXT NOT NULL,
    identifier_id TEXT NOT NULL,
    "action" INTEGER NOT NULL,
    timestamp DATETIME NOT NULL,
    project TEXT NOT NULL,
    page_id INTEGER NOT NULL,
    revision_id INTEGER NOT NULL,
    PRIMARY KEY (identifier_type, identifier_id, "action", timestamp, project, page_id, revision_id)
);

-- Index: timestamp_asc
CREATE INDEX IF NOT EXISTS timestamp_asc ON identifiers_history (timestamp ASC);

-- Index: identifier_asc
CREATE INDEX IF NOT EXISTS identifier_asc ON identifiers_history (
    identifier_type ASC,
    identifier_id ASC
);

-- Index: page_revision_asc
CREATE INDEX IF NOT EXISTS project_page_revision_asc ON identifiers_history (
    project ASC,
    page_id ASC,
    revision_id ASC
);
    ''')

def parse_record(r, default_project=None):
    # no project given
    if len(r) == 6:
        identifier_type, identifier_id, action, timestamp, page_id, revision_id = r
        project = default_project
    else:
        identifier_type, identifier_id, action, timestamp, project, page_id, revision_id = r

    timestamp = timestamp_parser_cached(timestamp)
    return identifier_type, identifier_id, action, timestamp, project, page_id, revision_id

def main():
    args = parse_args()

    conn = sqlite3.connect(args.sqlite_file)

    if args.create_tables:
        print('Creating tables and indexes')
        create_tables_and_indexes(conn)

    for file_path in args.input_files:
        print('Reading', file_path, '...')
        input_file = open_file(file_path)
        with input_file, conn:
            csvreader = csv.reader(input_file)
            records = (parse_record(r, 'en') for r in csvreader)

            conn.executemany(insert_tpl, records)


if __name__ == '__main__':
    main()
