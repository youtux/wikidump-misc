import ipdb
import csv
import argparse
import pathlib
import functools
import dateutil.parser
import pymysql
import collections
import datetime
import ipdb
import frogress

import utils

PapersRecord = collections.namedtuple(
    'PapersRecord',
    (
        'paper_id',
        'original_paper_title',
        'normalized_paper_title',
        'paper_publish_year',
        'paper_publish_date',
        'paper_doi',
        'original_venue_name',
        'normalized_venue_name',
        'journal_id_mapped_to_venue_name',
        'converence_series_id_mapped_to_venue_name',
        'paper_rank',
    ),
)


@functools.lru_cache(1000)
def parse_date(date):
    values = date.split('/')

    if len(values) < 3:
        return None

    try:
        return datetime.date(*map(int, values))
    except ValueError as e:
        print('Warning: parse_date ', date, 'error', e)
        return None


def parse_papers_record(raw_record):
    (
        paper_id,
        original_paper_title,
        normalized_paper_title,
        paper_publish_year,
        paper_publish_date,
        paper_doi,
        original_venue_name,
        normalized_venue_name,
        journal_id_mapped_to_venue_name,
        converence_series_id_mapped_to_venue_name,
        paper_rank,
    ) = raw_record

    if not paper_publish_year:
        paper_publish_year = None
    else:
        paper_publish_year = int(paper_publish_year)

    if not paper_publish_date:
        paper_publish_date = None
    else:
        paper_publish_date = parse_date(paper_publish_date)

    return PapersRecord(
        paper_id,
        original_paper_title,
        normalized_paper_title,
        paper_publish_year,
        paper_publish_date,
        paper_doi,
        original_venue_name,
        normalized_venue_name,
        journal_id_mapped_to_venue_name,
        converence_series_id_mapped_to_venue_name,
        paper_rank,
    )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_csv',
        type=pathlib.Path,
    )
    parser.add_argument(
        'mysql_url',
        type=utils.parse_mysql_url,
        help='MYSQL Database URL',
    )
    parser.add_argument(
        '--create-tables', '-c',
        action='store_true',
        default=False,
        required=False,
    )
    parser.add_argument(
        '--expected-records',
        type=int,
        required=False,
        default=None,
        help='Expected number of record for visualization purposes',
    )
    return parser.parse_args()


def create_tables_and_indexes(cursor):
    with cursor:
        cursor.execute('''
CREATE TABLE IF NOT EXISTS `mag_papers` (
  `paper_id` varchar(50) NOT NULL,
  `original_paper_title` varchar(255) DEFAULT NULL,
  `normalized_paper_title` TEXT DEFAULT NULL,
  `paper_publish_year` int(4) DEFAULT NULL,
  `paper_publish_date` datetime DEFAULT NULL,
  `paper_doi` varchar(255) DEFAULT NULL,
  `original_venue_name` TEXT DEFAULT NULL,
  `normalized_venue_name` TEXT DEFAULT NULL,
  `journal_id_mapped_to_venue_name` varchar(255) DEFAULT NULL,
  `conference_series_id_mapped_to_venue_name` varchar(255) DEFAULT NULL,
  `paper_rank` int(11) DEFAULT NULL,
  PRIMARY KEY (`paper_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''')


def main():
    args = parse_args()

    db_conn = pymysql.connect(**args.mysql_url)

    insert_tpl = '''
    INSERT INTO `mag_papers` (
        `paper_id`,
        `original_paper_title`,
        `normalized_paper_title`,
        `paper_publish_year`,
        `paper_publish_date`,
        `paper_doi`,
        `original_venue_name`,
        `normalized_venue_name`,
        `journal_id_mapped_to_venue_name`,
        `conference_series_id_mapped_to_venue_name`,
        `paper_rank`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    if args.create_tables:
        print('Creating tables and indexes')
        create_tables_and_indexes(db_conn.cursor())
        db_conn.commit()

    print('Reading', args.input_csv, '...')
    input_file = utils.open_compressed_file(args.input_csv)
    cursor = db_conn.cursor()
    with input_file, cursor:
        csvreader = csv.reader(
            input_file,
            delimiter='\t',
            quoting=csv.QUOTE_NONE,
        )
        records = (
            parse_papers_record(r)
            for r in csvreader
        )

        records_truncated = (
            (
                r.paper_id[:50],
                r.original_paper_title[:255],
                r.normalized_paper_title[:255],
                r.paper_publish_year,
                r.paper_publish_date,
                r.paper_doi[:255],
                r.original_venue_name[:255],
                r.normalized_venue_name[:255],
                r.journal_id_mapped_to_venue_name[:255],
                r.converence_series_id_mapped_to_venue_name[:255],
                r.paper_rank,
            ) for r in records
        )

        records_with_progress = frogress.bar(
            records_truncated,
            steps=args.expected_records,
        )
        cursor.executemany(insert_tpl, records_with_progress)
    db_conn.commit()

if __name__ == '__main__':
    main()
