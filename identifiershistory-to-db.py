import csv
import argparse
import pathlib
import functools
import dateutil.parser
import pymysql
import ipdb

import utils


timestamp_parser_cached = functools.lru_cache(100000)(dateutil.parser.parse)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_files',
        nargs='+',
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
    return parser.parse_args()


def create_tables_and_indexes(cursor):
    with cursor:
        cursor.execute('''
CREATE TABLE IF NOT EXISTS `identifiershistory` (
  `id` INT NOT NULL AUTO_INCREMENT COMMENT '',
  `project` VARCHAR(50) NOT NULL COMMENT '',
  `page_id` INT NOT NULL COMMENT '',
  `page_title` VARCHAR(255) NOT NULL COMMENT '',
  `identifier_type` VARCHAR(20) NOT NULL COMMENT '',
  `identifier_id` VARBINARY(255) NOT NULL COMMENT '',
  `start_date` DATETIME NULL COMMENT '',
  `end_date` DATETIME NULL COMMENT '',
  PRIMARY KEY (`id`)  COMMENT '');
    ''')


def main():
    args = parse_args()

    db_conn = pymysql.connect(**args.mysql_url)

    insert_tpl = '''
    INSERT INTO `identifiershistory` (
        `project`,
        `page_id`,
        `page_title`,
        `identifier_type`,
        `identifier_id`,
        `start_date`,
        `end_date`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''

    if args.create_tables:
        print('Creating tables and indexes')
        create_tables_and_indexes(db_conn.cursor())
        db_conn.commit()

    for file_path in args.input_files:
        print('Reading', file_path, '...')
        input_file = utils.open_compressed_file(file_path)
        cursor = db_conn.cursor()
        with input_file, cursor:
            csvreader = csv.reader(input_file)
            records = (
                utils.parse_identifier_history_record(r)
                for r in csvreader
            )

            records_truncated = (
                (
                    r.project,
                    r.page_id,
                    r.page_title[:255],
                    r.identifier_type[:20],
                    r.identifier_id[:255],
                    r.start_date,
                    r.end_date,
                )
                for r in records
            )

            cursor.executemany(insert_tpl, records_truncated)
    db_conn.commit()

if __name__ == '__main__':
    main()
