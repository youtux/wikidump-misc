import pathlib
import subprocess
import io
import gzip
import dateutil
import functools
import datetime
import collections
import urllib.parse

IdentifiersHistoryRecord = collections.namedtuple(
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


def open_compressed_file(file_path):
    mode = 'rt'
    encoding = 'utf-8'

    if not isinstance(file_path, pathlib.Path):
        file_path = pathlib.Path(file_path)

    if file_path.suffix == '.7z':
        f = subprocess.Popen(
            ['7z', 'e', '-so', str(file_path)],
            stdout=subprocess.PIPE,
        )
        return io.TextIOWrapper(f.stdout, encoding=encoding)
    elif file_path.suffix == '.gz':
        return gzip.open(str(file_path), mode, encoding=encoding)
    else:
        return file_path.open(mode, encoding=encoding)


def add_utc_if_naive(timestamp: datetime.datetime):
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

    return timestamp


@functools.lru_cache(10000)
def parse_timestamp(timestamp: str):
    timestamp = dateutil.parser.parse(timestamp)

    timestamp = add_utc_if_naive(timestamp)

    return timestamp


def parse_identifier_history_record(raw_record):
    (
        project, page_id, page_title,
        identifier_type, identifier_id,
        start_date, end_date,
    ) = raw_record

    page_id = int(page_id)
    end_date = None if not end_date else parse_timestamp(end_date)
    if not start_date:
        start_date = None
    else:
        start_date = start_date = parse_timestamp(start_date)

    return IdentifiersHistoryRecord(
        project,
        page_id,
        page_title,
        identifier_type,
        identifier_id,
        start_date,
        end_date,
    )


def parse_mysql_url(url):
    db_url = urllib.parse.urlparse(url)
    return dict(
        host=db_url.hostname,
        port=db_url.port or 3306,
        user=db_url.username,
        password=db_url.password or '',
        database=db_url.path.rpartition('/')[-1],
        charset='utf8',
    )
