import xml.etree.ElementTree
import csv
import argparse
import pathlib
import utils
import phpserialize
import gzip
import sys


# http://stackoverflow.com/questions/324214/what-is-the-fastest-way-to-parse-large-xml-docs-in-python/326541#326541
def iter_elems(fileobj, tag):
    it = xml.etree.ElementTree.iterparse(fileobj, events=('start', 'end'))
    context = iter(it)
    event, root = next(context)

    for event, elem in context:
        if event == 'start':
            continue

        if elem.tag == tag:
            yield elem

        root.clear()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_file',
        type=pathlib.Path,
        help='XML file containing page logs',
    )
    return parser.parse_args()


def get_redirect(params: str):
    try:
        params = phpserialize.loads(params.encode('utf-8'))
        redirect = params[b'4::target'].decode('utf-8')
        return redirect
    except ValueError:
        return params


def main():
    args = parse_args()

    move_actions = set(['move', 'move_redir'])

    input_file = utils.open_compressed_file(args.input_file)
    # output_file = gzip.open(str(args.output_file), 'wt', encoding='utf-8')
    output_file = sys.stdout

    with input_file, output_file:
        writer = csv.writer(output_file)
        writer.writerow(('timestamp', 'from', 'to'))

        logitems = iter_elems(
            input_file,
            tag='{http://www.mediawiki.org/xml/export-0.10/}logitem',
        )
        for logitem in logitems:
            action = logitem.find('{http://www.mediawiki.org/xml/export-0.10/}action')

            if action.text not in move_actions:
                continue

            params = logitem.find('{http://www.mediawiki.org/xml/export-0.10/}params')
            logtitle = logitem.find('{http://www.mediawiki.org/xml/export-0.10/}logtitle')

            if params is None or logtitle is None:
                continue

            redirect = get_redirect(params.text)
            timestamp = logitem.find('{http://www.mediawiki.org/xml/export-0.10/}timestamp')

            writer.writerow((
                timestamp.text,
                logtitle.text,
                redirect,
            ))

if __name__ == '__main__':
    main()
