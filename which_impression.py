#! /usr/bin/env python

import MySQLdb, fr_util, sys

def get_options():
    from argparse import ArgumentParser
    parser = ArgumentParser(
        description="Get results for which impression people clicked/donated on."
        )
    parser.add_argument('regex',
        help='Banner regex e.g. B14_0723_mob.')
    parser.add_argument('--format', dest='format', default='tsv',
        choices=['tsv', 'csv', 'pretty', 'mediawiki'],
        help='Output format.')

    if len(sys.argv) == 1:
        # No arguments, show instructions
        parser.print_help()
        sys.exit(1)

    args = vars(parser.parse_args())
    return args


def get_results():
    query = """SELECT
                  utm_key,
                  COUNT(*) AS clicks,
                  SUM( contribution_id IS NOT NULL ) AS donations -- works because true=1, false=0
                FROM drupal.contribution_tracking
                WHERE utm_source REGEXP %(regex)s
                AND ts > '20150701000000'
                GROUP BY utm_key
                ORDER BY cast(utm_key as int)
                """
    cursor.execute(query, args)
    rows = cursor.fetchall()
    return rows


def print_results(description, rows, format):
    if format == 'tsv':
        print '\t'.join([c[0] for c in description])
        for row in rows:
            print '\t'.join([str(cell) for cell in row])

    elif format == 'csv':
        print ', '.join([c[0] for c in description])
        for row in rows:
            print ', '.join([str(cell) for cell in row])

    elif format == 'pretty':
        from prettytable import PrettyTable
        headers = [d[0] for d in description]
        x = PrettyTable(headers)
        x.align = 'r'
        for row in rows:
            x.add_row(row)
        print x

    elif format == 'mediawiki':
        print '{| class="wikitable sortable"'
        print '|+ Caption'
        print '|-'
        for c in description:
            print '! scope="col" | ' + c[0]
        for row in rows:
            print '|-'
            print '! scope="row" | ' + row[0]
            print '| ' + ' || '.join([ str(cell) for cell in row[1:] ])
        print '|}'


if __name__ == "__main__":
    args = get_options()
    cursor = fr_util.get_db_cursor()

    rows = get_results()

    print "Banner regex:\t" + args['regex']
    print_results(cursor.description, rows, args['format'])
