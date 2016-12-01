#! /usr/bin/env python
"""Fundraising utility belt
"""

import MySQLdb

FORMAT_CHOICES = ['tsv', 'csv', 'pretty', 'mediawiki']

def get_db_cursor(type='default'):
    """Get a cursor to the database

    Type can be:
    - 'default'
    - 'dict': returns rows as a dictionary

    User must have .my.cnf file configured in their home directory
    """
    db = MySQLdb.connect(host='localhost',
                        unix_socket='/tmp/mysql.sock',
                        read_default_file="~/.my.cnf")
    if type == 'dict':
        cursor = db.cursor(MySQLdb.cursors.DictCursor)
    else:
        cursor = db.cursor()

    return cursor


def print_table(columns, rows, format):
    # Put results in a nice table for output

    if format == 'tsv':
        print '\t'.join(columns)
        for row in rows:
            print '\t'.join(row)

    elif format == 'csv':
        print ', '.join(columns)
        for row in rows:
            print ', '.join(row)

    elif format == 'pretty':
        from prettytable import PrettyTable
        x = PrettyTable(columns)
        x.align = 'r'
        for row in rows:
            x.add_row(row)
        print x

    elif format == 'mediawiki':
        print '{| class="wikitable sortable"'
        print '|+ Caption'
        print '|-'
        print '! ' + ' !! '.join( columns )
        for row in rows:
            print '|-'
            print '! scope="row" | ' + row[0]
            print '| ' + ' || '.join( row[1:] )
        print '|}'