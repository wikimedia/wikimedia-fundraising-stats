#!/usr/bin/env python
"""Check to see if there were impressions within the last hour.
If not, print a warning to stderr."""

from __future__ import print_function
import sys
import MySQLdb, fr_util

interval = "1 hour"
message = "Warning: no bannerimpressions entries in past {}. You should probably check on that.".format(interval)

def main():

    cursor = fr_util.get_db_cursor(type='dict')

    query = """
        SELECT timestamp 
        FROM pgehres.bannerimpressions 
        WHERE timestamp > DATE_SUB( NOW(), INTERVAL {} )
        LIMIT 10;""".format(interval)

    cursor.execute(query)
    data = cursor.fetchall()

    if not data:
        print(message, file=sys.stderr)


if __name__ == '__main__':
    main()
