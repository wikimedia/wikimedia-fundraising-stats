#! /usr/bin/env python

from datetime import datetime
import dateutil.parser
import MySQLdb, fr_util

def main():
    rows = get_results()
    print "amount\tdonations\ttotal"
    for row in rows:
        for c in row:
            print c, '\t',
        print

def get_options():
    from argparse import ArgumentParser
    parser = ArgumentParser(description="""Get donations split out by amount for a given currency.""")

    parser.add_argument('currency', help='ISO code of currency to check e.g. GBP')
    parser.add_argument('-s', '--start', dest='start', default='20130701000000',
        help='Start time (UTC). If not specified, defaults to 1 Jul 2013.')
    parser.add_argument('-e', '--end', dest='end', default=datetime.utcnow().strftime('%Y%m%d%H%M%S'), 
        help='End time (UTC). If not specified, defaults to now.')
    parser.add_argument('--sub', dest='substring', 
        help='utm_source substring e.g. B13_0905_badge')
    parser.add_argument('--country', dest='country',
        help='Filter by country (ISO code) e.g. GB')

    args = vars(parser.parse_args())
    return args

def get_results():
    # need to use a subquery here, in order to do calculation using aliases
    query = """
    SELECT 
        e.original_amount AS amount, 
        count(e.id) AS donations, 
        e.original_amount * count(e.id) AS total        
    FROM 
        drupal.contribution_tracking ct
        INNER JOIN civicrm.civicrm_contribution cc ON ct.contribution_id = cc.id
        LEFT JOIN civicrm.wmf_contribution_extra e ON e.entity_id = cc.id
    WHERE
        e.original_currency = %(currency)s
        AND receive_date BETWEEN %(start2)s AND %(end2)s"""

    if args['substring']:
        query += " AND ct.utm_source RLIKE %(substring)s"

    if args['country']:
        query += " AND ct.country=%(country)s"

    query += " GROUP BY e.original_amount ORDER BY e.original_amount"

    cursor.execute(query, args)

    print cursor._last_executed
    
    rows = cursor.fetchall()
    return rows


if __name__ == "__main__":
    args = get_options()

    # have these in the correct format
    args['start2'] = dateutil.parser.parse(args['start'])
    args['end2']   = dateutil.parser.parse(args['end'])

    cursor = fr_util.get_db_cursor()
    main()
