#! /usr/bin/env python

from datetime import datetime, timedelta
import dateutil.parser
import MySQLdb, sys
import fr_util

displayFormat = "%Y-%m-%d-%H:%M"
tsFormat      = "%Y%m%d%H%M%S"

def main():
    print """
Campaign filter:   {campaign}
utm_source filter: {source}
Country filter:    {country}
Language filter:   {language}
    """.format(**args)
    num = '{:,}'
    print 'start\tend\timpressions\tclicks\tdonations\tconversion\tamount'
    d = start
    while d <= end:
        d2 = d + delta
        impressions = get_impressions(d, d2)
        clicks, donations, conversion, amount = get_contribs(d, d2)

        print '\t'.join(
            [d.strftime(displayFormat), 
            d2.strftime(displayFormat), 
            num.format(impressions or 0), 
            num.format(clicks or 0), 
            num.format(donations or 0),
            '{:.2%}'.format(conversion or 0),
            '${:,.2f}'.format(amount or 0)])
        d += delta
        

def get_options():
    from argparse import ArgumentParser
    parser = ArgumentParser(description="""Get impression and donation info over time for a campaign.
        Note that old impression queries can be slow, so don't do too many at once.""")

    parser.add_argument('-s', '--start', dest='start', required=True,
        help='Start time (UTC)')
    parser.add_argument('-e', '--end', dest='end', default=datetime.utcnow().strftime('%Y%m%d%H%M%S'), 
        help='End time (UTC). If not specified, defaults to now.')
    parser.add_argument('-i', '--interval', dest='interval', default=24,
        help='Time (in hours) covered per row. Defaults to 24 hours')

    parser.add_argument('--campaign', dest='campaign',   
        help='Regexp filter by campaign name')
    parser.add_argument('--source', dest='source',
        help='Regexp filter by utm_source / banner name')

    parser.add_argument('--country',  dest='country', 
        help='Filter by country code e.g. GB')
    parser.add_argument('--language', dest='language', 
        help='Filter by language code e.g. en')

    if len(sys.argv) == 1:
        # No arguments, show instructions
        parser.print_help()
        sys.exit(1)

    args = vars(parser.parse_args())
    return args

def get_impressions(d, d2):
    query = """SELECT SUM(count) AS ban_imps
        FROM pgehres.bannerimpressions
        LEFT JOIN pgehres.country ON (country_id=country.id)
        LEFT JOIN pgehres.language ON (language_id=language.id)
        WHERE timestamp BETWEEN '{0}' AND '{1}'""".format(d.strftime(tsFormat), d2.strftime(tsFormat))

    if args['campaign']:
        query += "AND campaign REGEXP '{0}'".format(args['campaign'])

    if args['source']:
        query += "AND banner REGEXP '{0}'".format(args['source'])

    if args['country']:
        query += "AND country.iso_code='{0}'".format(args['country'])

    if args['language']:
        query += "AND language.iso_code='{0}'".format(args['language'])

    cursor.execute(query)
    rows = cursor.fetchall()
    return rows[0][0]

def get_contribs(d, d2):
    query = """SELECT 
        COUNT(ct.id) AS clicks,
        SUM(not isnull(cc.id)) AS donations,
        SUM(not isnull(cc.id)) / COUNT(ct.id) AS conversion,
        SUM(total_amount) AS amount
        FROM drupal.contribution_tracking ct
        LEFT JOIN civicrm.civicrm_contribution cc ON ct.contribution_id = cc.id
        WHERE ts BETWEEN '{0}' AND '{1}'""".format(d.strftime(tsFormat), d2.strftime(tsFormat))
    
    if args['campaign']:
        query += "AND utm_campaign REGEXP '{0}'".format(args['campaign'])

    if args['source']:
        query += "AND utm_source REGEXP '{0}'".format(args['source'])

    if args['country']:
        query += "AND ct.country='{0}'".format(args['country'])

    if args['language']:
        query += "AND ct.language='{0}'".format(args['language'])

    cursor.execute(query)
    rows = cursor.fetchall()
    return rows[0]

if __name__ == "__main__":
    args     = get_options()

    start    = dateutil.parser.parse(args['start'])
    end      = dateutil.parser.parse(args['end'])
    delta    = timedelta(hours=int(args['interval']))

    cursor   = fr_util.get_db_cursor(type='default')
    main()
