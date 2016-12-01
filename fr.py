#! /usr/bin/env python
from datetime import datetime, timedelta
import MySQLdb
from itertools import *
from operator import *
import sys
import re

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

def str_time_offset(str_time, offset = 1):
    time_time = datetime.strptime( str_time, '%Y%m%d%H%M%S' )
    str_time = ( time_time + timedelta( hours = offset )).strftime( '%Y%m%d%H%M%S' )
    return(str_time)

def str_now():
    return( datetime.now().strftime('%Y%m%d%H%M%S') )

def datetimefunix( unix_timestamp ):
    return datetime.fromtimestamp(unix_timestamp)

def strfunix( unix_timestamp ):
    return datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M')

def get_cursor(type='dict'):
    conn = MySQLdb.connect(host='localhost',
                            db='civicrm',
                            unix_socket='/tmp/mysql.sock',
                            read_default_file="~/.my.cnf")
    if type == 'dict':
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    elif type == 'norm':
        cursor = conn.cursor()
    return cursor

def count_symbol( c ):
    char = ""
    if c == 0: char = " "
    elif c == 1: char = u"\u2802"
    elif c == 2: char = u"\u2803"
    elif c == 3: char = u"\u2807"
    elif c == 4: char = u"\u280F"
    elif c == 5: char = u"\u281F"
    elif c == 6: char = u"\u283F"
    elif c == 7: char = u"\u287F"
    elif c == 8: char = u"\u28FF"
    elif c >= 9 and c < 50: char = u"\u2169"
    elif c >= 50 and c < 100: char = u"\u216C"
    elif c >= 100 and c < 500: char = u"\u216D"
    elif c >= 500 and c < 1000: char = u"\u216E"
    elif c >= 1000 and c < 5000: char = u"\u216E"
    elif c >= 5000 and c < 10000: char = u"\u2181"
    elif c >= 10000: char = u"\u2182"
    else: char = "?"
    return( char )

class Campaign:
    def __init__(self, utm_campaign, start=None, end=None,
            interval=1, country=None, language=None, multicountry=None):
        self.utm_campaign = utm_campaign
        self.interval = interval
        query = '''select
            min(ts) cstart, max(ts) cend
            from drupal.contribution_tracking
            where '''
        if start != None and end != None:
            query += " ts between '%s' and '%s' and  " % (start, end)
        query += "utm_campaign like '%s%%';" % ( utm_campaign )
        cursor = get_cursor()
        cursor.execute(query)
        row = cursor.fetchone()
# find the outermost book ends of the campign
        self.first_don = row['cstart']
        self.last_don = row['cend']
        if start == None and end == None:
            start = self.first_don
            end = self.last_don
            self.start = start
            self.end = end
        query = '''select
            unix_timestamp(ts) div (60*%s) minit,
            count(*) cnt
            from drupal.contribution_tracking
            where ''' % interval
        if start != None and end != None:
            query += " ts between '%s' and '%s' and  " % (start, end)
        query += '''utm_campaign like '%s%%'
        group by unix_timestamp(ts) div (60*%s) having count(*) > 0;''' % (
        utm_campaign,  interval)
        cursor.execute(query)
        self.minits = cursor.fetchall() # making the results available in case useful
# list of all the minit values, and then don counts for each
        self.minit_list = []
        self.minit_counts = {}
        for row in self.minits:
            self.minit_list.append(row['minit'])
            self.minit_counts[row['minit']] = row['cnt']
# list of lists of consequtive blocks of minits (i.e. when the camp was on)
        self.runs = []
        for k, g in groupby(enumerate(self.minit_list), lambda (i,x):i-x):
            self.runs.append( map(itemgetter(1), g) )
        self.runs_by_len = sorted(self.runs, key=len)
# lists of the hours and days over which a campaign ran
        query = '''select
        left(ts,8) dy,
        left(ts,10) hr,
        count(*) cnt
        from drupal.contribution_tracking
        where ts between '%s' and '%s' and  utm_campaign like '%s%%'
        group by 1 ,2
        order by 2;''' % (start, end, utm_campaign)
        cursor.execute(query)
        rs = cursor.fetchall()
        self.days = sorted(set([ row['dy'] for row in rs ]))
        self.hours = sorted(set([ row['hr'] for row in rs ]))
        self.day_counts = {}
        self.hour_counts = {}
        for row in rs:
            self.day_counts[row['dy']] = row['cnt']
        for row in rs:
            self.hour_counts[row['hr']] = row['cnt']

    def show_days(self, single_day = None):
        print "         +-----------+-----------+-----------+-----------+"
        if single_day == None:
            days = self.days
        else:
            days = [single_day]
        for dy in days:
            print dy,
            for hr in range(0,23):
                strhr = "%s%02d" % (dy, hr)
                if dy in strhr:
                    if strhr in self.hour_counts.keys():
                        print count_symbol(self.hour_counts[strhr]),
                    else:
                        print " ",
            print

    def show_hours(self, single_hour = None):
        cursor = get_cursor()
        if self.interval == 1:
            print "           --------+--------(1)--------+--------(2)--------+--------(3)--------+--------(4)--------+--------(5)--------+--------(6)"
        elif self.interval == 5:
            print "           ---+---+---+---+---+---+"
        for hr in self.hours:
            print "%s %02d " % ( hr[2:8], int(hr[8:10]) ),
            hour_start_minit =  int(datetime.strptime(hr, "%Y%m%d%H").strftime("%s")) // (60*self.interval)
            hour_end_minit =  ( int(datetime.strptime(hr, "%Y%m%d%H").strftime("%s"))+60*60) // (60*self.interval)
            query = '''select
            unix_timestamp(ts) div (60*%s) minit,
            count(*) cnt
            from drupal.contribution_tracking
            where ts between '%s0000' and '%s5959' and  utm_campaign like '%s%%'
            group by unix_timestamp(ts) div (60*%s)
            having count(*) > 0;''' % (self.interval, hr, hr, self.utm_campaign, self.interval)
            cursor.execute(query)
            rs = cursor.fetchall()
            minits_in_hour = dict([(row['minit'], row['cnt']) for row in rs ])
            for minit in range(hour_start_minit, hour_end_minit):
                if minit in minits_in_hour:
                    print count_symbol( int(self.minit_counts[minit]) ),
                else:
                    print " " ,
            print

def show_campaign_months(start=None, end=None, country=None, language=None, multicountry=None):
    query = '''select
        if(utm_campaign regexp '_[A-Z]{2}$' and utm_campaign not like '%%_FR',
        left(utm_campaign,length(utm_campaign)-3), utm_campaign) as camp,
        left(ts,8) day,
        count(*) cnt
        from drupal.contribution_tracking
        where ts between '%s' and '%s'
        group by camp, day
        having count(*) > 10
        order by camp, day
    ''' % ( start, end )
    cursor = get_cursor()
    cursor.execute(query)
    rs = cursor.fetchall()
    dist_camps =  sorted(set([row['camp'] if row['camp'] != None
        else 'Null' for row in rs]))
    dist_days = sorted(set([row['day'] for row in rs]))
    camp_name_width = len(max(dist_camps, key=len))
    if camp_name_width > 21: camp_name_width = 21
    print "%*s +%s%s%s+" % (26, " ", dist_days[0]
            ,"-"*((len(dist_days)*2)-18), dist_days[-1])
    for c in dist_camps:
        if len(c) > 20:
            short_name = "%s*" % c[:19]
        else:
            short_name = c
        print "%3s) %*s" % (dist_camps.index(c),
                camp_name_width, short_name),
        for dy in dist_days:
            cc = count_from_camp_day(rs, c, dy)
            if cc:
                print count_symbol(cc),
            else:
                print " ",
        print
    return dist_camps

def show_campaign_days2(start=None, end=None,
        country=None, language=None, multicountry=None):
    query = '''select if(utm_campaign regexp '_[A-Z]{2}$' and utm_campaign not like '%%_FR',
    left(utm_campaign,length(utm_campaign)-3), utm_campaign) as camp,
    left(ts,10) hour,
    count(*) cnt
    from drupal.contribution_tracking
    where ts between '%s' and '%s'
    group by camp, hour
    having count(*) > 10
    order by camp, hour
    ''' % ( start, end )
    cursor = get_cursor()
    cursor.execute(query)
    rs = cursor.fetchall()
    dist_camps =  sorted(set([row['camp'] if row['camp'] != None else 'Null' for row in rs]))
    dist_days = sorted(set([row['hour'][0:8] for row in rs]))
    dist_hours = sorted(set([row['hour'] for row in rs]))
    camp_name_width = len(max(dist_camps, key=len))
    print "%*s +%s%s%s+" % (29, " ", dist_hours[0]
            ,"-"*((len(dist_hours)*2)-22), dist_hours[-1])
    for c in dist_camps:
        print "%3s) %*s" % (dist_camps.index(c), camp_name_width, c),
        for dy in dist_days:
            for hr in range(0,23):
                strhr = "%s%02d" % (dy, hr)
                if strhr < dist_hours[0]: continue
                if dy in strhr:
                    cc = count_from_camp_hour(rs, c, strhr)
                    if cc:
                        print count_symbol(cc),
                    else:
                        print " ",
                    if strhr == dist_hours[-1]: break
        print
    return dist_camps

def show_campaign_days(start=None, end=None, thing_counted = 'donations',
        country=None, language=None, multicountry=None):
    if thing_counted == 'donations':
        query = '''select if(utm_campaign regexp '_[A-Z]{2}$' and utm_campaign not like '%%_FR',
                left(utm_campaign,length(utm_campaign)-3), utm_campaign) as camp,
                left(ts,10) hour,
                count(*) cnt
                from
                drupal.contribution_tracking ct
                left join civicrm.civicrm_contribution cc on ct.contribution_id = cc.id
                left join civicrm.civicrm_contact cn on cn.id = cc.contact_id
                left join civicrm.civicrm_address ca on cc.contact_id = ca.contact_id
                left join civicrm.civicrm_state_province sp on ca.state_province_id = sp.id
                left join civicrm.civicrm_country co on ca.country_id = co.id
                left join civicrm.civicrm_email ce on ce.contact_id = cc.contact_id
                where ts between '%s' and '%s'
                group by camp, hour
                having count(*) > 10
                order by camp, hour
                ''' % ( start, end )
    if thing_counted == 'clicks':
        query = '''select if(utm_campaign regexp '_[A-Z]{2}$' and utm_campaign not like '%%_FR',
                left(utm_campaign,length(utm_campaign)-3), utm_campaign) as camp,
                left(ts,10) hour,
                count(*) cnt
                from drupal.contribution_tracking
                where ts between '%s' and '%s'
                group by camp, hour
                having count(*) > 10
                order by camp, hour
                ''' % ( start, end )
    cursor = get_cursor()
    cursor.execute(query)
    rs = cursor.fetchall()
    dist_camps =  sorted(set([row['camp'] if row['camp'] != None else 'Null' for row in rs]))
    dist_days = sorted(set([row['hour'][0:8] for row in rs]))
    camp_name_width = len(max(dist_camps, key=len))
    print "%*s +%s%s%s+" % (camp_name_width+3, " ", dist_days[0]
            ,"-"*((len(dist_days)*2*24)-27), dist_days[-1])
    for c in dist_camps:
        print "%s) %*s" % (dist_camps.index(c), camp_name_width, c),
        for dy in dist_days:
            for hr in range(0,23):
                strhr = "%s%02d" % (dy, hr)
                if dy in strhr:
                    cc = count_from_camp_hour(rs, c, strhr)
                    if cc:
                        print count_symbol(cc),
                    else:
                        print " ",
        print
    return dist_camps

def count_from_camp_day(rs,camp,day):
    for row in rs:
        if row['camp'] == camp and row['day'] == day:
            return row['cnt']
    return None

def count_from_camp_hour(rs,camp,hour):
    for row in rs:
        if row['camp'] == camp and row['hour'] == hour:
            return row['cnt']
    return None

def get_group_by(group_by,optdict,semi=None):
    query = "group by "
    for c in group_by:
        if c == 'b': 
            if optdict['left']:
                query += "left(src_banner,%s), " % optdict['left']
            else:
                query += "src_banner, "
        if c == 'l': query += "landingpage, "
        if c == 'p': query += "src_payment_method, "
        if c == 'f': query += "payments_form, "
        if c == 'c': query += "country, "
    query = query.rstrip(', ')
    if semi:
        query += semi
    return query

def get_on(group_by,semi=None):
    query = "on "
    for c in group_by:
        if c == 'b': query += "ecom.banner=lps.banner and "
        if c == 'l': query += "ecom.landingpage=lps.landingpage and "
        if c == 'p': query += "ecom.payment_method=lps.payment_method and "
        if c == 'c': query += "convert(ecom.iso_code using utf8) =convert(lps.iso_code using utf8) and "
    query = query[:-4]
    if semi:
        query += semi
    return query

def get_amts_query(optdict, bans):
    start = optdict['start']
    end = optdict['end']
    campaign = optdict['campaign']
    query = '''select
if( utm_source regexp '%s','%s','%s') as banner,
total_amount as amount
from
drupal.contribution_tracking ct
join civicrm.civicrm_contribution cc on ct.contribution_id = cc.id
where ts >= '%s' and ts < '%s'
-- and utm_campaign regexp '%s'
and (utm_source regexp '%s' or utm_source regexp '%s')
''' % ( bans[0], bans[0], bans[1], start, end, campaign, bans[0], bans[1])
    return query

def utest(optdict,bans):
    import rpy2.robjects as robjects
    query = get_amts_query(optdict, bans)
    rs_string, rs = get_rs(query, optdict['raw'])
    a1 = []
    a2 = []
    r = robjects.r
    # as_numeric = r['as.numeric']
    for row in rs:
        if row['banner'] == bans[1]:
            a1.append( row['amount'] )
        else:
            a2.append( row['amount'] )
    v1 = robjects.FloatVector(a1)
    v2 = robjects.FloatVector(a2)
    wilcox_result =  r['wilcox.test'](v1, v2)
    print  "Wilcox test of means p= %.6f" % wilcox_result[2][0]
    # print re.sub('data:.*\n','',wilcox_result)

def get_test_query(optdict, type="donations"):
    start = optdict['start']
    end = optdict['end']
    campaign = optdict['campaign']
    query =""

#wrapper around basic query for the bigger impression query
    if type == "donations-impressions":
        query += '''select lps.lpi, ecom.* 
        from
        (select '''
        if 'group_by' in optdict.keys():
            display_cols = optdict['group_by']
            if display_cols:
                for c in display_cols:
                    if c == 'b':
                        if optdict['multicountry']:
                            query += "left(utm_source, length(utm_source)-3) as banner, "
                        else:
                            query += "utm_source as banner, "
                    if c == 'l': 
                        query += "landingpage, "
                    if c == 'c': query += "ct.country, "
        query += ("count(*) as lpi "
            "from pgehres.landingpageimpression_raw l "
            "left join pgehres.country co on l.country_id=co.id "
            "where timestamp between '%s' and '%s' ") % (start, end)
        if campaign != "None":
            query += "and  utm_campaign regexp '%s' " % (campaign)
        if optdict['substring']:
            query += "and bis.banner regexp '%s' " % (optdict['substring'])
# group by
        if optdict['group_by']:
            query += get_group_by(optdict['group_by'],optdict)
        query += ") lps left join ("

# donations part starts here
# columns to display
    query += "select "
    if 'group_by' in optdict.keys():
        display_cols = optdict['group_by']
        if display_cols:
            for c in display_cols:
                if c == 'b': # TODO: change these to use new ct banner column when it's introduced
                    if optdict['left']:
                        query += "left(SUBSTRING_index(substring_index(ct.utm_source, '.', 2),'.',1),%s) as src_banner, " % optdict['left'] 
                    elif optdict['multicountry']:
                        query += "left(SUBSTRING_index(substring_index(ct.utm_source, '.', 2),'.',1), length(SUBSTRING_index(substring_index(ct.utm_source, '.', 2),'.',1))-3) as src_banner, "
                    else:
                        query += "SUBSTRING_index(substring_index(ct.utm_source, '.', 2),'.',1) as src_banner, "
                if c == 'l': query += "SUBSTRING_index(substring_index(ct.utm_source, '.', 2),'.',-1) as landingpage, "
                if c == 'p': query += "substring_index(ct.utm_source, '.', -1) as src_payment_method, " # TODO: change to use new ct payment_method column when it's introduced
                if c == 'f': query += "payments_form as form, "
                if c == 'c': query += "ct.country as country, "
                if c == 't': query += "utm_campaign, "
    query += '''sum(not isnull(cc.id)) as donations,
            count(ct.id) as clicks,
            -- sum(if(trxn_id like "GLOBALCOLLECT%%" or trxn_id like "PAYFLOW%%",1,0)) as cc,
            -- sum(if(trxn_id like "PAYPAL%%",1,0)) as pp,
            concat(round(sum(if(trxn_id like "GLOBALCOLLECT%%" or trxn_id like "PAYFLOW%%" or trxn_id like "ADYEN%%" or trxn_id like "WORLDPAY%%",1,0))/sum(not isnull(cc.id))*100),"%") as ccpct,
            -- count(ct.id) as clicks,
            -- sum(if(utm_source regexp ".cc",1,0)) as cc,
            -- sum(if(utm_source regexp ".pp" or utm_source regexp ".paypal",1,0)) as pp,
            concat(round(sum(if(trxn_id like "GLOBALCOLLECT%%" or trxn_id like "PAYFLOW%%" or trxn_id like "ADYEN%%" or trxn_id like "WORLDPAY%%",1,0)) / sum(if(utm_source like "%.cc",1,0))*100),"%") as cccnv,
            concat(round(sum(if(trxn_id like "PAYPAL%%",1,0)) / sum(if(utm_source like "%.pp" or utm_source like "%.paypal",1,0))*100),"%") as ppcnv,
            concat(round(sum(if(trxn_id like "AMAZON%%",1,0)) / sum(if(utm_source like "%.amazon" ,1,0))*100),"%") as azcnv,
            sum(total_amount) AS amount,
            sum(if(total_amount > 3, 3, total_amount)) AS amount3,
            sum(if(total_amount > 5, 5, total_amount)) AS amount5,
            sum(if(total_amount > 20, 20, total_amount)) AS amount20,
            sum(if(total_amount > 50, 50, total_amount)) AS amount50,
            max(total_amount) as max,
            avg(total_amount) as avg,
            avg(if(total_amount > 20, 20, total_amount)) as avg20,
            std(total_amount) as stdev
            '''
# join
    query += '''
            from
            drupal.contribution_tracking ct
            left join civicrm.civicrm_contribution cc on ct.contribution_id = cc.id
            left join civicrm.civicrm_address ca on cc.contact_id = ca.contact_id
            left join civicrm.civicrm_country co on ca.country_id = co.id
            where ts >=  '%s' and ts < '%s'
            and (ca.is_primary=1 OR ca.is_primary IS NULL) -- don't double count extra addresses
            ''' % ( start, end )
# where
    if optdict['campaign'] != 'None':
        if optdict['campaign'] and optdict['multicountry']:
            query += "and utm_campaign regexp '%s' " % campaign
        elif optdict['campaign']:
            query += "and utm_campaign = '%s' " % campaign
    if optdict['country']:
        query += " and ct.country='%s' " % (optdict['country'])
    if optdict['language']:
        query += "and ct.language = '%s' " % (optdict['language'])
    if optdict['medium']:
        query += " and utm_medium = '%s' " % optdict['medium']
    if optdict['substring']:
        query += " and utm_source  regexp '%s' " % (optdict['substring'])

# group by
    if optdict['group_by']:
        query += get_group_by(optdict['group_by'],optdict)
# having
    if optdict['having']:
        query += " having donations > %s " % optdict['having']
# order by
    if optdict['order_by']:
        query += " order by "
        for c in optdict['order_by']:
            if c == 'l': query += "landingpage, "
            if c == 'b': query += "src_banner, "
            if c == 'p': query += "src_payment_method, "
            if c == 'c': query += "country, "
            if c == 'd': query += "donations desc, "
            if c == 'i': query += "lpi desc, "
            if c == 'g': query += "language, "
            if c == 'f': query += "payments_form, "
            if c == 't': query += "campaign, "
        query = query[0:-2]

# end of wrapper for larger impressions query
    if type == "donations-impressions":
        query += ") ecom "
        if optdict['group_by']:
            if 'l' in optdict['group_by'] and 'b' in optdict['group_by']:
                query += "on ecom.landingpage=lps.landingpage and ecom.banner=lps.banner "
            elif 'l' in optdict['group_by']: 
                query += "on ecom.landingpage=lps.landingpage "
            elif 'b' in optdict['group_by']: 
                query += "on ecom.banner=lps.banner "
        elif optdict['campaign'] == 'None':
            query += "on True "
        query += "order by lpi desc"

    return query

def donations(optdict):
    query = get_test_query(optdict,"donations-impressions" if optdict['impressions'] else "donations")
# output sql if requested
    if optdict['sqlonly']:
        print query
# output results
    rs_string, rs = get_rs(query, optdict['raw'])
    print rs_string
# link(s) to thumbtack tool
    do_confidence(optdict,rs)
# ban imps
    if optdict['impressions']:
        print_ban_imps(optdict)
# u.test
    if len(rs) != 2:
        print "\nu test only possible with 2 banners -- use --having to narrow it down."
    else:
        bans = []
        bans.append( rs[0]['src_banner'] )
        bans.append( rs[1]['src_banner'] )
        utest(optdict,bans)
def do_confidence(optdict,rs):
    if optdict['group_by']:
        if optdict['group_by'][0]=='l':
            print "d/clicks confidence test:"
            tt_query_string =  "&".join(
                    [ ( "%s=%s%%2C%s" % ( row['landingpage'] ,
                        row['donations'], row['clicks']) ) for row in rs ] )
            tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                    "&abba%%3AintervalConfidenceLevel=0.95&"
                    "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
            print tt_url
            print "d/bi confidence test:"
            tt_query_string =  "&".join(
                    [ ( "%s=%s%%2C%s" % ( row['landingpage'] ,
                        row['donations'], 1000000) ) for row in rs ] )
            tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                    "&abba%%3AintervalConfidenceLevel=0.95&"
                    "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
            print tt_url
            if optdict['impressions']:
                print "d/lpi confidence test:"
                tt_query_string =  "&".join(
                        [ ( "%s=%s%%2C%s" % ( row['landingpage'] ,
                            row['donations'], row['lpi']) ) for row in rs ] )
                tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                        "&abba%%3AintervalConfidenceLevel=0.95&"
                        "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
                print tt_url
                print "lpi/bi confidence test:"
                tt_query_string =  "&".join(
                        [ ( "%s=%s%%2C%s" % ( row['landingpage'] ,
                            row['lpi'], 1000000) ) for row in rs ] )
                tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                        "&abba%%3AintervalConfidenceLevel=0.95&"
                        "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
                print tt_url

        elif optdict['group_by'][0]=='b':
            print "d/bi confidence test:"
            tt_query_string =  "&".join(
                    [ ( "%s=%s%%2C%s" % ( row['src_banner'] ,
                        row['donations'], 1000000) ) for row in rs ] )
            tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                    "&abba%%3AintervalConfidenceLevel=0.95&"
                    "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
            print tt_url
            print "clicks/bi confidence test:"
            tt_query_string =  "&".join(
                    [ ( "%s=%s%%2C%s" % ( row['src_banner'] ,
                        row['clicks'], 1000000) ) for row in rs ] )
            tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                    "&abba%%3AintervalConfidenceLevel=0.95&"
                    "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
            print tt_url
            if optdict['impressions']:
                print "d/lpi confidence test:"
                tt_query_string =  "&".join(
                        [ ( "%s=%s%%2C%s" % ( row['src_banner'] ,
                            row['donations'], row['lpi']) ) for row in rs ] )
                tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                        "&abba%%3AintervalConfidenceLevel=0.95&"
                        "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
                print tt_url
                print "lpi/bi confidence test:"
                tt_query_string =  "&".join(
                        [ ( "%s=%s%%2C%s" % ( row['src_banner'] ,
                            row['lpi'], 1000000) ) for row in rs ] )
                tt_url = ( "http://www.thumbtack.com/labs/abba/#%s"
                        "&abba%%3AintervalConfidenceLevel=0.95&"
                        "abba%%3AuseMultipleTestCorrection=true" % tt_query_string )
                print tt_url
        else:
            print "Confidence tests only working now with banner or landing page grouping."

def get_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-d", "--display_cols", dest="display_cols",
                    help="Fields to display. b=ban, l=lp, p=pay method, c=country, g=language, t=campaign (test)", metavar="blp")
    parser.add_option("--having", dest="having",
                    help="Only groups having more than n donations", metavar="10")
    parser.add_option("-g", "--group_by", dest="group_by",
                    help="Fields to group by. b=ban, l=lp, p=pay method, c=country, g=language, t=campaign (test)", metavar="blp")
    parser.add_option("-o", "--order_by", dest="order_by",
                    help="Fields to order by. d=donations, i=lp impressions, b=ban, l=lp, p=pay method, c=country, g=language, t=campaign (test)", metavar="blp")
    parser.add_option("-c", "--campaign", dest="campaign", default = "None",
                    help="utm_campaign, aka campaign", metavar="UTM_CAMPAIGN")
    parser.add_option("-s", "--start", dest="start",
                    help="test start time", metavar="YYYYMMDDHHMMSS")
    parser.add_option("-e", "--end", dest="end",
                    help="test end time", metavar="YYYYMMDDHHMMSS")
    parser.add_option("--bi", "--ban impressions", dest="ban_imps", action="store_true", default=False, 
                    help="Show banner impressions too (only accurate about 15 minutes after test)" )
    parser.add_option("-i", "--impressions", dest="impressions", action="store_true", default=False, 
                    help="Show impressions too (only accurate about 10 minutes after test)" )
    #parser.add_option("--gc", "--gcountry", dest="gcountry", action="store_true", default=False, 
    #                help="Group results by country" )
    parser.add_option("--sqlonly",
                    action="store_true", dest="sqlonly", default=False,
                    help="Print sql, not results.")
    parser.add_option("--raw",
                    action="store_true", dest="raw",
                    help="Display raw output (may be easier to paste to spreadsheet.")
    parser.add_option("--substring", dest="substring", 
                    help="substring", metavar="")
    parser.add_option("--left", dest="left", 
                    help="left", metavar="")
    parser.add_option("--language", dest="language", 
                    help="language", metavar="en")
    parser.add_option("--country", dest="country", 
                    help="country", metavar="US")
    parser.add_option("--medium", dest="medium", 
                    help="medium", metavar="sitenotice")

    #parser.add_option("-b", "--gbanner",
    #                action="store_true", dest="gbanner", default=False,
    #                help="Group by banner.")
    #parser.add_option("-l", "--glandingpage",
    #                action="store_true", dest="glandingpage", default=False,
    #                help="Group by landing page.")
    #parser.add_option("-p", "--gpayment_method",
    #                action="store_true", dest="gpayment_method", default=False,
    #                help="Group by payment method.")
    parser.add_option("-m", "--multicountry",
                    action="store_true", dest="multicountry", default=False,
                    help="Remove country suffix of banner names to group by banner instead of banner+country.")

    if len(sys.argv) == 1:
        # No arguments, show instructions
        parser.print_help()
        sys.exit(1)

    (options, args) = parser.parse_args()

    if options.campaign and (not options.start or not options.end):
        with open('campaign_times.tsv') as f:
            for line in f.readlines():
                c, s, e = line.split()
                if c == options.campaign:
                    if not options.start: options.start = s
                    if not options.end: options.end = e
    optdict = vars(options)
    if  options.impressions:
        if options.group_by:
            if 'f' in options.group_by:
                print "Can't group on 'form' while getting impressions."
                sys.exit()
    return optdict

def print_ban_imps(optdict):
    start = optdict['start']
    end = optdict['end']
    campaign = optdict['campaign']
    query ="select "
    if optdict['group_by']:
        for c in optdict['group_by']:
            if c == 'b': query += "banner, "
#            if c == 'l': query += "landing_page, "
            if c == 'c': query += "country, "
    query += '''sum(counts)*100 as ban_imps
    from faulkner.banner_impressions_2012
    where on_minute > '%s' and on_minute < '%s' 
    ''' % (start, end)
    if optdict['campaign'] != 'None':
        if optdict['campaign']:
            query += "and campaign = '%s' " % campaign
    if optdict['group_by'] and optdict['group_by'] != 'l':
        query += "group by "
        for c in optdict['group_by']:
            if c == 'b': query += "banner, "
#            if c == 'l': query += "landing_page, "
            if c == 'c': query += "country, "
        query = query.rstrip(', ')
    rs_string, rs = get_rs(query)
    print rs_string

def get_rs(query, raw = None):
    from prettytable import PrettyTable
    cursor = get_cursor("dict")
    cursor.execute(query)
    rs = cursor.fetchall()
    rs_string = ""
    if not raw:
        x = PrettyTable([i[0] for i in cursor.description])
        for row in rs:
            x.add_row([row[i[0]] for i in cursor.description])
        rs_string = x
    else:
        headers = [i[0] for i in cursor.description]
        for h in headers:
            rs_string += "%s\t" % h
        rs_string += "\n" 
        for row in rs:
            the_row = [row[i[0]] for i in cursor.description]
            for col in the_row:
                rs_string += "%s\t" % col
            rs_string += "\n"
    return rs_string, rs
