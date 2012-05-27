
from base import BabeBase, StreamHeader, StreamFooter
import datetime
from timeparse import parse_date, parse_datetime
import json
import urllib
from pytz import timezone, utc
import cgi
from multiprocessing.dummy import Pool
from geo import get_gic
import logging
from subprocess import Popen, PIPE
import os

def get_url(date, kt_user, kt_pass, kt_appid):
	url = 'http://%s:%s@www.kontagent.com/data/raw_data/%s/%04u/%02u/%02u/%02u/?format=json' % (kt_user, kt_pass, kt_appid, date.year, date.month, date.day, date.hour)
	return url 

def add_password_to_url(url, kt_user, kt_pass):
	return url.replace('www.kontagent.com', '%s:%s@www.kontagent.com' % (kt_user, kt_pass))

def convert_to_datetime(d, referent_timezone):
	if isinstance(d, basestring):
		try: 
			d = parse_datetime(d)
		except ValueError: 
			d = parse_date(d)
	if isinstance(d, datetime.datetime):
		return d
	elif isinstance(d, datetime.date):
		# Convert to a datetime, taking 'referent_timezone in consideration'
		# e.g. if 'CET' is the timezone, the day 2012-01-15 interpreted as '2012-01-15 00:00 CET' hence '2012-01-14 23:00 UTC'
		tz = timezone(referent_timezone)
		ldt = tz.localize(datetime.datetime(d.year, d.month, d.day, 0, 0, 0))
		return ldt.astimezone(utc)

def enumerate_period_per_hour(start_time, end_time, referent_timezone):
	"Provide an hour per hour enumeration of a period of time. Start time and entime can be datetime or date, as objects or strings"
	start_time = convert_to_datetime(start_time, referent_timezone)
	end_time = convert_to_datetime(end_time, referent_timezone)
	time = start_time
	print start_time, end_time
	while time < end_time: 
		print time
		yield time
		time = time + datetime.timedelta(hours=1)

kt_msg = StreamHeader(typename='ktg', fields=[
			'date', 'hour', 'time', 
			'name', 
			# event  name.  VARCHAR(63)
				# Customer event or: 
				# ucc_new_install, ucc_old_install,
				# or [event_type] 
				# or gc1, gc2, gc3, gc4 
			'uid', # user id  that performs the action  BIGINT
				# also 'r' for  responses
			'st1', 'st2', 'st3', 
				# event subtyping  VARCHAR(63)
				# for pgr : 
				#   st1 = parsed referer
				#   st2 = source_ip_country 
				#   st3 = http or https 
				# for cpu
				# 	st1 = gender 
				#   st2 = local country 
				#   st3 = local state (us state)
			'channel_type', # channel type or transaction type VARCHAR(63) 
				# for pgr : fxb_ref or fx_type 
			'value',  # value associated to event (or revenue)  INTEGER
				 # for cpu : 
				 # 	v = number of friends
				 # for gc1, ...
				 #  value for the goal
			'level',  # user level associated to event          INTEGER
				# for cpu
				# 	l = age 
			'recipients', # list of recipients uid, comma separated (ins,nes) VARCHAR(1023)
			'tracking_tag', # unique tracking tag ( also match su : short tracking tag) VARCHAR(63)
			'data', # JSON Payload + additional query parameters not processed VARCHAR(255)
		])

def process_file(base_date, f):
	gic = get_gic()
	t = kt_msg.t 
	for line in f:
		line = line.rstrip('\n')
		line_segments = line.split(' ')
		if len(line_segments) != 5: 
			continue
		seconds, msgtype, params_string, source_ip, referer = line_segments 
		params = {}
		for k, v in cgi.parse_qs(params_string).items():
			params[k] = v[0]
		minute=int(seconds)/60
		second=int(seconds)%60
		time = datetime.datetime(base_date.year, base_date.month, base_date.day, base_date.hour, minute, second)
		date = datetime.date(time.year, time.month, time.day)
		hour = time.hour
		uid = params.get('s', None)
		st1 = params.get('st1', None)
		st2 = params.get('st2', None)
		st3 = params.get('st3', None)
		name = params.get('n', None)
		channel_type = params.get('tu', None)
		value = params.get('v', None)
		level = params.get('l', None)
		recipients = params.get('r', None) 
		tracking_tag = params.get('u', None)
		data = params.get('data', None)
		if not name: 
			name = msgtype 
		if msgtype == "pgr": 
			referer = referer.replace('\"', '')
			if referer == '-':
				referer = None
			if referer:
				srefs = referer.split('/')
				if len(srefs) >= 3:
					if srefs[2]  == "mailing-gift" and len(srefs) > 3:
						st1 = srefs[2] + '/' + srefs[3]
					else:
						st1 = srefs[2]
			if source_ip:
				try:
					st2 = gic.country_code_by_addr(source_ip)
				except :
					pass
			st3 = params.get('scheme', None)
			channel_type = params.get('fbx_type', params.get('fbx_ref', None))
		elif msgtype == 'cpu': 
			st1 = params.get('g', None)
			st2 = params.get('lc', None)
			st3 = params.get('ls', None)
			birth_year = params.get('b', None)
			if birth_year and birth_year.isdigit():
				level = base_date.year - int(birth_year)
			value = params.get('f', None)
		elif msgtype == 'gci': 
			for g in ['gc1', 'gc2', 'gc3', 'gc4']: 
				if g in params: 
					name = g
					value = int(params[g]) 
					break
		elif msgtype == 'ucc': 
			if 'i' in params: 
				name = "ucc_old_install" if params['i'] == '1'  else "ucc_new_install"
		elif msgtype == 'psr':
			uid = params.get('r', None)
			recipients = None 
		if 'su' in params:
			tracking_tag = params['su']
		yield t(date,hour,time,name,uid,
			st1,st2,st3,
			channel_type,value,level,
			recipients,tracking_tag,data)

log = logging.getLogger('kontagent')

def filenameify(url):
	return url.replace('http://', '') 

def read_url_with_cache(url, kt_user, kt_pass, kt_file_cache):
	"Read a kontagent file possibly from a cache (store in dir KT_FILECACHE)"
	f = filenameify(url)
	filepath = os.path.join(kt_file_cache, f)
	if os.path.exists(filepath):
		log.info('Kontagent: cache hit: %s', filepath)
		return filepath 
	else:
		tmpfile = os.path.join(kt_file_cache, str(hash(url)) + '.tmp')
		command = ['wget', '--user', kt_user, '--password', kt_pass, '-q', '-O', tmpfile, url]
		p = Popen(command, stdin=PIPE)
		p.stdin.close()
		p.wait()
		if p.returncode != 0:
			raise Exception('Unable to retrieve %s' % url)
		if not os.path.exists(os.path.dirname(filepath)):
			try:
				os.makedirs(os.path.dirname(filepath)) # ensure base directory exists.
			except OSError, e: 
				if e.errno == 17:  ## File Exists. 
					pass
				else:
					raise e 
		if os.stat(tmpfile).st_size > 0: 
			os.rename(tmpfile, filepath)
			log.info('Kontagent: cache store: %s', filepath)
			return filepath
		else: 
			raise Exception('Failed to retrieve url %s' % url)

def pull_kontagent(nostream, start_time, end_time, sample_mode=False, **kwargs):
	"""
	Generate streams from kontagent logs. 
	Generates a stream per hour and per message type. The streams are outputed per hour. 
	babes = Babe().pull_kontagent_streams(start_time='...', 'end_time='...')
	for babe in babes: 
		babe.push_sql(table='$typename')
	start_time : hour of the first stream to getattr
	end_time : hour of the first stream to getattr
	referent_timezone (optional, default utc): the timezone to use to interpret "day"
	KT_USER : user id
	KT_APPID : id of the app
	KT_PASS : password of the user 
	KT_FILECACHE : local copy of kontagent files. 
	Version 1.1
	"""
	referent_timezone = BabeBase.get_config_with_env("kontagent", "timezone", kwargs, "utc")
	kt_user = BabeBase.get_config_with_env("kontagent", "KT_USER", kwargs)
	kt_pass = BabeBase.get_config_with_env("kontagent", "KT_PASS", kwargs)
	kt_filecache = BabeBase.get_config_with_env(section='kontagent', key='KT_FILECACHE')
	if not os.path.exists(kt_filecache):
		os.makedirs(kt_filecache)
	kt_appid = BabeBase.get_config_with_env("kontagent", "KT_APPID", kwargs)
	for hour in enumerate_period_per_hour(start_time, end_time, referent_timezone): 
		url = get_url(hour, kt_user, kt_pass, kt_appid)
		log.info("Kontagent: retrieving list: %s" % url)
		s = urllib.urlopen(url).read()
		file_urls = json.loads(s)
		if sample_mode and len(file_urls) > 0: # Sample mode: just process the first file. 
			file_urls = file_urls[:1]
		p = Pool(8)
		downloaded_files = p.map(lambda url: read_url_with_cache(url, kt_user, kt_pass, kt_filecache), file_urls)
		p.close()
		header = kt_msg.replace(partition=[("date", datetime.date(hour.year, hour.month, hour.day)), ("hour", hour.hour)])
		yield header
		for f in downloaded_files:
			gzip = Popen(['gzip', '-d', '-c', f], stdin=PIPE, stdout=PIPE)
			for row in process_file(hour, gzip.stdout):
				yield row 
			gzip.stdin.close()
			gzip.wait()
		yield StreamFooter()

BabeBase.register("pull_kontagent", pull_kontagent)
