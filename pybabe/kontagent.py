
from base import BabeBase, StreamHeader, StreamFooter
import datetime
from timeparse import parse_date, parse_datetime
import json
import urllib
from pytz import timezone, utc
from compress_gz import get_content_list 
import cgi
from multiprocessing import Pool

def get_url(date, kwargs):
	kt_user = BabeBase.get_config_with_env("kontagent", "KT_USER", kwargs)
	kt_pass = BabeBase.get_config_with_env("kontagent", "KT_PASS", kwargs)
	kt_appid = BabeBase.get_config_with_env("kontagent", "KT_APPID", kwargs)
	url = 'http://%s:%s@www.kontagent.com/data/raw_data/%s/%04u/%02u/%02u/%02u/?format=json' % (kt_user, kt_pass, kt_appid, date.year, date.month, date.day, date.hour)
	return url 

def change_url(url, kwargs):
	kt_user = BabeBase.get_config_with_env("kontagent", "KT_USER", kwargs)
	kt_pass = BabeBase.get_config_with_env("kontagent", "KT_PASS", kwargs)
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
	while time < end_time: 
		yield time
		time = time + datetime.timedelta(hours=1)

kt_msg = StreamHeader(tyename='ktg_msg', fields=['time',  'param', 'source_ip', 'referer'])
messages = { 
'apa':kt_msg.insert(typename='apa', fields=['u', 's', 'su', 'scheme']), 
'pgr' :  kt_msg.insert(typename='pgr', fields=['s', 'ts', 'u', 'ip', 'fbx_ref', 'fbx_type', 'scheme']),
'cpu' : kt_msg.insert(typename='cpu', fields=['s', 'b', 'g', 'lc', 'ls', 'f', 'scheme']),
'apr' :  kt_msg.insert(typename='apr', fields=['s', 'scheme']),
'evt' : kt_msg.insert(typename='evt', fields=['s', 'n', 'v', 'l', 'st1', 'st2', 'st3', 'scheme']),
'ins' :  kt_msg.insert(typename='ins', fields=['s', 'r', 'u', 'st1', 'st2', 'st3', 'scheme']),
'inr' : kt_msg.insert(typename='inr', fields=['u', 'i', 'r', 'st1', 'st2', 'st3', 'scheme']),
 'pst' : kt_msg.insert(typename='pst', fields=['s', 'u', 'tu', 'st1', 'st2', 'st3', 'scheme']),
 'psr' : kt_msg.insert(typename='psr', fields=['u', 'tu', 'i', 'r', 'st1', 'st2', 'st3', 'scheme']),
 'nes' : kt_msg.insert(typename='nes', fields=['s', 'r', 'u', 'st1', 'st2', 'st3', 'scheme']),
 'nei' :  kt_msg.insert(typename='pgr', fields=['u', 'i', 'r', 'st1', 'st2', 'st3', 'scheme']),
 'mtu' : kt_msg.insert(typename='mtu', fields=['s', 'v', 'tu', 'st1', 'st2', 'st3', 'scheme']),
 'ucc' : kt_msg.insert(typename='ucc', fields=['tu', 'i', 'su', 's', 'st1', 'st2', 'st3', 'scheme']),
 'gci' : kt_msg.insert(typename='gci', fields=['gc1', 'gc2', 'gc3', 'gc4', 'scheme'])
}

def parse_kontagent_file(base_date, buffers, f): 
	for line in f: 
		line_segments = line.split(' ')
		if len(line_segments) != 5: 
			continue
		seconds, msgtype, params, source_ip, referer = line_segments 
		minute=int(seconds)/60
		second=int(seconds)%60
		date = datetime.datetime(base_date.year, base_date.month, base_date.day, base_date.hour, minute, second)
		header= messages[msgtype]
		a = [date, params, source_ip, referer]
		while len(a) < len(header.fields): 
			a.append(None)
		for k, v in cgi.parse_qs(params).items():
			try:
				i = header.fields.index(k)
				a[i] = v[-1]
			except ValueError:
				continue
		buffers[msgtype].append(a)

def read_url(v):
	(hour, file_url) = v
	print hour, file_url
	f = urllib.urlopen(file_url)
	proc  = get_content_list(f, None)[0]
	buffers = {}
	for k in messages:
		buffers[k] = []
	parse_kontagent_file(hour, buffers, proc.stdout)
	proc.wait()
	return buffers

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
	"""
	referent_timezone = BabeBase.get_config_with_env("kontagent", "timezone", kwargs, "utc")
	p = Pool(4)
	for hour in enumerate_period_per_hour(start_time, end_time, referent_timezone): 
		headers = {} 
		buffers = {}
		#print hour
		for k in messages: 
			headers[k] = messages[k].replace(partition=hour.strftime('%Y-%m-%d_%h'))
			buffers[k] = []
		url = get_url(hour, kwargs)
		#print url
		s = urllib.urlopen(url).read()
		#print s
		file_urls = map(lambda url: change_url(url, kwargs), json.loads(s)) 
		if sample_mode and len(file_urls) > 0:
			file_urls = file_urls[:1]
		buffers_list = p.map(read_url, [(hour, url) for url in file_urls])
		for k in headers:
			header = headers[k]
			yield header
			for buffers in buffers_list:
				for a in buffers[k]:
					yield header.t(*a)
			yield StreamFooter()

BabeBase.register("pull_kontagent", pull_kontagent)
