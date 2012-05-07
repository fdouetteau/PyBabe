
import time, datetime
from pytz import timezone
from base import StreamMeta, StreamHeader, BabeBase


time_formats = ['%H:%M', '%I:%M%p', '%H', '%I%p', '%I%p%M', '%I:%M:%S']


# Date possible format by order of precedence
date_formats = ['%d %m %Y', '%Y %m %d', '%d %B %Y', '%B %d %Y',
                                                  '%d %b %Y', '%b %d %Y',
                          '%d %m %y', '%y %m %d', '%d %B %y', '%B %d %y',
                                                  '%d %b %y', '%b %d %y']
    
date_time_formats = [ d  + ' ' + t for t in time_formats for d in date_formats]
    
    
def parse_date(string):
    string = string.strip()
    if not string: return None
    
    string = string.replace('/',' ').replace('-',' ').replace(',',' ')
    
    for format in date_formats:
        try:
            result = time.strptime(string, format)
            return datetime.date(result.tm_year, result.tm_mon, result.tm_mday)
        except ValueError:
            pass

    raise ValueError()
    
def parse_datetime(string):
    string = string.strip()
    if not string: return None
    
    string = string.replace('/',' ').replace('-',' ').replace(',',' ')
    
    for format in date_time_formats:
        try:
            result = time.strptime(string, format)
            return datetime.datetime(result.tm_year, result.tm_mon, result.tm_mday, result.tm_hour, result.tm_min)
        except ValueError:
            pass

    raise ValueError(string)


def stream_parse_datetime(stream, field, input_timezone, output_timezone, output_date=None, output_time=None, output_hour=None):
    input_tz = timezone(input_timezone)
    output_tz = timezone(output_timezone)
    header = None
    for row in stream:
        if isinstance(row, StreamHeader):
            added_fields = [f for f in [output_time, output_date, output_hour] if f and not f in row.fields]
            if added_fields:
                header = row.insert(None, added_fields)
            else:
                header = row
            yield header
        elif isinstance(row, StreamMeta):
            yield row 
        else: 
            time_value  = input_tz.localize(parse_datetime(getattr(row, field)))
            time_value_ext = time_value.astimezone(output_tz)
            d = row._asdict()
            if output_date:
                date = datetime.date(time_value_ext.year, time_value_ext.month, time_value_ext.day)
                d[output_date] = date
            if output_time:
                d[output_time] = time_value_ext
            if output_hour:
                d[output_hour] = time_value_ext.hour
            yield header.t(**d)

BabeBase.register("parse_time", stream_parse_datetime)


if __name__ == "__main__":
    print parse_date('2011/04/01')
    print parse_date('01 June 2009')
    print parse_datetime('2011/04/01 03:43')
    print parse_datetime('2011/04/01 3pm45')