
import time, datetime

time_formats = ['%H:%M', '%I:%M%p', '%H', '%I%p', '%I%p%M']


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

    raise ValueError()


if __name__ == "__main__":
    print parse_date('2011/04/01')
    print parse_date('01 June 2009')
    print parse_datetime('2011/04/01 03:43')
    print parse_datetime('2011/04/01 3pm45')