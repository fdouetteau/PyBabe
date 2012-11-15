
from base import BabeBase, StreamHeader, StreamMeta
import os


gic = None


def get_gic():
    global gic
    if gic == None:
        if os.path.exists('/usr/share/GeoIP/GeoIP.dat'):
            default = "/usr/share/GeoIP/GeoIP.dat"
        elif os.path.exists("/usr/local/share/GeoIP/GeoLiteCity.dat"):
            default = "/usr/local/share/GeoIP/GeoLiteCity.dat"
        elif os.path.exists("/usr/local/var/lib/GeoLiteCity.dat"):
            default = "/usr/local/var/lib/GeoLiteCity.dat"
        else:
            default = None
        filename = BabeBase.get_config_with_env('geoip', 'GEOIP_FILE', {}, default)
        from pygeoip import GeoIP
        gic = GeoIP(filename)
    return gic


def geoip(stream, field="ip", ip_blacklist=set(), country_code="country_code", region_name="region_name", city="city", latitude="latitude", longitude="longitude", ignore_error=True):
    gic = get_gic()
    error_count = 0
    for r in stream:
        if isinstance(r, StreamHeader):
            header = r.insert(typename=None, fields=[country_code, region_name, city, latitude, longitude])
            yield header
        elif isinstance(r, StreamMeta):
            yield r
        else:
            ip = getattr(r, field)
            try:
                if ip in ip_blacklist:
                    yield header.t(*r + (None, None, None, None, None))
                    continue
                cc = gic.record_by_addr(ip)
                yield header.t(*(r + (cc['country_code'], cc['region_name'], cc['city'], cc['latitude'], cc['longitude'])))
            except Exception, e:
                if ignore_error:
                    if error_count == 0:
                        print "Error in load", ip, e
                        error_count = error_count + 1 
                    yield header.t(*r + (None, None, None, None, None))
                else:
                    raise e

BabeBase.register("geoip", geoip)


def geoip_country_code(stream, field="ip", country_code="country_code", ignore_error=False, geoip_file=None):
    """"
Add a 'country_code' field from IP address in field "IP"
    """
    gic = get_gic()
    for r in stream:
        if isinstance(r, StreamHeader):
            header = r.insert(typename=None, fields=[country_code])
            yield header
        elif isinstance(r, StreamMeta):
            yield r
        else:
            ip = getattr(r, field)
            try:
                cc = gic.country_code_by_addr(ip)
            except Exception, e:
                if ignore_error:
                    cc = None
                    pass
                else:
                    raise e
            yield header.t(*(r + (cc,)))

## TODO : full region parsing
BabeBase.register("geoip_country_code", geoip_country_code)
