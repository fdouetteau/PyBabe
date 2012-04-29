
from base import BabeBase, StreamHeader, StreamMeta
import os 


gic = None

def geoip_country_code(stream, field="ip", country_code="country_code", geoip_file = None): 
    """"
Add a 'country_code' field from IP address in field "IP"
    """
    global gic
    if gic == None:
        from pygeoip import GeoIP
        if os.path.exists("/usr/local/share/GeoIP/GeoLiteCity.dat"):
            default = "/usr/local/share/GeoIP/GeoLiteCity.dat"
        elif os.path.exists("/usr/local/var/lib/GeoLiteCity.dat"):
            default = "/usr/local/var/lib/GeoLiteCity.dat" 
        else:
            default = None
        filename = BabeBase.get_config_with_env('geoip', 'GEOIP_FILE', {}, default)
        gic = GeoIP(filename)
    for r in stream:
        if isinstance(r, StreamHeader):
            header = r.insert(typename=None, fields=[country_code])
            yield header 
        elif isinstance(r, StreamMeta):
            yield r
        else:
            ip = getattr(r, field)
            cc = gic.country_code_by_addr(ip)
            yield header.t(*(r + (cc,)))

## TODO : full region parsing

BabeBase.register("geoip_country_code", geoip_country_code)


