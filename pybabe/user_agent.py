from base import BabeBase, StreamMeta, StreamHeader

http_detect = None


def do_detect(s):
    global http_detect
    if not http_detect:
        from httpagentparser import detect
        http_detect = detect
    return http_detect(s)


def user_agent(stream, field, output_os=None, output_browser=None, output_browser_version=None):
    for row in stream:
        if isinstance(row, StreamHeader):
            header = row.insert(typename=None, fields=filter(lambda x: x is not None, [output_os, output_browser, output_browser_version]))
            yield header
        elif isinstance(row, StreamMeta):
            yield row
        else:
            useragent = getattr(row, field)
            o = do_detect(useragent)
            d = []
            if output_os:
                ## On device such as "Ipad", os is in "flavor" and device name in "dist"
                d.append(o['os']['name'] if 'os' in o else o['dist']['name'] if 'dist' in o else None)
            if output_browser:
                d.append(o['browser']['name'] if 'browser' in o else None)
            if output_browser_version:
                d.append(o['browser']['version'] if 'browser' in o and 'version' in o['browser'] else None)
            yield header.t(*(row + tuple(d)))

BabeBase.register("user_agent", user_agent)
