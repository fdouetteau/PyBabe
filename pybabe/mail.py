

import smtplib
# Here are the email package modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from itertools import izip
from base import BabeBase, ordered_dict
from cStringIO import StringIO

COMMASPACE = ', '


def mail(stream, subject, recipients, in_body=False, in_body_row_limit=None, attach_formats="csv", **kwargs):
    """Format a stream in a mail and send it.
    Recipients: list of recipients mail addresses
    in_body: format (in HTML & text) the content
    in_body_row_limit : maximum number of line in body
    attach_format : file format to use for attachment
    """

    smtp_server = BabeBase.get_config('smtp', 'server', kwargs)
    smtp_port = BabeBase.get_config('smtp', 'port', kwargs)
    smtp_tls = BabeBase.get_config('smtp', 'tls', kwargs, False)
    smtp_login = BabeBase.get_config('smtp', 'login', kwargs)
    smtp_password = BabeBase.get_config('smtp', 'password', kwargs)
    author = BabeBase.get_config('smtp', 'author', kwargs)

    formats = []
    if in_body:
        formats.append("html")
    if attach_formats:
        if isinstance(attach_formats, basestring):
            formats.append(attach_formats)
        else:
            formats.extend(attach_formats)
    if isinstance(recipients, basestring):
        recipients = [recipients]

    babes = stream.tee(len(formats))
    if in_body and in_body_row_limit:
        babes[0] = babes[0].head(in_body_row_limit, all_streams=True)

    buffer_dicts = []
    for format, babe in izip(formats, babes):
        d = ordered_dict()
        babe.push(stream_dict=d, format=format)
        buffer_dicts.append((format, d))

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = author
    msg['To'] = ', '.join(recipients)

    for format, d in buffer_dicts:
        if format == "html":
            buf = StringIO()
            buf.write('<html><body>\n')
            for filename in d:
                buf.write(d[filename].getvalue())
                buf.write('\n')
            buf.write('\n</body></html>')
            att = MIMEText(buf.getvalue(), "html")
            msg.attach(att)
        else:
            for filename in d:
                c = d[filename].getvalue()
                (maintype, subtype) = BabeBase.getMimeType(format)
                att = MIMEBase(maintype, subtype)
                att.set_payload(c)
                encoders.encode_base64(att)
                att.add_header('Content-Disposition', 'attachment', filename=filename + "." + format)
                msg.attach(att)

    s = smtplib.SMTP(smtp_server, smtp_port)
    s.ehlo()
    if smtp_tls:
        s.starttls()
        s.ehlo()
    s.login(smtp_login, smtp_password)
    s.sendmail(author, recipients, msg.as_string())
    s.quit()


BabeBase.registerFinalMethod('mail', mail)
