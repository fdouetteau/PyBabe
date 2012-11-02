
import socket
try:
    from unittest import TestCase, skipUnless, main
except:
    from unittest2 import TestCase, skipUnless, main


def can_connect_to_the_net():
    try:
        socket.gethostbyname('www.google.com')
        return True
    except Exception:
        return False


def can_execute(s):
    try:
        from subprocess import Popen, PIPE
        p = Popen([s], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        p.stdin.close()
        p.wait()
        return True
    except OSError:
        return False


def can_connect(host, port):
    try:
        socket.create_connection((host, port), timeout=1)
        return True
    except Exception:
        return False

TestCase = TestCase
skipUnless = skipUnless
main = main


