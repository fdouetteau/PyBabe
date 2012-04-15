

from base import BabeBase, MetaInfo
import csv
from subprocess import Popen, PIPE
import time

PULL_DB = { 
    'mysql' : 
        { 
            'query_template' : '%s;\n',
            'command' : ['mysql'],
            'separator' : '--delimiter=%s',
            'user' : '-u%s',
            'password' : '-p%s'
        }, 
    'infinidb' : 
        {
            'query_template' : '%s;\n',
            'command' : ['/usr/local/Calpont/mysql/bin/mysql', '--defaults-file=/usr/local/Calpont/mysql/my.cnf'],
            'separator' : '--delimiter=%s',
            'user' : '-u%s',
            'password' : '-p%s'
        }, 
    'sqlite' : 
        { 
            'query_template' : '.header ON\n.separator "\t"\n%s;\n',
            'command' : ['sqlite3'],
            'user' : '-u%s',
            'password' : '-p%s', 
        }
}

PUSH_DB = { 
    'sqlite' : 
    { 
        'command': [ 'sqlite3' ], 
        'drop_table' : 'DROP TABLE IF EXISTS %s;\n',  
        'create_table' : 'CREATE TABLE IF NOT EXISTS %s ( %s );\n', 
        'import_query': '.separator "\t"\n.import %s %s\n', # Import into database. 
    }, 
    'mysql' : 
    { 
        'command' : ['mysql'], 
        'drop_table' : 'DROP TABLE IF EXISTS %s;\n', 
        'create_table' : 'CREATE TABLE IF NOT EXISTS %s ( %s );\n', 
        'import_query' : "LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t'"
    }
}

def pull_sql(false_stream, query=None, table=None, host=None, database_kind=None, database=None, ssh_host=None, user=None, password=None, sql_command=None, **kwargs):
    """Pull from SQL query to the database.  
    query : The query to execute, if not SELECT * FROM table
    table : The table to fetch from
    db    : The database to query
    host  : The host to connect to
    ssh_host : SSH to a remote connection. HOST  or USER@HOST
    command : Override the connection command string prefix
    """

    db_params = PULL_DB[database_kind]

    if sql_command:
        c = sql_command 
    else: 
        c = db_params['command']

    if 'separator' in db_params:
        c = c + [ db_params['separator'] % '\t' ] 

    if user:
        c = c + [ db_params['user'] % user  ] 
    if password: 
        c = c + [ db_params['password'] % password]
    
    c = c + [database]

    if not query: 
        query = 'SELECT * FROM %s' % table

    query = db_params['query_template'] % query

    p = Popen(c, stdin=PIPE, stdout=PIPE, stderr=None)
    p.stdin.write(query)
    p.stdin.close()
    dialect = sql_dialect()
    reader = csv.reader(p.stdout, dialect=dialect)        
    names = reader.next()
    metainfo = MetaInfo(name=table, dialect=dialect, names=names)
    yield metainfo
    for row in reader:
        yield metainfo.t._make([unicode(x, 'utf-8') for x in row])
    p.wait()

    

class sql_dialect(csv.Dialect):
    lineterminator = '\n'
    delimiter = '\t'
    doublequote = False
    escapechar = '\\'
    quoting = csv.QUOTE_MINIMAL
    quotechar = '"'

import os, tempfile

def get_tempfifo():
    tmpdir = tempfile.mkdtemp()
    filename = os.path.join(tmpdir, 'myfifo')
    os.mkfifo(filename)
    return (tmpdir, filename) 

def push_sql(stream, database_kind, table=None, host=None, create_table = False, drop_table = False, protocol=None, database=None, ssh_host=None, user=None, password=None,sql_command=None, **kwargs):
    db_params = PUSH_DB[database_kind]
    c = db_params['command']
    if user:
        c = c + [db_params['user']%user]
    if password:
        c = c + [db_params['password']%password]
    
    c = c + [database]
    
    p = Popen(c, stdin=PIPE, stdout=None, stderr=None)

    for row in stream:
        if isinstance(row, MetaInfo):
            metainfo = row
            if not table: 
                table = metainfo.name
            (tmpdir, filename)  = get_tempfifo()
            import_query = db_params['import_query'] % (filename, table) 
            if drop_table: 
                drop_table_query = db_params['drop_table'] % table
                #print drop_table_query
                p.stdin.write(drop_table_query)
                if p.returncode:
                    break
            if create_table:
                fields = ','.join([name + ' varchar(255)' for name in metainfo.names])
                create_table_query = db_params['create_table'] % (table, fields)
                #print create_table_query
                p.stdin.write(create_table_query)
                if p.returncode:
                    break
            #print import_query
            p.stdin.write(import_query)
            p.stdin.flush()
            writestream = None
            for retry in xrange(0,5):
                try:
                    fd = os.open(filename, os.O_WRONLY | os.O_NONBLOCK)
                    writestream = os.fdopen(fd, 'w')
                except OSError, e:
                    if retry == 4:
                        raise e
                    time.sleep(0.5)
            writer = csv.writer(writestream, dialect=sql_dialect()) 
        else:
            writer.writerow(row)
    writestream.close()
    os.remove(filename)
    os.rmdir(tmpdir)
    p.stdin.close()
    p.wait()


BabeBase.register('pull_sql', pull_sql)
BabeBase.registerFinalMethod('push_sql', push_sql)

