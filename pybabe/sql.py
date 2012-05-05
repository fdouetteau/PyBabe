

from base import BabeBase, StreamHeader, StreamFooter
import csv
from subprocess import Popen, PIPE
import time
from string import Template
from charset import UnicodeCSVWriter

PULL_DB = { 
    'mysql' : 
        { 
            'query_template' : '${query};\n',
            'command' : ['mysql'],
            'separator' : '--delimiter=%s',
            'user' : '-u%s',
            'password' : '-p%s'
        }, 
    'infinidb' : 
        {
            'query_template' : '${query};\n',
            'command' : ['/usr/local/Calpont/mysql/bin/mysql', '--defaults-file=/usr/local/Calpont/mysql/my.cnf', "--default-character-set", "utf8"],
            'separator' : '--delimiter=%s',
            'user' : '-u%s',
            'password' : '-p%s'
        }, 
    'sqlite' : 
        { 
            'query_template' : '.header ON\n.separator "\t"\n${query};\n',
            'command' : ['sqlite3'],
            'user' : '-u%s',
            'password' : '-p%s', 
        }, 
    'vectorwise' : 
        { 
            # 'query_template' : "DECLARE GLOBAL TEMPORARY TABLE SESSION.qresult AS ${query} ON COMMIT PRESERVE ROWS WITH NORECOVERY;\\g\nCOPY TABLE SESSION.qresult() INTO 'toto.txt';\\g\n", 
            'query_template' : '\\titles\n\\trim\n${query};\\g\n', 
            'command' : ['sql', '-S', '-v\t'], 
            'user' : '-u%s', 
            'password' :'-P%s'
        }
}

PUSH_DB = { 
    'sqlite' : 
    { 
        'command': [ 'sqlite3' ], 
        'drop_table' : 'DROP TABLE IF EXISTS %s;\n',  
        'create_table' : 'CREATE TABLE IF NOT EXISTS ${table} ( ${fields} );\n', 
        'import_query': '.separator "\t"\n.import %s %s\n', # Import into database. 
    }, 
    'mysql' : 
    { 
        'command' : ['mysql', '--local-infile'], 
        'user' : '-u%s',
        'password' : '-p%s',
        'drop_table' : 'DROP TABLE IF EXISTS %s;\n', 
        'create_table' : 'CREATE TABLE IF NOT EXISTS ${table} ( ${fields} );\n', 
        'import_query' : "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t';\n"
    }, 
    'vectorwise' : 
    { 
        'load_command' : ['vwload', '-f', '\t', '--table', '${table}', '${database}', '/dev/stdin'], 
        'command' : ['sql', '-S'], 
        'drop_table' : 'DROP TABLE IF EXISTS %s;commit;', 
        "create_table" : "CREATE TABLE  ${table} ( ${fields} );commit;\\g"
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

    if db_params.get('need_pipe', False):
        tmpfifo = TempFifo()     
        readstream = tmpfifo.open_read()
    else: 
        tmpfifo = None
        readstream = None

    print query
    query_ins = Template(db_params['query_template']).substitute(query=query, out_filename=tmpfifo.filename if tmpfifo else None)
    p = Popen(c, stdin=PIPE, stdout=None if readstream else PIPE, stderr=None)
    p.stdin.write(query_ins)
    p.stdin.flush()
    p.stdin.close()
    dialect = sql_dialect()
    reader = csv.reader(readstream if readstream else p.stdout, dialect=dialect)        
    fields = reader.next()
    if database_kind ==  'vectorwise': 
        fields[-1] = fields[-1][:-1]
    metainfo = StreamHeader(**dict(kwargs, typename=table, fields=fields))
    yield metainfo
    for row in reader:
        if database_kind ==  'vectorwise': 
            row[-1] = row[-1][:-1]
        yield metainfo.t._make([unicode(x, 'utf-8') for x in row])
    p.wait()
    if p.returncode != 0: 
        raise Exception("SQL process failed with errcode %u" % p.returncode)
    yield StreamFooter()
    

class sql_dialect(csv.Dialect):
    lineterminator = '\n'
    delimiter = '\t'
    doublequote = False
    escapechar = '\\'
    quoting = csv.QUOTE_MINIMAL
    quotechar = '"'

import os, tempfile

class TempFifo(object): 
    def __init__(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tmpdir, 'myfifo')
        os.mkfifo(self.filename)
        self.readstream = None
        self.writestream = None

    def open_read(self):
        fd = os.open(self.filename, os.O_RDONLY | os.O_NONBLOCK)
        self.readstream = os.fdopen(fd, 'r')
        return self.readstream

    def open_write(self):
        for retry in xrange(0,7):
            try:
                fd = os.open(self.filename, os.O_WRONLY | os.O_NONBLOCK)
                self.writestream = os.fdopen(fd, 'w')
            except OSError, e:
                if retry == 4:
                    raise e
                time.sleep(0.5)
        return self.writestream

    def close(self):
        if self.writestream:
            self.writestream.close()
        if self.readstream:
            self.readstream.close()
        os.remove(self.filename)
        os.rmdir(self.tmpdir)



def push_sql(stream, database_kind, table=None, host=None, create_table = False, drop_table = False, protocol=None, database=None, ssh_host=None, user=None, password=None,sql_command=None, **kwargs):
    db_params = PUSH_DB[database_kind]
    c = db_params['command']
    if user:
        c = c + [db_params['user']%user]
    if password:
        c = c + [db_params['password']%password]
    
    c = c + [database]
    

    for row in stream:
        if isinstance(row, StreamHeader):
            metainfo = row
            if not table: 
                table_name = metainfo.typename
            else: 
                table_name = table 
            p = Popen(c, stdin=PIPE, stdout=None, stderr=None)


            if drop_table: 
                drop_table_query = db_params['drop_table'] % table_name
                p.stdin.write(drop_table_query)
                p.stdin.flush()
                if p.returncode:
                    break
            if create_table:
                fields = ','.join([name + ' varchar(255)' for name in metainfo.fields])
                create_table_query = Template(db_params['create_table']).substitute(table=table_name, fields=fields)
                p.stdin.write(create_table_query)
                p.stdin.flush()
                if p.returncode:
                    break

            p.stdin.close()
            p.wait()

            writestream = None

            #print import_query
            if "import_query" in db_params:
                p = Popen(c, stdin=PIPE, stdout=None, stderr=None)
                tmpfifo = TempFifo()
                import_query = db_params['import_query'] % (tmpfifo.filename, table_name) 
                p.stdin.write(import_query)
                p.stdin.flush()
                writestream = tmpfifo.open_write()
            elif 'load_command' in db_params:
                load_command = [Template(s).substitute(table=table_name, database=database) for s in db_params['load_command']]
                print load_command 
                pp = Popen(load_command, stdin=PIPE, stdout=None, stderr=None)
                writestream = pp.stdin
            else:
                raise Exception("Missing load_command or import_query in db_kind spec")

            writer = UnicodeCSVWriter(writestream, dialect=sql_dialect(), encoding="utf-8")
            #writer = csv.writer(writestream, dialect=sql_dialect()) 
        elif isinstance(row, StreamFooter):
            if "import_query" in db_params:
                tmpfifo.close()
                p.stdin.close()
                p.wait()
            elif 'load_command' in db_params:
                pp.stdin.close()
                pp.wait()
        else:
            writer.writerow(row)


BabeBase.register('pull_sql', pull_sql)
BabeBase.registerFinalMethod('push_sql', push_sql)

