#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Author:         Carl, Chris
Email:          chrisbcarl@gmail.com
Date:           2021-02-27
Description:

An example of how to create a decent generic sql api with Flask and sqlalchemy.
Usefull to me as a code-snippet.

The MIT License (MIT)
Copyright © 2021 Chris Carl

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

# stdlib imports
from __future__ import print_function, absolute_import, division
import os
import sys
import json
import traceback
import logging
import datetime
import logging.handlers as l_handlers
import argparse
from collections import OrderedDict
try:
    # Python3
    from urllib.parse import quote, quote_plus, unquote, unquote_plus
except ImportError:
    # Python2
    from urllib import quote, quote_plus, unquote, unquote_plus

# 3rd party imports
import pyodbc
from six import string_types
from six.moves import configparser
from flask import Flask, request, jsonify, send_from_directory
from sqlalchemy import (create_engine, MetaData)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import (sessionmaker, scoped_session)

# globals
ENGINE, SESSION_MAKER, BASE, METADATA = (None, None, None, None)

# constants
APP_NAME = os.path.splitext(os.path.basename(__file__))[0]
API_VERSION = 'v1'
FILE_DIRPATH = os.path.abspath(os.path.dirname(__file__))
CONF_FILEPATH = os.path.join(FILE_DIRPATH, '{}.conf'.format(APP_NAME))
CACHE_DIRPATH = os.path.join(FILE_DIRPATH, 'ignoreme')
LOG_FILEPATH = os.path.join(CACHE_DIRPATH, '{}.log'.format(APP_NAME))
if not os.path.isdir(CACHE_DIRPATH):
    os.makedirs(CACHE_DIRPATH)
SQLA_FMT = 'mssql+pyodbc://{uid}:{pwd}@{host}:{port}/{name}?driver={driver}'
SQLA_FMT_TRUSTED = 'mssql+pyodbc://{host}:{port}/{name}?trusted_connection={trusted_connection}&driver={driver}'
MSSQL_PERMISSIONS = '''
EXEC sp_table_privileges @table_name = '%', @table_owner = 'dbo'
'''

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
app.config['JSON_SORT_KEYS'] = False  # default is True
werkzeug_logger = logging.getLogger('werkzeug')
_stream_hndl = logging.StreamHandler(sys.stdout)
_stream_hndl.setLevel(logging.DEBUG)
app.logger.addHandler(_stream_hndl)
werkzeug_logger.addHandler(_stream_hndl)
_file_hndl = l_handlers.RotatingFileHandler(LOG_FILEPATH, mode='a', maxBytes=1024 * 1024 * 32, backupCount=0)
_file_hndl.setLevel(logging.DEBUG)
app.logger.setLevel(logging.DEBUG)
app.logger.addHandler(_file_hndl)
werkzeug_logger.addHandler(_file_hndl)


class CustomJSONEncoder(json.JSONEncoder):
    # https://stackoverflow.com/a/43663918
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)  # the microsecond format is what I want
        try:
            iterable = iter(obj)
            return json.JSONEncoder.default(self, list(iterable))  # covers sets
        except TypeError:
            pass
        return json.JSONEncoder.default(self, obj)


app.json_encoder = CustomJSONEncoder


def stack_it_up(
    connection_string, isolation_level='READ UNCOMMITTED', schema='dbo', autoflush=True, pool_size=5, max_overflow=0
):
    # https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-optimistic
    # https://stackoverflow.com/questions/29905160/automap-reflect-tables-within-a-postgres-schema-with-sqlalchemy
    # https://stackoverflow.com/questions/29905160/automap-reflect-tables-within-a-postgres-schema-with-sqlalchemy
    global ENGINE, SESSION_MAKER, BASE, METADATA

    if ENGINE is None:
        ENGINE = create_engine(
            connection_string,
            isolation_level=isolation_level,
            pool_recycle=60,
            pool_size=pool_size,
            max_overflow=max_overflow
        )

    if SESSION_MAKER is None:
        SESSION_MAKER = sessionmaker(bind=ENGINE, autoflush=autoflush)

    if BASE is None or METADATA is None:
        METADATA = MetaData(schema=schema)
        BASE = automap_base(bind=ENGINE, metadata=METADATA)
        BASE.prepare(ENGINE, reflect=True)


def get_session():
    if SESSION_MAKER is None:
        raise RuntimeError('session maker needs to be created first dude...')
    return scoped_session(SESSION_MAKER)


class MsSqlOdbc(object):
    # [odbc]
    KEYS = ['driver', 'server', 'instance', 'database', 'port', 'username', 'password', 'trusted_connection']
    driver = 'SQL Server'
    server = 'localhost'
    instance = 'SQLEXPRESS'
    database = 'master'
    port = 1433
    username = 'username'
    password = 'password123'
    trusted_connection = 0

    def get_connection_string(self):
        if self.trusted_connection:
            cnxn_str = SQLA_FMT_TRUSTED.format(
                host=self.server,
                port=self.port,
                name=self.database,
                driver=self.driver,
                trusted_connection='yes'  # if self.trusted_connection else 'no',
            )
            printable_string = cnxn_str
        else:
            cnxn_str = SQLA_FMT.format(
                uid=self.username,
                pwd=self.password,
                host=self.server,
                port=self.port,
                name=self.database,
                driver=self.driver,
            )
            printable_string = SQLA_FMT.format(
                uid=self.username,
                pwd='*' * len(self.password) * 3 // 2,
                host=self.server,
                port=self.port,
                name=self.database,
                driver=self.driver,
            )
        app.logger.info(printable_string)
        return cnxn_str

    def __str__(self):
        dick = OrderedDict()
        for key in self.__class__.KEYS:
            dick[key] = getattr(self, key)
        return '[odbc]\n{}'.format('\n'.join('{}={}'.format(k, '' if v is None else v) for k, v in dick.items()))


def parse_config(ini_filepath):
    # type: (str) -> MsSqlOdbc
    parser = configparser.ConfigParser()
    parser.read(ini_filepath)
    # [odbc]
    driver = parser.get('odbc', 'driver')
    if driver not in pyodbc.drivers():
        raise ValueError('"{}" not in legal drivers: {}'.format(driver, pyodbc.drivers()))
    server = parser.get('odbc', 'server')
    if server == '':
        raise ValueError('server cannot be None!')
    instance = parser.get('odbc', 'instance')
    if instance == '':
        instance = server
    database = parser.get('odbc', 'database')
    if database == '':
        raise ValueError('database cannot be None!')
    port = parser.get('odbc', 'port')
    if port == '':
        raise ValueError('port cannot be None!')
    try:
        port = int(port)
    except ValueError:
        raise ValueError('port must be an int!')

    trusted_connection = parser.get('odbc', 'trusted_connection')
    if trusted_connection == '':
        trusted_connection = False
    try:
        trusted_connection = int(trusted_connection)
        trusted_connection = bool(trusted_connection)
    except ValueError:
        pass  # so it wasn't an int
    try:
        trusted_connection = bool(trusted_connection)
    except ValueError:
        raise ValueError('trusted_connection must be a bool!')
    username = parser.get('odbc', 'username')
    if username == '' and not trusted_connection:
        raise ValueError('username cannot be None!')
    password = parser.get('odbc', 'password')
    if password == '' and not trusted_connection:
        raise ValueError('password cannot be None!')

    o = MsSqlOdbc()
    o.driver = driver
    o.server = server
    o.instance = instance
    o.database = database
    o.port = port
    o.username = username
    o.password = password
    o.trusted_connection = trusted_connection
    return o


class QueryParams(object):
    search = None
    search_key = None
    order = None
    order_key = None
    offset = None
    limit = None


def parse_query_params():
    # type: () -> QueryParams

    # where id > 1234
    offset = request.args.get('offset', -1)
    if offset == '':
        offset = -1
    elif offset is not None:
        if str(offset).lower() == 'nan':
            offset = -1
        else:
            offset = int(offset)

    # where <key> like '%<search>%'
    search = request.args.get('search', None)
    if search == '':
        search = None
    if isinstance(search, string_types):
        search = unquote(str(search)).lower()
    search_key = request.args.get('search_key', None)
    if search_key == '':
        search_key = None
    if isinstance(search_key, string_types):
        search_key = unquote(str(search_key)).lower()
    if (search is not None and search_key is None) or (search_key is not None and search is None):
        raise RuntimeError('you must provide search and search_key at the same time!')

    # order by <order_key>
    order_key = request.args.get('order_key', None)
    if order_key == '':
        order_key = None
    if isinstance(order_key, string_types):
        order_key = unquote(str(order_key)).lower()
    # order by <order_key> desc
    order = request.args.get('order', None)
    if order == '':
        order = 'asc'
    elif order is not None:
        order = order.lower()
    if order not in ['asc', 'desc']:
        raise ValueError('order not in {}'.format(['asc', 'desc']))
    if (order is not None and order_key is None) or (order_key is not None and order is None):
        raise RuntimeError('you must provide order and order_key at the same time!')

    # top 10 *
    limit = request.args.get('limit', None)
    if limit == '':
        limit = None
    elif limit is not None:
        if str(limit).lower() == 'all':
            limit = None
        else:
            limit = int(limit)

    q = QueryParams()
    q.search = search
    q.search_key = search_key
    q.order = order
    q.order_key = order_key
    q.offset = offset
    q.limit = limit
    return q


def inspect_table(table):
    table_map = {t.lower(): t for t in dir(BASE.classes) if '__' not in t}

    if not hasattr(BASE.classes, table_map[table.lower()]):
        raise ValueError('provided table: "{}"; existing tables: {}'.format(table, dir(BASE.classes)))

    TABLE = getattr(BASE.classes, table_map[table.lower()])
    introspection = inspect(TABLE)
    column_map = OrderedDict()
    for column in introspection.columns:
        column_map[column.name.lower()] = column.name

    primary_key_map = OrderedDict()
    for key in introspection.primary_key:
        primary_key_map[key.name.lower()] = key.name

    return TABLE, introspection, column_map, primary_key_map


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static', 'img'), 'favicon.ico', mimetype='image/vnd.microsoft.icon'
    )


METADATA_SERIALIZED = None


@app.route('/api/{}/metadata'.format(API_VERSION), methods=['GET'])
def metadata_endpoint():
    global METADATA_SERIALIZED
    if METADATA_SERIALIZED is None:
        tables = set(filter(lambda x: not x.startswith('__'), dir(BASE.classes)))
        dick = OrderedDict()
        for table in tables:
            _, introspection, column_map, primary_key_map = inspect_table(table)
            table_dick = OrderedDict()
            columns = OrderedDict()
            for i, c in enumerate(introspection.columns):
                col_dick = dict(position=i, type=c.type.__class__.__name__, name=c.name)
                if hasattr(c.type, 'length'):
                    col_dick['length'] = c.type.length
                else:
                    col_dick['length'] = None
                columns[c.name] = col_dick
            table_dick['columns'] = columns
            table_dick['primary_key_map'] = primary_key_map
            table_dick['column_map'] = column_map
            dick[table] = table_dick
        METADATA_SERIALIZED = dick

    return jsonify(METADATA_SERIALIZED)


PERMISSIONS_SERIALIZED = None


@app.route('/api/{}/permissions'.format(API_VERSION), methods=['GET'])
def permissions_endpoint():
    global PERMISSIONS_SERIALIZED
    if PERMISSIONS_SERIALIZED is None:
        session = get_session()
        rows = []
        result_proxy = session.execute(MSSQL_PERMISSIONS)
        for row_proxy in result_proxy:
            dick = OrderedDict()
            for k, v in row_proxy.items():
                dick[k] = v
            rows.append(dick)
        PERMISSIONS_SERIALIZED = rows

    return jsonify(PERMISSIONS_SERIALIZED)


@app.route('/api/{}/<resource>'.format(API_VERSION), methods=['GET', 'POST', 'OPTIONS'])
@app.route('/api/{}/<resource>/<id_>'.format(API_VERSION), methods=['GET', 'PUT', 'DELETE', 'OPTIONS'])
def generic_endpoint(resource, id_=None):
    tables = set(filter(lambda x: not x.startswith('__'), dir(BASE.classes)))
    session = get_session()
    rows = []
    try:
        if resource not in tables:
            raise RuntimeError('{!r} not in {}'.format(resource, tables))

        TABLE, _, column_map, primary_key_map = inspect_table(resource)

        if request.method == 'GET':
            query = session.query(TABLE)

            if id_ is not None:  # route b
                if 'id' not in primary_key_map:
                    raise RuntimeError('{!r} is gonna need something custom to deal with offset.'.format(resource))
                id_column = getattr(TABLE, primary_key_map['id'])
                query = query.filter(id_column == id_)

            else:  # route a
                q = parse_query_params()
                if q.offset is not None:
                    if 'id' not in primary_key_map:
                        raise RuntimeError('{!r} is gonna need something custom to deal with offset.'.format(resource))
                    id_column = getattr(TABLE, primary_key_map['id'])
                    query = query.filter(id_column >= q.offset)

                if q.search is not None:
                    if q.search_key not in column_map:
                        raise RuntimeError(
                            '{!r} is not a real column for {!r}! these are: {}'.format(
                                q.search_key, resource, list(column_map.keys())
                            )
                        )
                    key_column = getattr(TABLE, column_map[q.search_key])
                    query = query.filter(key_column.like(q.search))

                if q.order is not None:
                    if q.order_key not in column_map:
                        raise RuntimeError(
                            '{!r} is not a real column for {!r}! these are: {}'.format(
                                q.order_key, resource, list(column_map.keys())
                            )
                        )
                    sort_column = getattr(TABLE, column_map[q.order_key])
                    if q.order == 'asc':
                        query = query.order_by(sort_column.asc())
                    else:
                        query = query.order_by(sort_column.desc())

                if q.limit is not None:
                    query = query.limit(q.limit)

            proxy_rows = query.all()
            if proxy_rows:
                for row in proxy_rows:
                    rows.append({k: getattr(row, k) for k in column_map})

        elif request.method == 'POST':
            if id_ is None:  # route a
                # this is correct and good
                body = request.data  # comes as a string or None
                if body is None:
                    raise ValueError('POST expects a body! got back nothing!')
                body = json.loads(body)
                sanitized = {}
                for k, v in body.items():
                    if k not in column_map:
                        raise RuntimeError(
                            '{!r} is not a real column for {!r}! these are: {}'.format(
                                k, resource, list(column_map.keys())
                            )
                        )
                    sanitized[column_map[k]] = v
                orm_object = TABLE(**sanitized)
                session.add(orm_object)
                session.commit()
                rows.append({k: getattr(orm_object, k) for k in column_map})
                session.expunge(orm_object)  # otherwise it'll hold onto it, driving memory up

            else:  # route b
                raise RuntimeError(
                    'thou shalt not {} to a {} like you just did at {}!'.format(
                        request.method, request.url_rule.rule, request.path
                    )
                )

        elif request.method == 'PUT':
            if id_ is None:  # route a
                # this is awful and wrong
                raise RuntimeError(
                    'thou shalt not {} to a {} like you just did at {}!'.format(
                        request.method, request.url_rule.rule, request.path
                    )
                )
            else:
                query = session.query(TABLE)
                if 'id' not in primary_key_map:
                    raise RuntimeError('{!r} is gonna need something custom to deal with offset.'.format(resource))
                id_column = getattr(TABLE, primary_key_map['id'])
                query = query.filter(id_column == id_)
                orm_object = query.all()

                if orm_object:
                    body = request.data  # comes as a string or None
                    if body is None:
                        raise ValueError('POST expects a body! got back nothing!')
                    body = json.loads(body)
                    for k, v in body.items():
                        if k not in column_map:
                            raise RuntimeError(
                                '{!r} is not a real column for {!r}! these are: {}'.format(
                                    k, resource, list(column_map.keys())
                                )
                            )
                        setattr(orm_object, column_map[k], v)
                    session.commit()
                    rows.append({k: getattr(orm_object, k) for k in column_map})
                    session.expunge(orm_object)  # otherwise it'll hold onto it, driving memory up

        elif request.method == 'DELETE':
            if id_ is None:  # route a
                # this is awful and wrong
                raise RuntimeError(
                    'thou shalt not {} to a {} like you just did at {}!'.format(
                        request.method, request.url_rule.rule, request.path
                    )
                )
            else:
                query = session.query(TABLE)
                if 'id' not in primary_key_map:
                    raise RuntimeError('{!r} is gonna need something custom to deal with offset.'.format(resource))
                id_column = getattr(TABLE, primary_key_map['id'])
                query = query.filter(id_column == id_)
                orm_object = query.all()

                if orm_object:
                    session.delete(orm_object)
                    rows.append(id_)

    except Exception as e:
        session.close()
        return jsonify(error=str(e), traceback=traceback.format_exc())

    return jsonify(rows)


if __name__ == '__main__':
    description = '''{}

Example "{}":
{}'''.format(__doc__, CONF_FILEPATH, '\n'.join('    {}'.format(line) for line in str(MsSqlOdbc()).splitlines()))
    parser = argparse.ArgumentParser(
        APP_NAME, formatter_class=argparse.RawDescriptionHelpFormatter, description=description
    )
    parser.add_argument(
        '--debug', action='store_true', help='ironically, disable the debug mode on flask for actual debugger reasons.'
    )
    args = parser.parse_args()
    app.logger.info('got args: {}'.format(vars(args)))

    odbc = parse_config(CONF_FILEPATH)
    app.logger.info('loading all of the nasty sql stuff')
    stack_it_up(odbc.get_connection_string())
    app.run(debug=not args.debug)
