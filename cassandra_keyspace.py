#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from flask import Blueprint, request

cassandra_keyspace = Blueprint('cassandra_keyspace', __name__)


@cassandra_keyspace.route('/create', methods=['POST'])
def create_keyspace():
    request_data = request.get_json()
    username = request_data['username']
    password = request_data['password']
    host = request_data['host']
    keyspace = request_data['keyspace']
    replication_factor = request_data['replication_factor']
    logging.info('cassandra %s create key space %s replication factor %s username %s',
                 host, keyspace, replication_factor, username)
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster([host], auth_provider=auth_provider)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE %s
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %s }
        """ % (keyspace, replication_factor))
    return 'OK'


@cassandra_keyspace.route('/delete', methods=['POST'])
def delete_keyspace():
    request_data = request.get_json()
    username = request_data['username']
    password = request_data['password']
    host = request_data['host']
    keyspace = request_data['keyspace']
    logging.info('cassandra %s delete key space %s username %s', host, keyspace, username)
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster([host], auth_provider=auth_provider)
    session = cluster.connect()
    session.execute("""
        DROP KEYSPACE %s
        """ % keyspace)
    return 'OK'
