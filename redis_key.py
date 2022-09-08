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

import redis
from flask import Blueprint, request

redis_key = Blueprint('redis_key', __name__)


@redis_key.route('/set', methods=['POST'])
def create_keyspace():
    request_data = request.get_json()
    url = request_data['url']
    key = request_data['key']
    value = request_data['value']
    cluster_enable = request.args.get('cluster')
    if cluster_enable == 'true':
        logging.info('redis cluster %s set key %s value %s', url, key, value)
        r = redis.cluster.RedisCluster.from_url(url)
        r.set(key, value)
    else:
        logging.info('redis %s set key %s value %s', url, key, value)
        r = redis.from_url(url)
        r.set(key, value)
    return 'OK'


@redis_key.route('/delete', methods=['POST'])
def delete_keyspace():
    request_data = request.get_json()
    url = request_data['url']
    key = request_data['key']
    cluster_enable = request.args.get('cluster')
    if cluster_enable == 'true':
        logging.info('redis cluster %s delete key %s', url, key)
        r = redis.cluster.RedisCluster.from_url(url)
        r.delete(key)
    else:
        logging.info('redis %s delete key %s', url, key)
        r = redis.from_url(url)
        r.delete(key)
    return 'OK'
