"""Copyright 2012 Carlos Alberto Cortez

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import os
import datetime
import time

from carbon import state
from carbon.cache import MetricCache
from carbon.storage import getFilesystemPath, loadStorageSchemas, loadAggregationSchemas
from carbon.conf import settings
from carbon import log, events, instrumentation
from carbon.writer import BasePersister

import psycopg2


'''
The most important thing to think about, right now,
is how to handle the connection. Should we keep one per thread?
Should we keep it open?
ALSO: what to do when the connection gets refused? Besides logging it, of
course.
Let us somewhere specify the credentials por the persister (dbinfo, that is).
'''

PG_BACKEND_SETTINGS = {
    "dbname" : "senzari_stats",
    "user" : "senzari_dev",
    "password" : "senzari_dev",
    "host" : "localhost"
}

class PostgresqlPersister(BasePersister):
    def __init__ (self):
        self.reset()

    def reset(self):
        connection = psycopg2.connect(database=PG_BACKEND_SETTINGS["dbname"],
                        host=PG_BACKEND_SETTINGS["host"],
                        user=PG_BACKEND_SETTINGS["user"],
                        password=PG_BACKEND_SETTINGS["password"])
        self._connection = connection

    #Our persister takes for granted the databases are
    #created already (which is different to Whisper, which creates
    #one per Metric, on demand)
    def get_dbinfo(self, metric):
        '''
        Gets the database info related to a metric, as well as whether it
        exists already or not (as a tuple)
        '''
        return ('core_stats', True)

    def update_one(self, metric, datapoint):
        value = datapoint[1]
        timestamp = datetime.datetime.fromtimestamp(int(datapoint[0]))
        sql_stmt =  """
            INSERT into core_stats(name, "time", value)
            VALUES ('%s', '%s', %f);
        """
        cursor = self._connection.cursor()
        cursor.execute(sql_stmt % (metric, timestamp, value))
        log.msg("successfully inserted value to metric %s" % (metric,))

    def update_many(self, metric, datapoints, dbIdentifier):
        '''
        Updates the datapoints for a metric.
        'metric' is the name of the param ('my.value')
        'datapoints' is a list of tuples, containing the timestamp and value
        '''
        log.msg("updating metric %s using the postgresql persister" % (metric,))
        try:
            for datapoint in datapoints:
                self.update_one(metric, datapoint)

            self._connection.commit()
        except:
            log.msg("failed to insert/update stats into postgresql")
            log.err()

    def __del__(self):
        if hasattr(self, '_connection') and self.connection:
            self.connection.close()

