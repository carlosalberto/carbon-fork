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
import time

from carbon import state
from carbon.cache import MetricCache
from carbon.storage import getFilesystemPath, loadStorageSchemas, loadAggregationSchemas
from carbon.conf import settings
from carbon import log, events, instrumentation

import psycopg2

class PostegresqlPersister(object):
    def __init__ (self):
        pass

    def writeDataPoints (self):
        pass

