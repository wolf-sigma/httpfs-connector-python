# Copyright 2012 Alex Breshears.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import django.conf

DEBUG = django.conf.settings.DEBUG

HTTPFS_ROOT_URL = django.conf.settings.HTTPFS_ROOT_URL
HTTPFS_USERNAME = django.conf.settings.HTTPFS_USERNAME
HTTPFS_PASSWORD = django.conf.settings.HTTPFS_PASSWORD

MAX_REDIRECTS = 10