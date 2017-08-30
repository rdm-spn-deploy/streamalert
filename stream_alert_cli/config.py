"""
Copyright 2017-present, Airbnb Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from collections import OrderedDict
import json
import os
import re

from stream_alert_cli.logger import LOGGER_CLI


class CLIConfigError(Exception):
    pass

class CLIConfig(object):
    """A class to load, modify, and display the StreamAlertCLI Config"""
    DEFAULT_CONFIG_PATH = 'conf'

    def __init__(self, **kwargs):
        self.config_path = kwargs.get('config_path', self.DEFAULT_CONFIG_PATH)
        self.config = {'clusters': {}}
        self.load()

    def __repr__(self):
        return str(self.config)

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, new_value):
        self.config.__setitem__(key, new_value)
        self.write()

    def get(self, key):
        """Lookup a value based on its key"""
        return self.config.get(key)

    def keys(self):
        """Config keys"""
        return self.config.keys()

    def clusters(self):
        """Return list of cluster configuration keys"""
        return self.config['clusters'].keys()

    def generate_athena(self):
        """Generate a base Athena config"""
        if 'athena_partition_refresh_config' in self.config['lambda']:
            LOGGER_CLI.warn('The Athena configuration already exists, skipping.')
            return

        athena_config_template = {
            'enabled': True,
            'enable_metrics': False,
            'current_version': '$LATEST',
            'refresh_type': {
                'add_hive_partition': {},
                'repair_hive_table': {}
            },
            'handler': 'stream_alert.athena_partition_refresh.main.handler',
            'timeout': '60',
            'memory': '128',
            'log_level': 'info',
            'source_bucket': 'PREFIX_GOES_HERE.streamalert.source',
            'source_current_hash': '<auto_generated>',
            'source_object_key': '<auto_generated>',
            'third_party_libraries': [
                'backoff'
            ]
        }

        # Check if the prefix has ever been set
        if self.config['global']['account']['prefix'] != 'PREFIX_GOES_HERE':
            athena_config_template['source_bucket'] = self.config['lambda'] \
                ['rule_processor_config']['source_bucket']

        self.config['lambda']['athena_partition_refresh_config'] = athena_config_template
        self.write()

        LOGGER_CLI.info('Athena configuration successfully created')

    def set_athena_lambda_enable(self):
        """Enable athena partition refreshes"""
        if 'athena_partition_refresh_config' not in self.config['lambda']:
            LOGGER_CLI.error('No configuration found for Athena Partition Refresh. '
                             'Please run: $ python manage.py athena init')
            return

        self.config['lambda']['athena_partition_refresh_config']['enabled'] = True
        self.write()

        LOGGER_CLI.info('Athena configuration successfully enabled')

    def set_prefix(self, prefix):
        """Set the Org Prefix in Global settings"""
        if not isinstance(prefix, (unicode, str)):
            LOGGER_CLI.error('Invalid prefix type, must be string')
            return

        self.config['global']['account']['prefix'] = prefix
        self.config['global']['terraform']['tfstate_bucket'] = self.config['global']['terraform'][
            'tfstate_bucket'].replace('PREFIX_GOES_HERE', prefix)

        self.config['lambda']['alert_processor_config']['source_bucket'] = self.config['lambda'][
            'alert_processor_config']['source_bucket'].replace('PREFIX_GOES_HERE', prefix)
        self.config['lambda']['rule_processor_config']['source_bucket'] = self.config['lambda'][
            'rule_processor_config']['source_bucket'].replace('PREFIX_GOES_HERE', prefix)
        self.write()

        LOGGER_CLI.info('Prefix successfully configured')

    def set_aws_account_id(self, aws_account_id):
        """Set the AWS Account ID in Global settings"""
        if not re.search(r'\A\d{12}\Z', aws_account_id):
            LOGGER_CLI.error('Invalid AWS Account ID, must be 12 digits long')
            return

        self.config['global']['account']['aws_account_id'] = aws_account_id
        self.write()

        LOGGER_CLI.info('AWS Account ID successfully configured')

    def _config_reader(self, key, file_path, **kwargs):
        """Read a given file into a config key

        Args:
            key (str): The key in the config dictionary to place the loaded
                config file.
            file_path (str): The location on disk to load the config file.

        Keyword Arguments:
            cluster_file (bool): If the file to load is a cluster file.
        """
        # This accounts for non files passed in, such as a
        # directory from os.listdir()
        if not os.path.isfile(file_path):
            return

        with open(file_path) as data:
            try:
                if kwargs.get('cluster_file', False):
                    self.config['clusters'][key] = json.load(data)
                else:
                    # For certain log types (csv), the order of the schema
                    # must be retained.  By loading as an OrderedDict,
                    # the configuration is gauaranteed to keep its order.
                    if key == 'logs':
                        self.config[key] = json.load(data,
                                                     object_pairs_hook=OrderedDict)
                    else:
                        self.config[key] = json.load(data)
            except ValueError:
                raise CLIConfigError('[Config Error]: %s is not valid JSON', file_path)

    @staticmethod
    def _config_writer(config, path, **kwargs):
        with open(path, 'r+') as conf_file:
            conf_file.write(json.dumps(config,
                                       indent=2,
                                       separators=(',', ': '),
                                       sort_keys=kwargs.get('sort_keys', True)))
            conf_file.truncate()

    def load(self):
        """Load all files found under conf, including cluster configurations"""
        # Load configuration files
        config_files = [conf for conf in os.listdir(self.config_path) if conf.endswith('.json')]
        for config_file in config_files:
            config_key = os.path.splitext(config_file)[0]
            file_path = os.path.join(self.config_path, config_file)
            self._config_reader(config_key, file_path)

        # Load cluster files
        for cluster_file in os.listdir(os.path.join(self.config_path, 'clusters')):
            config_key = os.path.splitext(cluster_file)[0]
            file_path = os.path.join(self.config_path, 'clusters', cluster_file)
            self._config_reader(config_key, file_path, cluster_file=True)

    def write(self):
        """Write the current config in memory to disk"""
        # Write loaded configuration files
        for config_key in [key for key in self.config if key != 'clusters']:
            file_path = os.path.join(self.config_path,
                                     '{}.json'.format(config_key))
            if config_key == 'logs':
                self._config_writer(self.config[config_key],
                                    file_path,
                                    sort_keys=False)
            else:
                self._config_writer(self.config[config_key], file_path)

        # Write loaded cluster files
        for cluster_key in self.config['clusters']:
            file_path = os.path.join(self.config_path,
                                     'clusters',
                                     '{}.json'.format(cluster_key))
            self._config_writer(self.config['clusters'][cluster_key],
                                file_path)
