"""
Copyright 2017-present, Airbnb Inc.

Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an 'AS IS' BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import json

from nose.tools import assert_equal

from stream_alert_cli.config import CLIConfig
from tests.unit.helpers.base import mock_open

def test_load_config():
    """CLI - Load config"""
    config_data = {
        'global': {
            'account': {
                'aws_account_id': 'AWS_ACCOUNT_ID_GOES_HERE',
                'kms_key_alias': 'stream_alert_secrets',
                'prefix': 'unit-testing',
                'region': 'us-west-2'
            },
            'terraform': {
                'tfstate_bucket': 'PREFIX_GOES_HERE.streamalert.terraform.state',
                'tfstate_s3_key': 'stream_alert_state/terraform.tfstate',
                'tfvars': 'terraform.tfvars'
            },
            'infrastructure': {
                'monitoring': {
                    'create_sns_topic': True
                }
            }
        },
        'lambda': {
            'alert_processor_config': {
                'handler': 'stream_alert.alert_processor.main.handler',
                'source_bucket': 'PREFIX_GOES_HERE.streamalert.source',
                'source_current_hash': '<auto_generated>',
                'source_object_key': '<auto_generated>',
                'third_party_libraries': []
            },
            'rule_processor_config': {
                'handler': 'unit-test.handler',
                'source_bucket': 'PREFIX_GOES_HERE.streamalert.source',
                'source_current_hash': '<auto_generated>',
                'source_object_key': '<auto_generated>',
                'third_party_libraries': [
                    'jsonpath_rw',
                    'netaddr'
                ]
            }
        }
    }

    # Use a string to retain the order
    log_data = '''
{
    "json_test": {
        "parser": "json",
        "schema": {
            "key_01": "integer",
            "key_02": "string"
        }
    },
    "csv_test": {
        "parser": "csv",
        "schema": {
            "key1": [],
            "key2": "string",
            "key3": "integer",
            "key9": "boolean",
            "key10": {},
            "key11": "float"
        }
    }
}
'''

    global_file = 'tests/unit/conf/global.json'
    global_contents = json.dumps(config_data['global'], indent=2)

    lambda_file = 'tests/unit/conf/lambda.json'
    lambda_contents = json.dumps(config_data['lambda'], indent=2)

    logs_file = 'tests/unit/conf/logs.json'

    with mock_open(global_file, global_contents):
        with mock_open(lambda_file, lambda_contents):
            with mock_open(logs_file, log_data):
                config = CLIConfig(config_path='tests/unit/conf')

                assert_equal(config['global']['account']['prefix'], 'unit-testing')
                assert_equal(config['lambda']['rule_processor_config']['handler'],
                             'unit-test.handler')
                assert_equal(set(config['logs'].keys()),
                             {'json_test', 'csv_test'})

                # Check that the csv schema retains its order
                assert_equal(config['logs']['csv_test']['schema'].keys()[0], 'key1')
