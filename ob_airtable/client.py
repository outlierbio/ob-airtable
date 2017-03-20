import os
import os.path as op
import requests

AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_API_ENDPOINT = os.environ.get('AIRTABLE_API_ENDPOINT')

from .s3 import upload_to_s3_as_md5_hash

import logging


class AirtableClient(object):

    def __init__(self, endpoint=None, api_key=None):
        self.api_key = api_key or AIRTABLE_API_KEY
        self.endpoint = endpoint or AIRTABLE_API_ENDPOINT
        
        if not self.api_key:
            raise ValueError(
                'Airtable API key must be passed as constructor kwarg (api_key)'
                ' or environment variable (AIRTABLE_API_KEY)'
            )
        if not self.endpoint:
            raise ValueError(
                'Airtable API endpoint must be passed as constructor kwarg (endpoint)'
                ' or environment variable (AIRTABLE_API_ENDPOINT)'
            )
        self.endpoint = self.endpoint if self.endpoint.endswith('/') else self.endpoint + '/'

    def _request(self, method, table, path, params={}, **kwargs):
        headers = {'Authorization': 'Bearer ' + self.api_key}
        url = self.endpoint + table + path

        response = requests.request(method.upper(), url, headers=headers, params=params, **kwargs)
        response.raise_for_status()
        content = response.json()

        if method.upper() == 'GET' and url.endswith('/'):
            params = {}
            records = content['records']

            # Loop to get additional records from pagination
            while 'offset' in content:
                logging.debug('retrieving page, offset: {}'.format(content['offset']))
                params['offset'] = content['offset']
                response = requests.request('GET', url, headers=headers, params=params, **kwargs)
                response.raise_for_status()
                content = response.json()
                records += content['records']

            # Add full records list back
            content['records'] = records

        return content

    def get_record_ids(self,table, name=True):
        params = {
            'fields[]': 'Name',
            'pageSize': 100
        }
        records = self._request('get', table, '/', params=params)['records']

        if name:
            return [rec['fields']['Name'] for rec in records]
        else: 
            return [rec['id'] for rec in records]

    def get_record_fields(self, fields, table):
        params = {'fields[]': fields}
        return self._request('get', table, '/', params=params)['records']

    def find_record_id(self, name, table):
        params = {
            'filterByFormula': '{Name} = "%s"' % name,
        }
        content = self._request('get', table, '/', params=params)
        return content['records'][0]['id']

    def get_record(self, key, table):
        return self._request('get', table, '/' + key)

    def get_record_by_name(self, name, table):
        key = self.find_record_id(name, table)
        return self.get_record(key, table)

    def update_record(self, key, table, new_record):
        # Update record on Airtable
        return self._request('patch', table, '/' + key, json=new_record)

    def get_attachment_url(self, name, table, field, index=0):
        record_key = self.find_record_id(name, table)
        attachments = self.get_record(record_key, table)['fields'][field]
        return attachments[index]['url']

    def post_attachment(self, fpath, table, name, field, valid_fields=None):
        """Post an attachment to the given table, record and field
        
        Given a local filepath, post the file as an attachment to the 
        record with the given name, using the given table and field. 
        """

        # Upload to S3, keyed by content, get public URL
        url = upload_to_s3_as_md5_hash(fpath)
        ext = op.splitext(fpath)[1]

        # Create attachment object and add to record
        label = field.replace(' ', '_').lower()
        attachment = {
                'url': url,
                'filename': '{}_{}{}'.format(name, label, ext)
        }
        new_record = {'fields': {field: [attachment]}}
        
        # Post to Airtable record, OVERWRITING previous data. To append
        # instead of overwrite, get existing record and include all
        # attachment objects in new_record.
        record_key = self.find_record_id(name, table)
        response = self.update_record(record_key, table, new_record)

        return response


def update_if_missing(records, field, required_field, function):        
    """Check records for data and generate if missing

    Loop through records, checking for non-existence of `field` and
    existence of `required_field`. If both criteria are met, run the
    provide function on the record Name
    """
    for rec in records:
        name = rec['fields'].get('Name')
        if not name:
            continue
        if field in rec['fields']: 
            logging.debug('Record {} already has "{}".'.format(name, field))
            continue

        if required_field not in rec['fields']:
            logging.debug('Record {} is missing "{}".'.format(name, required_field))
            continue

        logging.info('Updating {} from {} for {}'.format(field, required_field, name))
        try:
            function(name)
        except Exception as e:
            logging.warning(e)
