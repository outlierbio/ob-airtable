import os
import requests

AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
API_ENDPOINT = os.environ.get('AIRTABLE_API_ENDPOINT')

from .s3 import upload_to_s3_as_md5_hash

import logging

def _request(method, table, path, **kwargs):
    headers = {'Authorization': 'Bearer ' + AIRTABLE_API_KEY}
    url = API_ENDPOINT + table + path

    response = requests.request(method.upper(), url, headers=headers, 
                                **kwargs)
    response.raise_for_status()
    content = response.json()
    return content


def get_record_ids(table, name=True):
    params = {'fields[]': 'Name'}
    records = _request('get', table, '/', params=params)['records']
    if name:
        return [rec['fields']['Name'] for rec in records]
    else: 
        return [rec['id'] for rec in records]


def get_record_fields(fields, table):
    params = {'fields[]': fields}
    return _request('get', table, '/', params=params)['records']


def find_record_id(name, table):
    params = {
        'filterByFormula': '{Name} = "%s"' % name,
    }
    content = _request('get', table, '/', params=params)
    return content['records'][0]['id']


def get_record(key, table):
    return _request('get', table, '/' + key)


def get_record_by_name(name, table):
    key = find_record_id(name, table)
    return get_record(key, table)


def update_record(key, table, new_record):
    # Update record on Airtable
    return _request('patch', table, '/' + key, json=new_record)


def get_attachment_url(name, table, field, index=0):
    record_key = find_record_id(name, table)
    attachments = get_record(record_key, table)['fields'][field]
    return attachments[index]['url']


def post_attachment(fpath, table, name, field, valid_fields=None):
    """Post an attachment to the given table, record and field
    
    Given a local filepath, post the file as an attachment to the 
    record with the given name, using the given table and field. 
    """

    # Upload to S3, keyed by content, get public URL
    fig_url = upload_to_s3_as_md5_hash(fpath)

    # Create attachment object and add to record
    label = field.replace(' ', '_').lower()
    attachment = {
            'url': fig_url,
            'filename': '{}_{}.png'.format(name, label)
    }
    new_record = {'fields': {field: [attachment]}}
    
    # Post to Airtable record, OVERWRITING previous data. To append
    # instead of overwrite, get existing record and include all
    # attachment objects in new_record.
    record_key = find_record_id(name, table)
    response = update_record(record_key, table, new_record)

    return response


def update_if_missing(records, field, required_field, function):        
    """Check records for data and generate if missing

    Loop through records, checking for non-existence of `field` and
    existence of `required_field`. If both criteria are met, run the
    provide function on the record Name
    """
    for rec in records:
        name = rec['fields']['Name']
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
