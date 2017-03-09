from setuptools import setup, find_packages

setup(
    name='ob-airtable',
    author='Jacob Feala',
    author_email='jake@outlierbio.com',
    version='0.1',
    url='http://github.com/outlierbio/ob-airtable',
    packages=find_packages(),
    description='Airtable client with support for storing attachments to S3 and polling for updates',
    install_requires=[
        'requests'
    ]
)