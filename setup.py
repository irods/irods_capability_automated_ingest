from setuptools import setup, find_packages
import codecs
from os import path

# Get package version
version = {}
here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'irods_capability_automated_ingest/version.py')) as f:
    exec(f.read(), version)

# Get the long description from the README file
with codecs.open(path.join(here, 'README.md'), 'r', 'utf-8') as f:
    long_description = f.read()

setup(
    name='irods-capability-automated-ingest',
    version=version['__version__'],
    description='Implement filesystem scanners and landing zones',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/irods/irods_capability_automated_ingest',
    author='iRODS Consortium',
    author_email='support@irods.org',
    license='BSD',
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.5',
        'Operating System :: POSIX :: Linux'
    ],
    keywords='irods automated ingest landingzone filesystem',
    packages=find_packages(),
    install_requires=[
        'minio',
        'flask',
        'flask-restful',
        'python-irodsclient>=0.8.0',
        'python-redis-lock>=3.2.0',
        'redis>=2.10.6',
        'celery[redis]',
        'scandir',
        'structlog>=18.1.0',
        'progressbar2',
        'billiard'
    ],
    setup_requires=['setuptools>=38.6.0'],
    entry_points={
        'console_scripts': [
            'irods_capability_automated_ingest=irods_capability_automated_ingest.irods_sync:main'
        ],
    },
    project_urls={
        'Bug Reports': 'https://github.com/irods/irods_capability_automated_ingest/issues',
        'Source': 'https://github.com/irods/irods_capability_automated_ingest'
    },
)
