from setuptools import setup, find_packages
import codecs
from os import path

# Get package version
version = {}
here = path.abspath(path.dirname(__file__))
with open(path.join(here, "irods_capability_automated_ingest/version.py")) as f:
    exec(f.read(), version)

# Get the long description from the README file
with codecs.open(path.join(here, "README.md"), "r", "utf-8") as f:
    long_description = f.read()

setup(
    name="irods-capability-automated-ingest",
    version=version["__version__"],
    description="Implement filesystem scanners and landing zones",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/irods/irods_capability_automated_ingest",
    author="iRODS Consortium",
    author_email="support@irods.org",
    license="BSD",
    python_requires=">=3.8,",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords="irods automated ingest landingzone filesystem",
    packages=find_packages(),
    install_requires=[
        "minio",
        "python-irodsclient<4.0.0",
        "python-redis-lock>=3.2.0",
        "redis>=3.4.1, <5.0.0",
        "celery[redis]<6.0.0",
        "structlog>=18.1.0",
        "progressbar2",
    ],
    setup_requires=["setuptools>=38.6.0"],
    entry_points={
        "console_scripts": [
            "irods_capability_automated_ingest=irods_capability_automated_ingest.irods_sync:main"
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/irods/irods_capability_automated_ingest/issues",
        "Source": "https://github.com/irods/irods_capability_automated_ingest",
    },
)
