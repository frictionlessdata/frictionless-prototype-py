import os
import io
from setuptools import setup, find_packages


# Helpers
def read(*paths):
    """Read a text file."""
    basedir = os.path.dirname(__file__)
    fullpath = os.path.join(basedir, *paths)
    contents = io.open(fullpath, encoding='utf-8').read().strip()
    return contents


# Prepare
PACKAGE = 'frictionless'
NAME = PACKAGE.replace('_', '-')
CORE_REQUIRES = [
    'click>=6.6',
    'requests>=2.10',
    'jsonschema>=2.5',
    'stringcase>=1.2',
    'tabulator>=1.52',
    'datapackage>=1.14',
]
PROB_REQUIRES = [
    'statistics>=1.0',
]
RULE_REQUIRES = [
    'simpleeval>=0.9',
]
SERVER_REQUIRES = [
    'gunicorn>=20',
]
DEVOPS_REQUIRES = [
    'mypy',
    'black',
    'pylama',
    'pytest',
    'pytest-cov',
    'coveralls',
    'ipython',
]
README = read('README.md')
VERSION = read(PACKAGE, 'assets', 'VERSION')
PACKAGES = find_packages(exclude=['tests'])
ENTRY_POINTS = {'console_scripts': ['frictionless = frictionless.__main__:program']}


# Run
setup(
    name=NAME,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=CORE_REQUIRES,
    tests_require=DEVOPS_REQUIRES,
    extras_require={
        'prob': PROB_REQUIRES,
        'rule': RULE_REQUIRES,
        'server': SERVER_REQUIRES,
        'devops': DEVOPS_REQUIRES,
    },
    entry_points=ENTRY_POINTS,
    zip_safe=False,
    long_description=README,
    long_description_content_type='text/markdown',
    description='Frictionless is a data framework',
    author='Open Knowledge Foundation',
    author_email='info@okfn.org',
    url='https://github.com/frictionlessdata/frictionless-py',
    license='MIT',
    keywords=[
        'data validation',
        'frictionless data',
        'open data',
        'json schema',
        'json table schema',
        'data package',
        'tabular data package',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
