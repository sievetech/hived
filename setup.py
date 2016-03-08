import sys
import os
from setuptools import setup

from hived import __version__

BASE_PATH = os.path.dirname(__file__)
setup(
    name='hived',
    version=__version__,
    description='Hived is a library for writing distributed daemons.',
    long_description=open(os.path.join(BASE_PATH, 'README.md')).read(),
    author='Dalton Barreto',
    author_email='daltonmatos@gmail.com',
    url='https://github.com/sievetech/hived',
    packages=['hived'],
    entry_points={'console_scripts': ['run_module = hived.run_module:main']},
    install_requires=['amqp==1.4.7', 'simplejson>=2.6.2', 'raven>=5.3.1',
                      'six==1.10.0'],
    test_suite='tests',
    tests_require=['tox>=2.3.1'] + (
        ['mock==1.3.0'] if sys.version_info.major == 2 else []
    ),
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
