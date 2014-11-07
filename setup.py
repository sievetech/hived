import os
from setuptools import setup, find_packages

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
    packages=find_packages(),
    install_requires=['amqp==1.4.5', 'simplejson==2.6.2'],
    test_suite='tests',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
