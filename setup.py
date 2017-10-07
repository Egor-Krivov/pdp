from distutils.core import setup
from setuptools import find_packages

classifiers = """Development Status :: 5 - Production/Stable
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6"""

with open('README.md') as file:
    long_description = file.read()

setup(
    name='pdp',
    packages=find_packages(),
    include_package_data=True,
    version='0.1',
    description='Build fast data processing pipelines easily',
    long_description=long_description,
    author='Egor-Krivov',
    author_email='e.a.krivov@gmail.com',
    license='MIT',
    url='https://github.com/Egor-Krivov/pdp',
    download_url='https://github.com/Egor-Krivov/pdp/archive/0.1.tar.gz',
    keywords=['data processing', 'parallel', 'thread', 'data augmentation'],
    classifiers=classifiers.splitlines(),
    install_requires=[]
)
