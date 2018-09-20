from setuptools import setup, find_packages

version = 'v0.3.0'

with open('README.rst') as file:
    long_description = file.read()

setup(
    name='pdp',
    version=version,

    description='Build fast data processing pipelines easily',
    long_description=long_description,

    url='https://github.com/Egor-Krivov/pdp',
    download_url='https://github.com/Egor-Krivov/pdp/archive/{}.tar.gz'.format(version),

    author='Egor-Krivov',
    author_email='e.a.krivov@gmail.com',
    license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],

    python_requires='>=3.5',
    keywords='pipeline parallel thread data processing augmentation',

    packages=find_packages(exclude=['examples', 'tests', 'prototypes']),
    include_package_data=True,

    install_requires=[]
)
