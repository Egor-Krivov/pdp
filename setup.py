from setuptools import setup, find_packages

with open('README.rst') as file:
    long_description = file.read()

setup(
    name='pdp',
    version='0.1',

    description='Build fast data processing pipelines easily',
    long_description=long_description,

    url='https://github.com/Egor-Krivov/pdp',
    download_url='https://github.com/Egor-Krivov/pdp/archive/0.1.tar.gz',

    author='Egor-Krivov',
    author_email='e.a.krivov@gmail.com',
    license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    python_requires='>=3.5',
    keywords='pipeline parallel thread data processing augmentation',

    packages=find_packages(exclude=['tests', 'prototypes']),
    include_package_data=True,

    install_requires=[]
)
