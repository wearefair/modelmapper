from setuptools import setup, find_packages

with open("requirements.txt") as reqs_file:
    reqs = reqs_file.readlines()

setup(
    name='modelmapper',
    description='Model Mapper: Auto generate SQLalchemy models and GRPC Prorotbufs from your csv files!',
    author='Sep Dehpour',
    url='https://github.com/wearefair/modelmapper',
    download_url='https://github.com/wearefair/modelmapper/tarball/master',
    author_email='sepd@fair.com',
    version='0.0.1',
    install_requires=reqs,
    dependency_links=[],
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True,
    scripts=[],
    test_suite="tests",
    tests_require=['mock'],
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
        "Programming Language :: Python :: 3.6",
        "Development Status :: 4 - Beta",
    ],
    entry_points='''
        [console_scripts]
        modelmapper=modelmapper.management:cli
    ''',

)
