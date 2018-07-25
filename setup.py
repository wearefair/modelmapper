import re
from setuptools import setup, find_packages

VERSIONFILE = "modelmapper/__init__.py"
with open(VERSIONFILE, "r") as the_file:
    verstrline = the_file.read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))


def get_reqs(filename):
    with open(filename, "r") as reqs_file:
        reqs = reqs_file.readlines()
        reqs = list(map(lambda x: x.replace('==', '>='), reqs))
    return reqs


reqs = get_reqs("requirements.txt")
loader_reqs = get_reqs("requirements-loader.txt")

try:
    with open('README.rst') as file:
        long_description = file.read()
except Exception:
    long_description = (
        "Model Mapper: Auto generate SQLalchemy models, cleaning "
        "and field normalization from your csv files!"
    )

setup(
    name='modelmapper',
    description=long_description,
    author='Sep Dehpour',
    url='https://github.com/wearefair/modelmapper',
    download_url='https://github.com/wearefair/modelmapper/tarball/master',
    author_email='sepd@fair.com',
    version=verstr,
    install_requires=reqs,
    dependency_links=[],
    extras={
        'loader': loader_reqs
    },
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
        "Programming Language :: Python :: 3.7",
        "Development Status :: 4 - Beta",
    ],
    entry_points='''
        [console_scripts]
        modelmapper=modelmapper.management:cli
    ''',

)
