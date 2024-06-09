from setuptools import setup, find_packages
import sys

install_requires = [
    "pydantic==2.7.3",
    "asyncpg == 0.29.0",
    "aiosqlite==0.19.0"
]

if sys.platform.startswith('linux'):
    install_requires.append("pysqlite3-binary==0.5.2.post2")

setup(
    name='piping_bag',
    version='0.1.5',
    packages=find_packages(),
    install_requires=install_requires,
)
