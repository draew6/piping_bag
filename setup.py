from setuptools import setup, find_packages

setup(
    name='piping_bag',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        "pydantic==2.5.3",
        "asyncpg == 0.29.0",
        "aiosqlite==0.19.0",
        "pysqlite3-binary==0.5.2.post2"
    ],
)
