from setuptools import setup, find_packages

setup(
    name='servicepackage',
    version='0.1.0',
    packages=['servicepackage'],
    install_requires=[
        'pymongo==4.13.1',
        'pika==1.3.2',
        'python-dotenv==1.1.0',
        'protobuf==3.20.3'
    ],
    python_requires='>=3.7',
    author='techvantage.ai',
    description='Service package framework',
    long_description = open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
)
