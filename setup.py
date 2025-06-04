from setuptools import setup, find_packages

setup(
    name="elasticutils",
    version="0.2.0",
    description="A simple Elasticsearch Utilities library",
    author="Cybersilo",
    packages=find_packages(),
    install_requires=[
        "requests"
    ],
    python_requires=">=3.7",
)