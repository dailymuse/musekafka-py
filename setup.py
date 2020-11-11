#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="musekafka",
    use_scm_version={"write_to": "src/musekafka/_version.py"},
    setup_requires=["setuptools_scm"],
    description="Python client to simplify work with Kafka consumers and producers.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    author="Ian Axelrod",
    author_email="ian@themuse.com",
    maintainer="The Muse",
    maintainer_email="dev@themuse.com",
    project_urls={
        "Source": "https://github.com/dailymuse/musekafka-py",
    },
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
    ],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=["confluent-kafka[avro]>=1.4.1", "muselog>=2.1.1"],
)
