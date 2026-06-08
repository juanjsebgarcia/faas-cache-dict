from setuptools import find_packages, setup

from faas_cache_dict import __version__

# Retrieve long description from README file
with open("README.md") as readme_file:
    long_description = readme_file.read()

# Retrieve requirements to avoid duplication
with open("requirements.txt") as requirements_file:
    install_requires = requirements_file.read().splitlines()

setup(
    name="faas-cache-dict",
    version=__version__,
    description=(
        "Thread-safe Python in-memory cache dict with LRU eviction, TTL expiry & "
        "memory size limits — built for FaaS / AWS Lambda"
    ),
    keywords=(
        "lru-cache ttl-cache in-memory-cache cache dictionary thread-safe "
        "serverless aws-lambda faas python lru ttl memory-bounded"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    author="Juan Garcia Alvite",
    url="https://github.com/juanjsebgarcia/faas-cache-dict",
    packages=find_packages(
        exclude=(
            "tests",
            "scripts",
        )
    ),
    python_requires=">=3.10",
    license="MIT",
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Caching",
    ],
)
