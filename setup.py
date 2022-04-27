from setuptools import find_packages, setup

from faas_cache_dict import __version__

# Retrieve long description from README file
with open('README.md') as readme_file:
    long_description = readme_file.read()

# Retrieve requirements to avoid duplication
with open('requirements.txt') as requirements_file:
    install_requires = requirements_file.read().splitlines()

setup(
    name='faas-cache-dict',
    version=__version__,
    description=(
        'A Python dictionary implementation designed to act as an in-memory cache '
        'for FaaS environments'
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',
    include_package_data=True,
    author='Juan Garcia Alvite',
    url='https://github.com/juanjsebgarcia/faas-cache-dict',
    packages=find_packages(exclude=('tests', 'scripts',)),
    python_requires='>=3.8',
    license='MIT',
    install_requires=install_requires,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
    ],
)
