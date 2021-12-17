VERSION=$(python -c 'import faas_cache_dict; print(faas_cache_dict.__version__)')

python -m pip install pip wheel twine

python setup.py bdist_wheel

python -m twine upload --repository-url "$@" dist/*

rm -rf dist build *.egg-info
