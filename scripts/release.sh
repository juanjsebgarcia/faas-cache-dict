python -m pip install pip wheel twine

python setup.py bdist_wheel

python -m twine upload --verbose --repository-url "$@" dist/*

rm -rf dist build *.egg-info
