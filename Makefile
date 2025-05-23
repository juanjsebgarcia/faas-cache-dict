PROD_PYPI=""  # Implicit
TEST_PYPI="https://test.pypi.org/legacy/"

lint:
	pre-commit run --all-files

test:
	pytest -s --pdb .

release-test:
	eval 'scripts/release.sh $(TEST_PYPI)'

release-prod:
	eval 'scripts/release.sh $(PROD_PYPI)'
