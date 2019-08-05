files := ./featurestore/clients/*.py ./featurestore/lambda/*.py

lint: $(files)
	black $(files)
	flake8 $(files)
	mypy --ignore-missing-imports $(files)
	pytype $(files)


test:
	python3 -m pytest --cov=featurestore -vv


clean:
	rm -rf ./build ./dist ./*.pyc /*.tgz ./*.egg-info */*.egg-info


build: clean lint test
	python3 setup.py sdist bdist_wheel


upload-pypi: build
	s3pypi --verbose --private --bucket pypi.featurestore.com


update-lambda: clean lint
	mkdir -pv ./dist
	cd ./featurestore/lambda/; zip -D ../../dist/lambda.zip lambda.py
	zip_loc="fileb://`realpath ./dist/lambda.zip`"; \
	aws lambda update-function-code --function-name featurestore-lambda --zip-file $$zip_loc --no-publish


init:
	pipenv install --dev


tools:
	pip3 install pipenv --upgrade
	pip3 install s3pypi --upgrade
	pip3 install black --upgrade
	pip3 install flake8 --upgrade
	pip3 install mypy --upgrade
	pip3 install pytype --upgrade
	pip3 install aws --upgrade
	pip3 install pytest --upgrade
	pip3 install pytest-cov --upgrade
