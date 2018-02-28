dev:
	pipenv lock -r > requirements.txt
	pipenv --three install '-e .' --dev

test:
	pipenv run nosetests
