echo -e "[distutils]
index-servers=pypi

[pypi]
username = $PYPI_USER
password = $PYPI_PASSWORD
" > ~/.pypirc

pipenv lock -r > requirements.txt
echo $CIRCLE_TAG > VERSION.txt
python setup.py sdist
twine upload dist/nyuki-$CIRCLE_TAG.tar.gz
