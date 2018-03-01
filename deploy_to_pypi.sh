VERSION_TAG=$1

if [ ! -z $VERSION_TAG ]; then
  echo -e "[distutils]
  index-servers=pypi

  [pypi]
  username = $PYPI_USER
  password = $PYPI_PASSWORD
  " > ~/.pypirc

  pipenv lock -r > requirements.txt
  echo $VERSION_TAG > VERSION.txt
  python setup.py sdist
  twine upload dist/nyuki-$VERSION_TAG.tar.gz
else
  echo "No VERSION_TAG defined, skipping packaging and upload."
  exit 1;
fi
