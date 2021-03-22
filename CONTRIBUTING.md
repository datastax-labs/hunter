# Setting up for development

* [Install poetry](https://python-poetry.org/docs/#installation)

* If you want to maintain your own virtualenv, [install pyenv and use pyenv virtualenv](https://datastax.jira.com/wiki/spaces/~741246479/pages/827785323/Coping+with+python+environments) to create and manage one.  Poetry will automatically find any active virtualenv and use that.

* Run poetry to install dependencies:

```
poetry install
```

* Run the development version of hunter using poetry:

```
poetry run hunter ...
```

See the [poetry docs](https://python-poetry.org/docs) for more.

# Running tests

```
poetry run pytest
```

...or using [tox](https://tox.readthedocs.io/):

```
ci-tools/tox-bootstrap
```

# Linting and formatting

Code-style is enforced using [black](https://black.readthedocs.io/).  Linting is automatically applied when tox runs tests; if linting fails, you can fix it with:

```
ci-tools/tox-bootstrap -e format
```


# Build a docker image

```
ci-tools/tox-bootstrap -e docker-build
```
