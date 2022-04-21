# Setting up for development

* Ensure that `python3` points to a version of python >= 3.8 (`python3 --version` will tell you).  If it does not, use [pyenv](https://github.com/pyenv/pyenv) to both install a recent python version and make it your current python.

* There are two wrappers (`poetryw` and `toxw`) that install and run the correct versions of [poetry](https://python-poetry.org) and [tox](https://tox.wiki) for you.

* Run poetry to install dependencies:

```
./poetryw install
```

* Run the development version of hunter using poetry:

```
./poetryw run hunter ...
```

See the [poetry docs](https://python-poetry.org/docs) for more.

# Running tests

```
./poetryw run pytest
```

...or using [tox](https://tox.readthedocs.io/):

```
./toxw
```

# Linting and formatting

Code-style is enforced using [black](https://black.readthedocs.io/) and [flake8](https://flake8.pycqa.org/); import optimisation is handled by [isort](https://pycqa.github.io/isort/) and [autoflake](https://pypi.org/project/autoflake/).  Linting is automatically applied when tox runs tests; if linting fails, you can fix trivial problems with:

```
./toxw -e format
```


# Build a docker image

```
./toxw -e docker-build
```
