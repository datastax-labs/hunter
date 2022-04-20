# Setting up for development

* Ensure that `python3` points to a version of python >= 3.8 (`python3 --version` will tell you).  If it does not, use [pyenv](https://github.com/pyenv/pyenv) to install a recent python version.

* There are two wrappers (`poetryw` and `toxw`) that install and run the correct versions of [poetry](https://python-poetry.org) and [tox](https://tox.wiki) for you; respectively.

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
./poetryw run pytest tests
```

...or using [tox](https://tox.readthedocs.io/):

```
./toxw
```

# Linting and formatting

Code-style is enforced using [black](https://black.readthedocs.io/).  Linting is automatically applied when tox runs tests; if linting fails, you can fix it with:

```
./toxw -e format
```


# Build a docker image

```
./toxw -e docker-build
```
