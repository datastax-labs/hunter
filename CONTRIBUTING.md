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
