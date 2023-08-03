# Making a new release of sparglim

## Auto release

When releasing on github, both the python package and the docker image are automatically released

[Github Action](https://github.com/Wh1isper/sparglim/actions/workflows/publish.yml)

## Manual release

### Python package

This project can be distributed as Python
packages. Before generating a package, we first need to install `build`.

```bash
pip install build twine hatch
```

Bump the version using `hatch`. By default this will create a tag.
See the docs on [hatch-nodejs-version](https://github.com/agoose77/hatch-nodejs-version#semver) for details.

```bash
hatch version <new-version>
```

To create a Python source package (`.tar.gz`) and the binary package (`.whl`) in the `dist/` directory, do:

```bash
rm -rf dist/*
python -m build
```

> `python setup.py sdist bdist_wheel` is deprecated and will not work for this package.

Then to upload the package to PyPI, do:

```bash
twine upload dist/*
```


### Docker

```
docker build -t wh1isper/sparglim-server:latest -f docker/Dockerfile.sparglim-server .

docker build -t wh1isper/jupyterlab-sparglim:latest -f docker/Dockerfile.jupyterlab-sparglim .
```
