# Contribution Guide

## General Guideline

We use Github as a code and issues hosting platform. To report a bug or propose a new feature please open an issue. For pull requests we would ask you initially create an issue and then create a pull requests linked to this issue.

## Docs Contribution

We use a mixed documentation system as it's generated from:
- Google Colab [notebooks](https://drive.google.com/drive/folders/1boOu13YdhGkPOYiKe6KBkRmkYaaBbcsH?usp=sharing)
- markdown documents in the `docs` directory
- source code using some scripts

The simplest way to contribute is leaving comments on a Google Colab document from the directory mentioned above. You don't need to setup a development environment for it and it's the fastest way. If you'd like to work with text files you can contribute to:
- `docs` directory cotaining sources for the documentation
- `docs/build` directory containing the built documentation

It's OK to propose changes to generated files in the `docs/build` directory as we will move the changes to the corresponding Google Colab.

### Building Process

In the `docs` directory we have 3 types of documentation sources:
- `*.md` - markdown documents which will be just copied to the `docs/build`
- `*.nb` - links to corresponding Google Colab guides to download and convert
- `*.py` - scripts to generate, e.g. corresponding references

You can run `make docs` to build documentation after you have setup a development environment as it's described in the next section.

## Code Contribution

Frictionless is a Python3.6+ framework and it uses some basically standard Python tools for the development process:
- testing: `pytest`
- linting: `pylama`
- formatting: `black`
- type checking: `mypy` (under construction)

It's commonplace but, of course, you need `git` to work on the project, also `make` is recommended.

### Development Environment

After cloning the repository it's recommended to create a virtual environment and install the dependencies:

> it will install a `git commit` hook running the tests

```bash
python3.8 -m venv .python
source .python/bin/activate
pip install wheel
make install
alias "frictionless=python -m frictionless"
```

Then you can run various make commands:
- `make docs` - build the docs
- `make format` - format source code
- `make install` - install the dependencies (we did before)
- `make lint` - lint the project
- `make release` - release a new version
- `make test` - run the tests

Of course, it's possible and recommended to run underlaying commands like `pytest` or `pylama` to speed up the development process.

## Releasing to PyPi

To release a new version:
- check that you have push access to the `master` branch
- update `frictionless/assets/VERSION` following the SemVer standard
- add changes to `CHANGELOG.md` if it's not a patch release (major or micro)
- run `make release` which create a release commit and tag and push it to Github
- an actual release will happen on the Travis CI platform after running the tests
