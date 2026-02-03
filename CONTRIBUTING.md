# Contributions

The `CONTRIBUTING.md` file is a development guide for anyone wishing to contribute to the project. Below are the key points covered:

- **Git Versioning & Gitflow**: Guidelines for branching, committing, and versioning using Git and the Gitflow workflow.
- **Coding Style with Ruff**: Standards for writing code, enforced with the Ruff linter to ensure consistency and readability.
- **Unit Testing with Pytest**: Instructions on writing and running unit tests using Pytest to ensure code quality and functionality.
- **Documentation with Sphinx**: Guidelines for documenting the codebase and project using Sphinx, ensuring comprehensive and accessible documentation.
- **CI/CD Pipelines**: Information on the CI/CD pipelines, detailing how to set up, manage, and utilize automated testing, building, and deployment processes.


---

# Versioning

In this project, we use Git for version control and Gitflow as a workflow strategy to organize development efficiently. Gitflow is a version management strategy designed to help software development teams manage the different phases of the development lifecycle. For more details, you can refer to Vincent Driessen's article, the creator of [Gitflow](https://nvie.com/posts/a-successful-git-branching-model/).

## Main Project Branches

The project has two main branches:

- `main`: The main branch containing the stable version of the project.
- `develop`: The development branch where all features, updates, and fixes are integrated before being released.

## Developing New Features

For a new feature, create a `feature` branch from `develop`. Once the feature is complete, merge it back into `develop`.

## Git Version Management

- Adherence to [Git Flow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow):
  - A common "develop" branch for ongoing development
    - Each feature branch starts from the "develop" branch
  - A "main" branch for production

- Commit Management:
  - Only "useful" commits (avoid a series of "Fix TU" commits)
  - Commit messages in English
Here’s the translated section for “Style” and “Development Process” for your `CONTRIBUTING.md` file:

---

## Style

To ensure code style consistency, the project uses [Ruff](https://github.com/astral-sh/ruff). The maximum line length has been increased to 140 characters to encourage explicit method names. A pre-commit hook is also set up to check that code is properly formatted before each commit. You can install and use it locally to ensure your code is correctly formatted:

```bash
pip install ruff
ruff check --fix
ruff format
```

### Pre-commit

Pre-commit helps ensure that code is formatted correctly before each commit and will pass the formatting CI. Its configuration is defined in the `.pre-commit-config.yaml` file. It is necessary to install it for the project the first time.

First, install the Python package locally:

```bash
pip install pre-commit
```

Then:

```bash
pre-commit install
```

To ensure that you pass the pre-commit checks, you can run the following command:

```bash
pre-commit run --all-files
```

### Specific Rules

- Business-related comments should be in French; other comments in English.
- Avoid excessive comments: Python is expressive, and well-named methods usually make the code self-explanatory.
- Avoid code duplication; aim to reuse or refactor.
  - Follow the principle: Make it Work, Make it Right, Make it Fast.
- Take the time to name everything correctly.
- Avoid global `except` or `except Exception`.
- Functions should not be too long (~30 lines).
- Files should not be too long (~300 lines).
- For strings with variables, use f-strings rather than `%` or `format`:
  ```python
  example = f'Text with {a_variable} using f-strings.'
  ```
- Hardcoded strings should be extracted into constants if reused.
- Variable names should be relevant and explicit.
- Watch out for dead code!

## Development Process

- For medium to large projects, create a Design and Construction Document (DCT) before starting development.
- When a technical decision needs to be made (e.g., choosing one library over another), write an Architecture Decision Record (ADR).
- The functionality should be tested locally by the developer.
- If the installation or usage


---
Here’s the translated and updated section for “Project Configuration (`pyproject.toml`)” and “GitLab CI/CD Configuration” in your `CONTRIBUTING.md` file:

## Project Configuration (`pyproject.toml`)

The file `pyproject.toml` is the cornerstone of your Python package configuration.

### Project Section

```toml
[project]
name = "ckanapi_harvesters"
version = "0.0.0"
authors = [
  { name = "AUTHOR", email = "author@example.com" }
]
description = "DESCRIPTION"
readme = "README.md"
requires-python = ">=3.10"
classifiers = [
    "Programming Language :: Python :: 3.10",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
]
dependencies = [
    "pytest == 8.0.1",
    # add necessary dependencies here
]
```

### Setuptools Configuration

```toml
[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools_scm]
version_scheme = "guess-next-dev"
local_scheme = "dirty-tag"
```

### Tools and Linting with Ruff

```toml
[tool.ruff]
line-length = 140
exclude = ["tests"]

[tool.ruff.lint]
extend-select = [
  "UP",
  "E501",
  "I",
  "B",
  "F",
  "E",
  "N",
  "A",
  "PL",
  "D"
]

[tool.ruff.lint.pydocstyle]
convention = "numpy"

[tool.ruff.format]
quote-style = "single"
indent-style = "space"
docstring-code-format = true
```

This documentation reflects the current configuration of your `pyproject.toml` file, focusing on specific aspects of your project such as package management, Python compatibility, and integration with linting and formatting tools. Be sure to adjust the `name`, `version`, `authors`, `description`, etc., fields according to the specific needs of your project.

---

## GitLab CI/CD Documentation

### CI/CD Job Descriptions

**GitLab CI/CD:**

- **`ruff_guardian`**: Checks that the code is properly formatted. If errors are found, it reformats the code.
- **`pytest`**: Runs unit tests defined in the `tests` folder and generates coverage reports.
- **`pages`**: Generates and publishes documentation on GitLab Pages.
- **`py_package_uploader`**: Uploads the package to the Nexus repository.

### Required Environment Variables

To ensure the CI/CD pipelines function correctly, the following environment variables need to be set:

- `NEXUS_USERNAME`: Username for the Nexus repository.
- `NEXUS_PASSWORD`: Password for the Nexus repository.
- `PYTHON_VERSION`: Python version used for the package (e.g., 3.10).
- `UPLOAD_REPOSITORY`: Nexus repository to which the package is uploaded (e.g., `https://nexus.fastit.dev/repository/fast-it/`).

> **⚠️ Warning:** It is crucial not to store passwords in plain text in the `.gitlab-ci.yml` file. These should be set as environment variables in the GitLab repository.

### Specific Stage Details

- **`ruff_guardian`**: Uses a Python slim image to install and run `ruff`. If formatting issues are detected, the code is reformatted.
- **`pytest`**: Uses a Python image to run unit tests with `pytest`, generating artifacts and coverage reports.
- **`pages`**: Uses Sphinx to generate the documentation and publish it on GitLab Pages.
- **`py_package_uploader`**: Prepares and uploads the Python package based on the project name and version tagged in GitLab.

---

## Creating a Release

### Manually

In your Python virtual environment, install the `bump-my-version` utility:

```shell
pip install --upgrade bump-my-version
```

The `bump-my-version` utility helps version the project efficiently and allows for pre-release versions.

To get an overview of available version changes for the project, use the following command:

```shell
bump-my-version show-bump
```

For example, if the current version of the project is 0.0.0:

```bash
bump-my-version show-bump
```
```
0.0.0 ── bump ─┬─ major ─ 1.0.0
               ├─ minor ─ 0.1.0
               ├─ patch ─ 0.0.1
               ├─ pre_l ─ 0.0.0-rc0
               ╰─ pre_n ─ 0.0.0-dev1
```

This indicates that if you increment the major version, the project will move to 1.0.0, or if you increment the minor version, it will move to 0.1.0, and so on.

To upgrade the version of your project based on the associated keyword, run the following command at the root of your project:

```bash
bump-my-version bump <major|minor|patch|pre_l|pre_n>
```

For example:

```bash
bump-my-version bump minor
```

### In the GitLab Pipeline

To create a release, create a new tag on the master branch with the new version as the tag name. GitLab CI will then automatically build the package and redeploy it to Nexus.

> **NOTE:** Tags must follow the Python versioning format `X.Y.Z` (e.g., `1.2.3`) <br>
> X for the major version, Y for the minor version, and Z for the patch version.

Here is the translated and updated section for “Managing Dependencies”, “Updating Documentation”, and “Installing and Testing the Package” in your `CONTRIBUTING.md` file:

---

## Managing Dependencies

To add a dependency to the project, modify the `pyproject.toml` file:

- If the dependency is essential for the core functionality of the application, add it to the `[project] > dependencies` section.
- If the dependency is only required for the production version, add it to the `[project.optional-dependencies] > production` section.

The Dockerfiles handle the installation of dependencies based on the environment (development or production), simplifying the dependency management process.

## Updating Documentation

#### Contributing

To contribute to the documentation, place your `.md` or `.rst` files in the `sphinx` folder.

To set up automatic documentation, create an `.rst` file similar to the example below:

```rst
auto docstring for package
===========================

   This is my package.

   .. autopackagesummary:: ckanapi_harvesters
      :toctree: ckanapi_harvesters
      :template: autosummary/package.rst

.. toctree::
   :glob:

   ckanapi_harvesters/*
 ```

#### Compilation

To compile the documentation, run the following commands:

```shell
pip install sphinx sphinx-rtd-theme
sphinx-build -b html ./sphinx ./public
```

### Installing and Testing the Package

Before merging into `develop`, ensure that your package is working correctly by running local tests with `pytest`. To install the package locally, use the following commands:

```bash
pip install build
python -m build
pip install dist/ckanapi_harvesters-0.0.0-py3-none-any.whl
pytest
```

Replace `ckanapi_harvesters` with the name of your repository. Once installed, the package becomes part of your dependencies, and you can use it in your code with `import ckanapi_harvesters` or `from ckanapi_harvesters import module`.

