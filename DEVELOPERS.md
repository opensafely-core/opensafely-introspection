# Notes for developers

## System requirements

### `just`

[`just`][1] is a handy way to save and run project-specific commands.
It's unrelated to the package with the same name on PyPI.

```sh
# macOS
brew install just

# Linux
# Install from https://github.com/casey/just/releases

# Show all available commands
just # Shortcut for just --list
```

## Reproducibility

opensafely-introspection uses [DVC][] (Data Version Control) to ensure the analysis is reproducible.
However, the database dump is not stored in [remote storage][2],
meaning that it should first be copied to *data/raw*.
Thereafter, set up a local development environment with

```sh
just devenv
```

Reproduce the pipeline with

  ```sh
  just run-dvc repro
  ```

## Development

Set up a local development environment with

```sh
just devenv
```

and create a new branch.
Then, iteratively:

* Make changes to the code
* Check the code for issues with

  ```sh
  just check
  ```

* Fix any issues with

  ```sh
  just fix
  ```

* Run the tests with

  ```sh
  just test
  ```

* Reproduce the pipeline with

  ```sh
  just run-dvc repro
  ```

* Commit the changes

Finally, push the branch to GitHub and open a pull request against the `main` branch.

[1]: https://github.com/casey/just/
[2]: https://dvc.org/doc/command-reference/remote
[DVC]: https://dvc.org/
