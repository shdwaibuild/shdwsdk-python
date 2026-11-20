# Releasing

1. Bump the version in `pyproject.toml`, `src/shdwsdk/__init__.py` and `src/shdwsdk/client.py`, and add a `CHANGELOG.md` entry.
2. Build and upload from a clean checkout of `main`:

```bash
python -m pip install --upgrade build twine
rm -rf dist && python -m build
python -m twine check dist/*
python -m twine upload dist/*              # username: __token__, password: a PyPI API token
```

3. Tag the release: `git tag v<version> && git push --tags`
