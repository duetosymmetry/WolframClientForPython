#!/bin/bash
PYTHONPATH=".:$PYTHONPATH" uv run --with-requirements requirements.txt --with ../. sphinx-apidoc -o api ../wolframclient ../wolframclient/tests* && PYTHONPATH=".:$PYTHONPATH" uv run --with-requirements requirements.txt --with ../. sphinx-build -M html . _build && npx sass ./wri_theme/static/mma.scss > "./_build/html/_static/mma.css"
