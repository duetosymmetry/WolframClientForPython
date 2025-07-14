# Documentation Building

This directory contains the documentation source files for the Wolfram Client Library for Python. This readme was generated using AI.

## Requirements

- **uv** - Fast Python package manager and project runner
- **Node.js** (optional) - For CSS compilation via npx sass

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install Node.js (optional)

```bash
# macOS
brew install node

# Or download from https://nodejs.org/
```

## Building Documentation

### Local Development (using uv)

For local development, use the simple build script:

```bash
./build.sh
```

This script uses `uv` to manage dependencies and builds the documentation in one command.

### CI/Production Build

The `build_docs.sh` script is used in CI environments and supports additional options:

```bash
./build_docs.sh
```

#### Clean Build (recommended after code changes)

```bash
./build_docs.sh --all
```

#### Build to Custom Directory

```bash
./build_docs.sh --build /path/to/output/directory
```

## Output

The generated HTML documentation will be available in:
- `_build/html/index.html` (default)
- Or your specified output directory

## Dependencies

The build process uses the dependencies specified in `requirements.txt`:
- Pygments 2.19.1 (syntax highlighting)
- pygments-mathematica 0.4.2 (Wolfram Language lexer)
- Sphinx (documentation generator)

Both scripts automatically install these dependencies and the wolframclient package. The `build.sh` script uses `uv` for faster dependency resolution, while `build_docs.sh` uses traditional pip in virtual environments for CI compatibility.

## Troubleshooting

- **Missing Node.js/npx**: CSS compilation will be skipped with a warning, but documentation will still build
- **Missing uv**: The script will exit with installation instructions
- **Import errors**: Some optional dependencies (pandas, PIL, etc.) may show warnings but don't affect the build
