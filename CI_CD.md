# CI/CD Pipeline Documentation

This document describes the automated testing and deployment pipelines configured for the ConnectedDrivingPipelineV4 project.

## Overview

The project uses **GitHub Actions** for continuous integration and continuous deployment (CI/CD). The pipeline consists of three main workflows:

1. **Test Suite** - Automated testing on every push and pull request
2. **Code Quality** - Linting and formatting checks
3. **Documentation** - Automated deployment of MkDocs documentation

---

## Workflows

### 1. Test Suite (`.github/workflows/test.yml`)

**Triggers:**
- Push to `main`, `master`, or `develop` branches
- Pull requests targeting `main`, `master`, or `develop` branches

**Jobs:**

#### 1.1 Unit Tests (`test`)
- **Python Versions:** 3.8, 3.9, 3.10, 3.11
- **Matrix Strategy:** Tests run in parallel across all Python versions
- **Steps:**
  1. Checkout repository
  2. Set up Python with pip caching
  3. Install system dependencies (gcc, gfortran, BLAS/LAPACK)
  4. Install Python dependencies from `requirements.txt`
  5. Validate Dask setup with `validate_dask_setup.py`
  6. Run pytest with coverage reporting
  7. Upload coverage to Codecov (Python 3.11 only)
  8. Archive HTML coverage report as artifact
  9. Check coverage threshold (70% minimum)

**Coverage Reports:**
- Terminal output with missing lines
- XML format for Codecov integration
- HTML report archived as workflow artifact (30-day retention)

**Coverage Threshold:**
- Minimum: 70% (configured in `.coveragerc`)
- Warning issued if below threshold (non-blocking)

#### 1.2 Code Quality (`lint`)
- **Python Version:** 3.11
- **Tools:**
  - **flake8:** Python linting (critical errors fail, complexity warnings continue)
  - **black:** Code formatting check (continue on error)
  - **isort:** Import sorting check (continue on error)

**Linting Rules:**
- Critical errors (E9, F63, F7, F82) cause job failure
- Complexity warnings (max-complexity=10, max-line-length=127) are non-blocking
- Formatting issues are reported but don't block merges

#### 1.3 Integration Tests (`integration-test`)
- **Python Version:** 3.11
- **Test Markers:**
  - `integration`: End-to-end workflow tests
  - `slow`: Long-running tests (e.g., large dataset processing)

**Behavior:**
- Both integration and slow tests continue on error (non-blocking)
- Useful for identifying flaky tests without blocking CI

#### 1.4 Docker Build (`docker-build`)
- **Steps:**
  1. Build Docker image from `Dockerfile`
  2. Test image by running `validate_dask_setup.py` inside container
  3. Validate Docker Compose configurations (dev and prod)

**Validation:**
- Ensures Docker image builds successfully
- Verifies Dask environment inside container
- Checks Docker Compose syntax for both development and production configs

#### 1.5 Test Summary (`summary`)
- **Runs:** Always (even if previous jobs fail)
- **Purpose:** Aggregates results from all test jobs
- **Failure Condition:** Fails if main test suite fails

---

### 2. Documentation (`.github/workflows/ci.yml`)

**Triggers:**
- Push to `main` or `master` branches

**Jobs:**

#### 2.1 Deploy Documentation (`deploy-docs`)
- **Python Version:** 3.x (latest)
- **Steps:**
  1. Checkout repository
  2. Set up Python
  3. Cache MkDocs dependencies
  4. Install MkDocs Material theme
  5. Deploy to GitHub Pages with force push

**Output:**
- Documentation site published to GitHub Pages
- Accessible at: `https://<username>.github.io/<repository>/`

---

## Configuration Files

### pytest.ini
- **Test Discovery:** `test_*.py`, `*_test.py`, `Test*.py`
- **Test Path:** `Test/` directory
- **Coverage:** Enabled with HTML and terminal reports
- **Markers:** `unit`, `integration`, `slow`, `spark`, `pandas`, `dask`, etc.
- **Options:** Verbose output, short tracebacks, strict markers

### .coveragerc
- **Source:** Current directory (`.`)
- **Omit:** Test files, virtual environments, `setup.py`, `conftest.py`
- **Threshold:** 70% minimum coverage
- **Report:** Precision 2 decimals, show missing lines

### requirements.txt
- **Core:** pandas, numpy, pyarrow
- **Dask:** dask[complete], dask-ml, distributed, lz4, numba
- **ML:** scikit-learn, tensorflow, easymllib
- **Geospatial:** geographiclib, geopy
- **Visualization:** matplotlib
- **Legacy:** pyspark, py4j
- **Testing:** pytest, pytest-cov, pytest-spark
- **Docs:** mkdocs-material

---

## Local Development

### Running Tests Locally

```bash
# Run all tests with coverage
pytest -v --cov=. --cov-report=html --cov-report=term-missing

# Run specific test markers
pytest -v -m unit           # Unit tests only
pytest -v -m integration    # Integration tests only
pytest -v -m slow           # Slow tests only

# Run tests for specific Python version (with pyenv/conda)
python3.11 -m pytest -v

# Check coverage threshold
coverage report --fail-under=70
```

### Running Linters Locally

```bash
# Install linting tools
pip install flake8 black isort mypy

# Run flake8
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --max-complexity=10 --max-line-length=127 --statistics

# Check formatting
black --check --diff .

# Check import sorting
isort --check-only --diff .

# Auto-fix formatting (use with caution)
black .
isort .
```

### Testing Docker Build Locally

```bash
# Build Docker image
docker build -t connected-driving-pipeline:test .

# Test Docker image
docker run --rm connected-driving-pipeline:test python validate_dask_setup.py

# Validate Docker Compose
docker compose config
docker compose -f docker-compose.prod.yml config
```

---

## Workflow Artifacts

### Coverage Report
- **Job:** Test Suite → Unit Tests (Python 3.11)
- **Artifact Name:** `coverage-report`
- **Contents:** HTML coverage report (`htmlcov/`)
- **Retention:** 30 days
- **Access:** Download from workflow run page

---

## Integration with External Services

### Codecov (Optional)
- **Service:** https://codecov.io
- **Setup:**
  1. Sign up at Codecov with GitHub account
  2. Enable repository in Codecov dashboard
  3. Add `CODECOV_TOKEN` to GitHub repository secrets
  4. Coverage reports automatically uploaded on Python 3.11 runs

**Benefits:**
- Historical coverage trends
- Pull request coverage diffs
- Coverage badges for README
- Detailed coverage analytics

---

## CI/CD Best Practices

### 1. Fast Feedback
- **Matrix Testing:** Parallel execution across Python versions (3.8-3.11)
- **Dependency Caching:** Pip cache reduces dependency install time by ~80%
- **Continue on Error:** Non-critical jobs (lint, slow tests) don't block CI

### 2. Comprehensive Testing
- **Unit Tests:** Fast, isolated component tests
- **Integration Tests:** End-to-end workflow validation
- **Slow Tests:** Large dataset processing benchmarks
- **Docker Tests:** Container environment validation

### 3. Code Quality
- **Linting:** flake8 catches common Python errors
- **Formatting:** black ensures consistent code style
- **Import Sorting:** isort maintains organized imports

### 4. Coverage Monitoring
- **Threshold:** 70% minimum coverage enforced
- **HTML Reports:** Detailed line-by-line coverage visualization
- **Artifacts:** Coverage reports saved for 30 days

### 5. Docker Validation
- **Build Testing:** Ensures Dockerfile always builds successfully
- **Runtime Validation:** Tests Dask setup inside container
- **Compose Validation:** Checks both dev and prod configurations

---

## Troubleshooting

### Test Failures

**Symptom:** Tests fail in CI but pass locally

**Common Causes:**
1. **Python Version Mismatch:** CI tests Python 3.8-3.11, local may differ
2. **Environment Variables:** Missing env vars in CI
3. **System Dependencies:** CI installs BLAS/LAPACK, local may lack them
4. **File Paths:** Hardcoded absolute paths may differ between local and CI

**Solutions:**
- Test locally with same Python version as CI
- Check GitHub Actions logs for detailed error messages
- Ensure system dependencies are installed (see workflow file)
- Use relative paths or `pathlib` for cross-platform compatibility

### Coverage Below Threshold

**Symptom:** Coverage check warns about <70% coverage

**Solutions:**
1. Add tests for untested code paths
2. Check `.coveragerc` to ensure correct source paths
3. Review `htmlcov/index.html` to identify uncovered lines
4. Consider adjusting threshold if coverage target is unrealistic

### Docker Build Failures

**Symptom:** Docker build fails in CI

**Common Causes:**
1. **Dependency Issues:** `requirements.txt` has incompatible versions
2. **System Packages:** Missing system dependencies in Dockerfile
3. **File Permissions:** Dockerfile USER directive may cause permission issues

**Solutions:**
- Test Docker build locally: `docker build -t test .`
- Check Dockerfile for correct base image and dependencies
- Review `.dockerignore` to ensure required files are included

### Linting Failures

**Symptom:** flake8, black, or isort fail

**Solutions:**
- Run linters locally before pushing
- Auto-fix with `black .` and `isort .`
- Review flake8 errors and fix manually (complexity, unused imports)

---

## Performance Benchmarks

### CI Pipeline Timings (Approximate)

| Job               | Duration | Notes                              |
|-------------------|----------|------------------------------------|
| Unit Tests (3.8)  | 3-5 min  | With dependency caching            |
| Unit Tests (3.9)  | 3-5 min  | With dependency caching            |
| Unit Tests (3.10) | 3-5 min  | With dependency caching            |
| Unit Tests (3.11) | 4-6 min  | Includes coverage upload           |
| Code Quality      | 1-2 min  | Linting only, no heavy computation |
| Integration Tests | 5-10 min | Depends on dataset size            |
| Docker Build      | 3-4 min  | With Docker layer caching          |
| Documentation     | 2-3 min  | With MkDocs cache                  |

**Total Pipeline Time:** ~10-15 minutes (parallel execution)

---

## Security Considerations

### Secrets Management
- **CODECOV_TOKEN:** Stored in GitHub repository secrets
- **Never commit:** API keys, passwords, or tokens to repository
- **Environment Variables:** Use GitHub Actions secrets for sensitive data

### Permissions
- **Test Workflow:** `contents: read`, `pull-requests: write`
- **Documentation Workflow:** `contents: write` (for GitHub Pages deployment)
- **Principle of Least Privilege:** Each workflow has minimal required permissions

---

## Maintenance

### Updating Workflows

**When to Update:**
1. New Python version released (add to matrix)
2. New testing tool added (update lint job)
3. New dependency required (update install steps)
4. Coverage threshold changed (update `.coveragerc`)

**How to Update:**
1. Edit `.github/workflows/test.yml` or `.github/workflows/ci.yml`
2. Test changes locally if possible (e.g., linting, Docker build)
3. Commit and push to a feature branch
4. Open pull request to validate changes in CI
5. Merge after successful CI run

### Dependency Updates

**Automated Updates (Recommended):**
- Use **Dependabot** to automatically update GitHub Actions versions
- Create `.github/dependabot.yml` to enable:
  - Weekly updates for GitHub Actions
  - Weekly updates for pip dependencies (if using `requirements.txt`)

**Manual Updates:**
```bash
# Update Python dependencies
pip install --upgrade -r requirements.txt
pip freeze > requirements.txt

# Update GitHub Actions (manually edit workflow files)
# Check for latest versions: https://github.com/actions/checkout/releases
```

---

## Future Enhancements

### Planned Improvements
1. **Performance Benchmarking:** Track execution time trends over commits
2. **Docker Registry:** Push images to Docker Hub or GitHub Container Registry
3. **Staging Deployments:** Auto-deploy to staging environment on merge
4. **Notification Integration:** Slack/Discord notifications for CI failures
5. **Scheduled Tests:** Nightly runs with large datasets
6. **Multi-platform Testing:** Test on Windows and macOS runners

---

## References

- **GitHub Actions Documentation:** https://docs.github.com/en/actions
- **pytest Documentation:** https://docs.pytest.org
- **Coverage.py Documentation:** https://coverage.readthedocs.io
- **Docker Documentation:** https://docs.docker.com
- **Codecov Documentation:** https://docs.codecov.com

---

**Last Updated:** 2026-01-18
**Maintainer:** ConnectedDrivingPipelineV4 Team
