.PHONY: test test-fast test-network test-all install-hooks

# Fast local loop: no network, no AWS credentials required (~5s)
test-fast:
	pytest -m "not network and not aws_creds"

# Fast + network tests, still no AWS credentials required
test-network:
	pytest -m "not aws_creds"

# Full suite, as run in CI (requires AWS credentials for aws_creds tests)
test-all:
	pytest

test: test-fast

# One-time setup: installs the pre-push hook that runs test-fast
install-hooks:
	cp scripts/pre-push .git/hooks/pre-push
	chmod +x .git/hooks/pre-push
	@echo "pre-push hook installed: 'make test-fast' will run before each push"