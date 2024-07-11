#!/bin/bash

# Run pytest tests
pytest tests/

# Check the exit status of pytest
if [ $? -ne 0 ]; then
  echo "Unit tests failed, deployment aborted!"
  exit 1
fi

# Proceed with SAM build if tests pass
sam build
