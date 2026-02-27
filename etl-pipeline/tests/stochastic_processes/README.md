# Stochastic Process Tests

## Overview

This directory contains tests for AI agent behavior that involves stochastic processes (LLM-based decision making). Unlike deterministic unit tests, these tests verify that the agent exhibits expected *patterns* of behavior rather than exact outputs.

These tests  do not universally cover all possible inputs of the same type like a unit or integration test but rather test sample inputs that represent a class of inputs we expect the LLM to respond to in a a similar way. However the definition of that class of inputs is fuzzy and the LLMs behavior is non-deterministic, so the test may fail intermittently (although we strive to create tests that don't).

## Future Improvements

1. **Retry Logic**: Add automatic retries for flaky tests
2. **Semantic Equivalence**: Better checks for semantically equivalent filters
3. **LLM Testing Framework**: Consider using specialized frameworks for LLM testing
4. **Baseline Recording**: Record typical agent outputs to detect regression
5. **Metrics**: Track success rates over time to identify degradation
