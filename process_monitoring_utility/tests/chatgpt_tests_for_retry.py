import pytest
import time
from unittest.mock import MagicMock
from common_utils import CommonUtils  # Assuming the retry decorator is in common_utils.py

# Unit tests for CommonUtils.retry decorator
class TestCommonUtils:

    def test_retry_decorator_success(self, monkeypatch):
        """
        Test if the retry decorator works correctly when the function succeeds.
        """
        # Create a function that will succeed
        def success_func():
            return "Success"

        # Apply the decorator
        decorated_func = CommonUtils.retry(max_retries=3, delay=1)(success_func)

        # Call the decorated function
        result = decorated_func()

        # Assert that the function was called only once, and it returned "Success"
        assert result == "Success"

    def test_retry_decorator_failure(self, monkeypatch):
        """
        Test if the retry decorator retries the function when it fails.
        """
        # Create a function that will always raise an exception
        def failing_func():
            raise Exception("Test Exception")

        # Apply the decorator
        decorated_func = CommonUtils.retry(max_retries=3, delay=1)(failing_func)

        # Mock time.sleep to avoid waiting in the test
        monkeypatch.setattr(time, 'sleep', MagicMock())

        # Call the decorated function and check that it raises the exception after 3 retries
        with pytest.raises(Exception):
            decorated_func()

        # Assert that time.sleep was called 2 times (retry attempts)
        assert time.sleep.call_count == 2

    def test_retry_decorator_retry_count(self, monkeypatch):
        """
        Test if the retry decorator correctly handles retry count and delay.
        """
        # Create a function that raises an exception in the first 2 calls and succeeds in the third
        retry_attempts = 0

        def retrying_func():
            nonlocal retry_attempts
            retry_attempts += 1
            if retry_attempts < 3:
                raise Exception("Test Exception")
            return "Success"

        # Apply the decorator
        decorated_func = CommonUtils.retry(max_retries=3, delay=1)(retrying_func)

        # Mock time.sleep to avoid delays during the test
        monkeypatch.setattr(time, 'sleep', MagicMock())

        # Call the decorated function
        result = decorated_func()

        # Assert that the function eventually returns "Success"
        assert result == "Success"

        # Assert that time.sleep was called 2 times (after the first two failed attempts)
        assert time.sleep.call_count == 2

    def test_retry_decorator_no_retry_on_success(self, monkeypatch):
        """
        Test if the retry decorator doesn't retry if the function succeeds on the first attempt.
        """
        # Create a function that will succeed
        def success_func():
            return "Success"

        # Apply the decorator
        decorated_func = CommonUtils.retry(max_retries=3, delay=1)(success_func)

        # Mock time.sleep to avoid delays
        monkeypatch.setattr(time, 'sleep', MagicMock())

        # Call the decorated function
        result = decorated_func()

        # Assert that the function was called once and returned the correct result
        assert result == "Success"

        # Assert that time.sleep was never called because the function didn't fail
        assert time.sleep.call_count == 0


# Step 2: Explanation of Each Test
# test_retry_decorator_success:

# This test checks if the retry decorator works correctly when the decorated function succeeds on the first try.

# It asserts that the function returns the correct result without retrying.

# test_retry_decorator_failure:

# This test checks if the retry decorator retries the function when it raises an exception.

# We simulate a function that always raises an exception and then ensure it retries the specified number of times.

# We also mock time.sleep using monkeypatch to prevent actual delays in the test.

# test_retry_decorator_retry_count:

# This test ensures that the retry logic works as expected when the function fails multiple times and eventually succeeds.

# It checks that time.sleep is called the correct number of times (i.e., after each failed attempt).

# test_retry_decorator_no_retry_on_success:

# This test checks if the retry decorator does not attempt retries when the function is successful on the first try.

# It also ensures that time.sleep is not called when the function doesn't fail.

# Step 3: Running the Tests
# You can run the tests using pytest by executing the following command in your terminal:

# bash
# Copy
# Edit
# pytest test_common_utils.py
# Make sure that the file name is correct and it contains the CommonUtils class with the retry decorator.

# Final Notes:
# The monkeypatch is used to mock time.sleep so that the tests do not have artificial delays during execution.

# The tests cover different retry scenarios, including success, failure, retry count, and ensuring no retries occur if the function is successful.
