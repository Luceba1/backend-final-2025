"""
Tests for P11: Sanitized Logging Utilities

Tests verify the implementation of logging sanitization to prevent
exposure of sensitive information in logs.
"""
import pytest
import logging
from utils.logging_utils import (
    sanitize_string,
    get_error_id,
    log_error_sanitized,
    log_repository_error,
    create_user_safe_error,
    SanitizedLogger,
    get_sanitized_logger
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_logger():
    """Create mock logger for testing"""
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []  # Clear any existing handlers
    return logger


# ============================================================================
# P11: SANITIZE_STRING FUNCTION
# ============================================================================

class TestP11SanitizeString:
    """
    Test P11: sanitize_string() function

    Validates that sensitive patterns are properly redacted
    """

    def test_sanitize_password(self):
        """Test password sanitization"""
        # Various password formats
        test_cases = [
            ("password=secret123", "password=[PASSWORD_REDACTED]"),
            ("password: secret123", "password: [PASSWORD_REDACTED]"),
            ('password="secret123"', 'password="[PASSWORD_REDACTED]"'),
            ("password='secret123'", "password='[PASSWORD_REDACTED]'"),
            ("PASSWORD=SECRET", "PASSWORD=[PASSWORD_REDACTED]"),  # Case insensitive
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[PASSWORD_REDACTED]" in result, f"Failed for: {input_str}"


    def test_sanitize_token(self):
        """Test token sanitization"""
        test_cases = [
            ("token=abc123xyz", "token=[TOKEN_REDACTED]"),
            ("token: abc123xyz", "token: [TOKEN_REDACTED]"),
            ('token="abc123xyz"', 'token="[TOKEN_REDACTED]"'),
            ("TOKEN=XYZ", "TOKEN=[TOKEN_REDACTED]"),
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[TOKEN_REDACTED]" in result


    def test_sanitize_api_key(self):
        """Test API key sanitization"""
        test_cases = [
            ("api_key=sk-123456", "api_key=[API_KEY_REDACTED]"),
            ("api-key=sk-123456", "api-key=[API_KEY_REDACTED]"),
            ("apikey=sk-123456", "apikey=[API_KEY_REDACTED]"),
            ("API_KEY=SK-123456", "API_KEY=[API_KEY_REDACTED]"),
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[API_KEY_REDACTED]" in result


    def test_sanitize_secret(self):
        """Test secret sanitization"""
        test_cases = [
            ("secret=mysecret", "secret=[SECRET_REDACTED]"),
            ("secret: mysecret", "secret: [SECRET_REDACTED]"),
            ("SECRET=MYSECRET", "SECRET=[SECRET_REDACTED]"),
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[SECRET_REDACTED]" in result


    def test_sanitize_authorization(self):
        """Test authorization header sanitization"""
        test_cases = [
            ("authorization=Bearer xyz", "authorization=[AUTH_REDACTED]"),
            ("Authorization: Bearer xyz", "Authorization: [AUTH_REDACTED]"),
            ("AUTHORIZATION=BEARER XYZ", "AUTHORIZATION=[AUTH_REDACTED]"),
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[AUTH_REDACTED]" in result


    def test_sanitize_credit_card(self):
        """
        Test P11 FIX: Credit card number sanitization

        Validates various credit card formats are redacted
        """
        test_cases = [
            ("Card: 4532-1234-5678-9010", "Card: [CARD_REDACTED]"),
            ("Card: 4532 1234 5678 9010", "Card: [CARD_REDACTED]"),
            ("Card: 4532123456789010", "Card: [CARD_REDACTED]"),
            ("Cards: 4532-1234-5678-9010 and 5678-9012-3456-7890",
             "Cards: [CARD_REDACTED] and [CARD_REDACTED]"),
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[CARD_REDACTED]" in result
            assert "4532" not in result  # Card digits should be gone


    def test_sanitize_ssn(self):
        """
        Test P11 FIX: SSN sanitization

        Validates Social Security Number format (XXX-XX-XXXX) is redacted
        """
        test_cases = [
            ("SSN: 123-45-6789", "SSN: [SSN_REDACTED]"),
            ("SSN is 123-45-6789 for verification", "SSN is [SSN_REDACTED] for verification"),
        ]

        for input_str, expected in test_cases:
            result = sanitize_string(input_str)
            assert "[SSN_REDACTED]" in result
            assert "123-45-6789" not in result


    def test_sanitize_multiple_patterns(self):
        """
        Test P11 FIX: Multiple sensitive patterns in same string

        Validates that multiple different sensitive patterns are all redacted
        """
        input_str = (
            "User login failed: email=user@example.com, password=secret123, "
            "token=abc-xyz-789, card=4532-1234-5678-9010"
        )

        result = sanitize_string(input_str)

        # All sensitive data should be redacted
        assert "[PASSWORD_REDACTED]" in result
        assert "[TOKEN_REDACTED]" in result
        assert "[CARD_REDACTED]" in result

        # Original values should be gone
        assert "secret123" not in result
        assert "abc-xyz-789" not in result
        assert "4532" not in result


    def test_sanitize_preserves_non_sensitive_data(self):
        """Test that non-sensitive data is preserved"""
        input_str = "User john@example.com logged in at 2025-11-17 10:30:45"
        result = sanitize_string(input_str)

        # Should be unchanged
        assert result == input_str
        assert "john@example.com" in result
        assert "2025-11-17" in result


    def test_sanitize_handles_non_string_input(self):
        """Test sanitize_string converts non-string input to string"""
        assert sanitize_string(123) == "123"
        assert sanitize_string(None) == "None"
        assert sanitize_string(True) == "True"


# ============================================================================
# P11: ERROR ID GENERATION
# ============================================================================

class TestP11ErrorIdGeneration:
    """Test error ID generation for tracking"""

    def test_get_error_id_format(self):
        """Test error ID is 8-character string"""
        error_id = get_error_id()

        assert isinstance(error_id, str)
        assert len(error_id) == 8


    def test_get_error_id_uniqueness(self):
        """Test error IDs are unique"""
        ids = {get_error_id() for _ in range(100)}

        # Should have 100 unique IDs
        assert len(ids) == 100


# ============================================================================
# P11: LOG_ERROR_SANITIZED FUNCTION
# ============================================================================

class TestP11LogErrorSanitized:
    """
    Test P11: log_error_sanitized() function

    Validates sanitized error logging with error ID tracking
    """

    def test_log_error_sanitized_basic(self, mock_logger, caplog):
        """Test basic error logging with sanitization"""
        with caplog.at_level(logging.ERROR):
            error_id = log_error_sanitized(
                mock_logger,
                "Database error occurred",
                exception=Exception("Connection failed")
            )

        # Verify error ID format
        assert len(error_id) == 8

        # Verify log was created with error ID
        assert len(caplog.records) > 0
        log_message = caplog.records[0].message
        assert error_id in log_message
        assert "Database error occurred" in log_message


    def test_log_error_sanitized_redacts_sensitive_data(self, mock_logger, caplog):
        """
        Test P11 FIX: Sensitive data in error message is sanitized

        Verifies that passwords, tokens, etc. are redacted in logs
        """
        with caplog.at_level(logging.ERROR):
            error_id = log_error_sanitized(
                mock_logger,
                "Login failed with password=secret123",
                exception=Exception("Auth error")
            )

        log_message = caplog.records[0].message

        # Password should be redacted
        assert "[PASSWORD_REDACTED]" in log_message
        assert "secret123" not in log_message


    def test_log_error_sanitized_with_context(self, mock_logger, caplog):
        """
        Test P11 FIX: Context dictionary is also sanitized

        Verifies that context values are sanitized
        """
        with caplog.at_level(logging.DEBUG):
            error_id = log_error_sanitized(
                mock_logger,
                "Operation failed",
                exception=Exception("Error"),
                context={
                    "user_id": 123,
                    "api_key": "sk-12345",
                    "operation": "login"
                }
            )

        # Find debug log with context
        debug_logs = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert len(debug_logs) > 0

        context_message = debug_logs[0].message

        # Context should be sanitized
        assert "[API_KEY_REDACTED]" in context_message
        assert "sk-12345" not in context_message

        # Non-sensitive data preserved
        assert "123" in context_message
        assert "login" in context_message


    def test_log_error_sanitized_without_exception(self, mock_logger, caplog):
        """Test logging without exception object"""
        with caplog.at_level(logging.ERROR):
            error_id = log_error_sanitized(
                mock_logger,
                "Warning: High memory usage"
            )

        log_message = caplog.records[0].message
        assert error_id in log_message
        assert "High memory usage" in log_message


# ============================================================================
# P11: LOG_REPOSITORY_ERROR FUNCTION
# ============================================================================

class TestP11LogRepositoryError:
    """
    Test P11: log_repository_error() function

    Validates standardized repository error logging
    """

    def test_log_repository_error_with_record_id(self, mock_logger, caplog):
        """Test repository error logging with record ID"""
        with caplog.at_level(logging.ERROR):
            error_id = log_repository_error(
                mock_logger,
                operation="save",
                model_name="ProductModel",
                record_id=123,
                exception=Exception("Database constraint violation")
            )

        log_message = caplog.records[0].message

        assert "save" in log_message
        assert "ProductModel" in log_message
        assert "123" in log_message


    def test_log_repository_error_without_record_id(self, mock_logger, caplog):
        """Test repository error logging without record ID"""
        with caplog.at_level(logging.ERROR):
            error_id = log_repository_error(
                mock_logger,
                operation="find_all",
                model_name="ClientModel",
                record_id=None,
                exception=Exception("Query timeout")
            )

        log_message = caplog.records[0].message

        assert "find_all" in log_message
        assert "ClientModel" in log_message
        # Should not have specific ID
        assert "with id" not in log_message.lower()


# ============================================================================
# P11: CREATE_USER_SAFE_ERROR FUNCTION
# ============================================================================

class TestP11CreateUserSafeError:
    """
    Test P11: create_user_safe_error() function

    Validates user-safe error response generation
    """

    def test_create_user_safe_error_structure(self):
        """
        Test P11 FIX: User-safe error has correct structure

        Verifies error response doesn't expose internal details
        """
        error_id = "abc123xy"
        result = create_user_safe_error(error_id, "save operation")

        # Verify structure
        assert "error" in result
        assert "error_id" in result
        assert "message" in result

        # Verify error ID is included
        assert result["error_id"] == error_id

        # Verify generic error message (no internal details)
        assert "save operation" in result["error"]
        assert "contact support" in result["message"].lower()


    def test_create_user_safe_error_default_operation(self):
        """Test user-safe error with default operation"""
        error_id = "xyz789ab"
        result = create_user_safe_error(error_id)

        assert result["error_id"] == error_id
        assert "operation" in result["error"]


# ============================================================================
# P11: SANITIZED LOGGER WRAPPER
# ============================================================================

class TestP11SanitizedLogger:
    """
    Test P11: SanitizedLogger wrapper class

    Validates automatic sanitization of all log levels
    """

    def test_sanitized_logger_error(self, mock_logger, caplog):
        """Test SanitizedLogger.error() sanitizes automatically"""
        sanitized_logger = SanitizedLogger(mock_logger)

        with caplog.at_level(logging.ERROR):
            sanitized_logger.error("Error with password=secret123")

        log_message = caplog.records[0].message
        assert "[PASSWORD_REDACTED]" in log_message
        assert "secret123" not in log_message


    def test_sanitized_logger_info(self, mock_logger, caplog):
        """Test SanitizedLogger.info() sanitizes automatically"""
        sanitized_logger = SanitizedLogger(mock_logger)

        with caplog.at_level(logging.INFO):
            sanitized_logger.info("User login with token=abc-xyz-123")

        log_message = caplog.records[0].message
        assert "[TOKEN_REDACTED]" in log_message
        assert "abc-xyz-123" not in log_message


    def test_sanitized_logger_debug(self, mock_logger, caplog):
        """Test SanitizedLogger.debug() sanitizes automatically"""
        sanitized_logger = SanitizedLogger(mock_logger)

        with caplog.at_level(logging.DEBUG):
            sanitized_logger.debug("Debug: api_key=sk-12345")

        log_message = caplog.records[0].message
        assert "[API_KEY_REDACTED]" in log_message
        assert "sk-12345" not in log_message


    def test_sanitized_logger_warning(self, mock_logger, caplog):
        """Test SanitizedLogger.warning() sanitizes automatically"""
        sanitized_logger = SanitizedLogger(mock_logger)

        with caplog.at_level(logging.WARNING):
            sanitized_logger.warning("Warning: card 4532-1234-5678-9010 declined")

        log_message = caplog.records[0].message
        assert "[CARD_REDACTED]" in log_message
        assert "4532" not in log_message


    def test_sanitized_logger_critical(self, mock_logger, caplog):
        """Test SanitizedLogger.critical() sanitizes automatically"""
        sanitized_logger = SanitizedLogger(mock_logger)

        with caplog.at_level(logging.CRITICAL):
            sanitized_logger.critical("CRITICAL: SSN 123-45-6789 compromised")

        log_message = caplog.records[0].message
        assert "[SSN_REDACTED]" in log_message
        assert "123-45-6789" not in log_message


    def test_get_sanitized_logger_factory(self):
        """Test get_sanitized_logger() factory function"""
        logger = get_sanitized_logger("test_module")

        assert isinstance(logger, SanitizedLogger)
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'debug')


# ============================================================================
# P11: INTEGRATION TESTS
# ============================================================================

class TestP11LoggingIntegration:
    """
    Integration tests for P11 logging utilities

    Validates complete workflow from error to user-safe response
    """

    def test_complete_error_logging_workflow(self, mock_logger, caplog):
        """
        Test P11 FIX: Complete error handling workflow

        Simulates:
        1. Error occurs with sensitive data
        2. Error is logged with sanitization
        3. User-safe error is created
        """
        with caplog.at_level(logging.ERROR):
            # Simulate error with sensitive data
            try:
                raise Exception("Auth failed: password=secret, token=xyz")
            except Exception as e:
                # Log error (sanitized)
                error_id = log_error_sanitized(
                    mock_logger,
                    f"Authentication error: {str(e)}",
                    exception=e
                )

                # Create user-safe response
                user_error = create_user_safe_error(error_id, "authentication")

        # Verify log is sanitized
        log_message = caplog.records[0].message
        assert "[PASSWORD_REDACTED]" in log_message
        assert "[TOKEN_REDACTED]" in log_message
        assert "secret" not in log_message
        assert "xyz" not in log_message

        # Verify user response doesn't expose internals
        assert user_error["error_id"] == error_id
        assert "password" not in user_error["error"].lower()
        assert "token" not in user_error["error"].lower()
        assert "contact support" in user_error["message"].lower()


    def test_repository_error_complete_workflow(self, mock_logger, caplog):
        """
        Test P11 FIX: Repository error workflow

        Validates standardized repository error handling
        """
        with caplog.at_level(logging.ERROR):
            # Simulate repository error
            error_id = log_repository_error(
                mock_logger,
                operation="update",
                model_name="UserModel",
                record_id=456,
                exception=Exception("Unique constraint violation: email already exists")
            )

            user_error = create_user_safe_error(error_id, "update")

        # Verify structured logging
        log_message = caplog.records[0].message
        assert "update" in log_message
        assert "UserModel" in log_message
        assert "456" in log_message

        # Verify user-safe response
        assert user_error["error_id"] == error_id
        assert "An error occurred" in user_error["error"]


# ============================================================================
# TEST SUMMARY
# ============================================================================

"""
TEST COVERAGE SUMMARY FOR P11:

sanitize_string(): 10 tests
✅ test_sanitize_password
✅ test_sanitize_token
✅ test_sanitize_api_key
✅ test_sanitize_secret
✅ test_sanitize_authorization
✅ test_sanitize_credit_card
✅ test_sanitize_ssn
✅ test_sanitize_multiple_patterns
✅ test_sanitize_preserves_non_sensitive_data
✅ test_sanitize_handles_non_string_input

Error ID Generation: 2 tests
✅ test_get_error_id_format
✅ test_get_error_id_uniqueness

log_error_sanitized(): 4 tests
✅ test_log_error_sanitized_basic
✅ test_log_error_sanitized_redacts_sensitive_data
✅ test_log_error_sanitized_with_context
✅ test_log_error_sanitized_without_exception

log_repository_error(): 2 tests
✅ test_log_repository_error_with_record_id
✅ test_log_repository_error_without_record_id

create_user_safe_error(): 2 tests
✅ test_create_user_safe_error_structure
✅ test_create_user_safe_error_default_operation

SanitizedLogger: 6 tests
✅ test_sanitized_logger_error
✅ test_sanitized_logger_info
✅ test_sanitized_logger_debug
✅ test_sanitized_logger_warning
✅ test_sanitized_logger_critical
✅ test_get_sanitized_logger_factory

Integration Tests: 2 tests
✅ test_complete_error_logging_workflow
✅ test_repository_error_complete_workflow

TOTAL: 28 tests for P11 (Sanitized Logging)
"""