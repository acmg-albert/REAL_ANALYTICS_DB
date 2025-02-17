"""Custom exceptions for the application."""

class ScrapingError(Exception):
    """Raised when scraping operations fail."""
    pass

class DataValidationError(Exception):
    """Raised when data validation fails."""
    pass

class DatabaseError(Exception):
    """Raised when database operations fail."""
    pass

class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass 