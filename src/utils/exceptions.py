"""Custom exceptions for the application."""

class ScrapingError(Exception):
    """Raised when scraping operations fail."""
    pass

class DataValidationError(Exception):
    """数据验证错误异常"""
    pass

class DatabaseError(Exception):
    """Raised when database operations fail."""
    pass

class ConfigurationError(Exception):
    """配置错误异常"""
    pass

class ProcessingError(Exception):
    """数据处理错误异常"""
    pass

class DataImportError(Exception):
    """数据导入错误异常"""
    pass

class DownloadError(Exception):
    """文件下载错误异常"""
    pass 