from setuptools import setup, find_packages

setup(
    name="real_analytics_db",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas>=2.0.0',
        'numpy>=1.24.0',
        'requests>=2.31.0',
        'beautifulsoup4>=4.12.0',
        'sqlalchemy>=2.0.0',
        'python-dotenv>=1.0.0',
        'pytest>=7.4.0',
        'black>=23.0.0',
        'isort>=5.12.0',
        'flake8>=6.1.0',
        'mypy>=1.5.0',
        'tqdm>=4.66.0',
        'loguru>=0.7.0',
        'aiohttp>=3.9.0',
        'tenacity>=8.2.0',
        'pydantic>=2.0.0',
        'apscheduler>=3.10.0',
        'supabase>=2.3.0'
    ]
)