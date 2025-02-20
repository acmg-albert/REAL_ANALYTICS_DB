# Real Estate Analytics Database

A comprehensive real estate data analytics platform that collects, processes, and analyzes data from various sources including Apartment List.

## Project Structure

```
REAL_ANALYTICS_DB/
├── src/
│   ├── scrapers/          # Data scraping modules
│   ├── database/          # Database models and operations
│   ├── utils/             # Utility functions and helpers
│   └── scripts/           # Automation scripts
├── tests/                 # Test files
└── docs/                  # Documentation
```

## Features

- Data collection from Apartment List
  - Rent estimates
  - Vacancy rates
  - Time on market data
- Automated data updates
- Data validation and cleaning
- Database storage and management

## Setup

1. Clone the repository
2. Create and activate virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\activate   # Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and configure environment variables
5. Run setup script:
   ```bash
   python src/scripts/setup.py
   ```

## Development

- Follow PEP 8 style guide
- Write tests for new features
- Update documentation as needed
- Use meaningful commit messages

## License

MIT License - see LICENSE file for details 