# Real Estate Analytics DB

## Overview

A real estate market data analytics project that collects, processes, and analyzes real estate market data from multiple sources.

## Features

- Multi-source real estate market data collection
- Data cleaning and standardization
- Data analysis and visualization
- API access to processed data
- Automated data updates

## Tech Stack

- Python 3.11+
- FastAPI
- Supabase
- Pandas
- Plotly
- React + TypeScript

## Project Structure

```
REAL_ANALYTICS_DB/
├── src/                    # Source code
│   ├── database/          # Database related code
│   │   └── supabase/      # Supabase client and operations
│   ├── scrapers/          # Data scrapers for different sources
│   │   ├── apartment_list/# ApartmentList data scrapers
│   │   └── zillow/        # Zillow data scrapers
│   ├── scripts/           # Data processing scripts
│   └── utils/             # Utility functions
├── tests/                 # Test code
├── data/                  # Data files
├── logs/                  # Log files
└── docs/                  # Documentation
```

## Environment Setup

1. Create Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Fill in required configuration (database URL, API keys, etc.)

## Database Configuration

The project uses Supabase as the database. You need to:

1. Create a Supabase project
2. Configure the following in your `.env` file:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_ANON_KEY`: Public API key
   - `SUPABASE_SERVICE_ROLE_KEY`: Service role key (for data imports)

## Usage

1. Run data imports:

```bash
# ApartmentList data
python -m src.scripts.scrape_apartment_list_rent_estimates
python -m src.scripts.process_apartment_list_rent_estimates
python -m src.scripts.import_apartment_list_rent_estimates

python -m src.scripts.scrape_apartment_list_vacancy_index
python -m src.scripts.process_apartment_list_vacancy_index
python -m src.scripts.import_apartment_list_vacancy_index

python -m src.scripts.scrape_apartment_list_time_on_market
python -m src.scripts.process_apartment_list_time_on_market
python -m src.scripts.import_apartment_list_time_on_market

# Zillow data
python -m src.scripts.scrape_zillow_affordability
python -m src.scripts.process_zillow_affordability
python -m src.scripts.import_zillow_affordability

python -m src.scripts.scrape_zillow_renter_affordability
python -m src.scripts.process_zillow_renter_affordability
python -m src.scripts.import_zillow_renter_affordability
```

2. Start API service:
```bash
uvicorn src.main:app --reload
```

3. Access API documentation:
   - http://localhost:8000/docs

## Data Sources

### ApartmentList
- Rent Estimates: Monthly rent estimates by city
- Vacancy Index: Rental vacancy rates by city
- Time on Market: Average time properties spend on the rental market

### Zillow
- New Homeowner Affordability: Monthly affordability metrics for new homeowners (20% down payment)
- New Renter Affordability: Monthly affordability metrics for renters

## Data Security

- All sensitive information (database URLs, API keys) should be configured through environment variables
- Never hardcode sensitive information in the code
- Ensure `.env` file is added to `.gitignore`
- Use `.env.example` as a configuration template

## Development Guidelines

1. Code Style
   - Use Black for code formatting
   - Use Flake8 for code quality checks
   - Use MyPy for type checking

2. Testing
   - Use Pytest for running tests
   - Maintain test coverage above 80%

3. Documentation
   - All code should have clear docstrings
   - API endpoints should have complete OpenAPI documentation

## Deployment

The project is deployed using Render:

1. Configure Render service
2. Set environment variables
3. Configure deployment hooks
4. Enable automatic deployment

## Contributing

1. Fork the project
2. Create a feature branch
3. Submit changes
4. Create a Pull Request

## License

MIT License 