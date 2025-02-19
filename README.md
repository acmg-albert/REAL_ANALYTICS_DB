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

## Security Configuration

### Local Development

1. Create a `.env` file based on `.env.example`
2. Add your Supabase configuration:
   ```
   SUPABASE_URL=your_project_url
   SUPABASE_ANON_KEY=your_anon_key
   SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
   ```
3. Never commit the `.env` file to version control

### GitHub Configuration

1. Go to your GitHub repository settings
2. Navigate to "Settings" -> "Secrets and variables" -> "Actions"
3. Add the following secrets:
   - `SUPABASE_URL`
   - `SUPABASE_ANON_KEY`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `RENDER_API_KEY`
   - `RENDER_SERVICE_ID`

### Render Deployment

1. Log in to your Render dashboard
2. Create a new Web Service
3. Connect to your GitHub repository
4. Add the following environment variables:
   - `SUPABASE_URL`
   - `SUPABASE_ANON_KEY`
   - `SUPABASE_SERVICE_ROLE_KEY`
5. Mark all environment variables as "Secret"

## Database Security

The database uses Row Level Security (RLS) with the following policies:
- Read access: Available to authenticated and anonymous users
- Write access: Restricted to service role only
- All operations are logged with timestamps

## Development

- Follow PEP 8 style guide
- Write tests for new features
- Update documentation as needed
- Use meaningful commit messages

## License

MIT License - see LICENSE file for details 