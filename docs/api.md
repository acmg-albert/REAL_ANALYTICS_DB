# API Documentation

## Overview

This document describes the available API endpoints for accessing real estate market data. The API provides access to various metrics including rent estimates, vacancy rates, time on market data, and affordability indices.

## Base URL

```
http://localhost:8000/api/v1
```

## Authentication

All API endpoints require authentication using a Supabase API key. Include the key in the request headers:

```
Authorization: Bearer your_supabase_anon_key
```

## Endpoints

### Rent Estimates

#### Get Rent Estimates by City

```
GET /rent-estimates/{city_id}
```

Returns monthly rent estimates for a specific city.

**Parameters:**
- `city_id` (path): The unique identifier for the city
- `start_date` (query, optional): Start date for the data range (YYYY-MM-DD)
- `end_date` (query, optional): End date for the data range (YYYY-MM-DD)

**Response:**
```json
{
  "city_id": "string",
  "city_name": "string",
  "state": "string",
  "data": [
    {
      "date": "2024-03",
      "rent_estimate": 1500.00,
      "rent_estimate_1bd": 1200.00,
      "rent_estimate_2bd": 1800.00
    }
  ]
}
```

### Vacancy Index

#### Get Vacancy Index by City

```
GET /vacancy-index/{city_id}
```

Returns vacancy rate data for a specific city.

**Parameters:**
- `city_id` (path): The unique identifier for the city
- `start_date` (query, optional): Start date for the data range (YYYY-MM-DD)
- `end_date` (query, optional): End date for the data range (YYYY-MM-DD)

**Response:**
```json
{
  "city_id": "string",
  "city_name": "string",
  "state": "string",
  "data": [
    {
      "date": "2024-03",
      "vacancy_rate": 5.2,
      "vacancy_rate_trend": "increasing"
    }
  ]
}
```

### Time on Market

#### Get Time on Market Data by City

```
GET /time-on-market/{city_id}
```

Returns average time on market data for a specific city.

**Parameters:**
- `city_id` (path): The unique identifier for the city
- `start_date` (query, optional): Start date for the data range (YYYY-MM-DD)
- `end_date` (query, optional): End date for the data range (YYYY-MM-DD)

**Response:**
```json
{
  "city_id": "string",
  "city_name": "string",
  "state": "string",
  "data": [
    {
      "date": "2024-03",
      "days_on_market": 25,
      "trend": "decreasing"
    }
  ]
}
```

### Homeowner Affordability

#### Get Homeowner Affordability by Metro

```
GET /homeowner-affordability/{metro_id}
```

Returns homeowner affordability metrics for a specific metropolitan area.

**Parameters:**
- `metro_id` (path): The unique identifier for the metropolitan area
- `start_date` (query, optional): Start date for the data range (YYYY-MM-DD)
- `end_date` (query, optional): End date for the data range (YYYY-MM-DD)

**Response:**
```json
{
  "metro_id": "string",
  "metro_name": "string",
  "state": "string",
  "data": [
    {
      "date": "2024-03",
      "affordability_index": 85.5,
      "median_home_price": 350000,
      "monthly_payment": 2100.00,
      "income_required": 84000
    }
  ]
}
```

### Renter Affordability

#### Get Renter Affordability by Metro

```
GET /renter-affordability/{metro_id}
```

Returns renter affordability metrics for a specific metropolitan area.

**Parameters:**
- `metro_id` (path): The unique identifier for the metropolitan area
- `start_date` (query, optional): Start date for the data range (YYYY-MM-DD)
- `end_date` (query, optional): End date for the data range (YYYY-MM-DD)

**Response:**
```json
{
  "metro_id": "string",
  "metro_name": "string",
  "state": "string",
  "data": [
    {
      "date": "2024-03",
      "affordability_index": 92.3,
      "median_rent": 1800.00,
      "income_required": 72000
    }
  ]
}
```

## Error Responses

The API uses standard HTTP status codes to indicate the success or failure of requests.

### Common Error Codes

- `400 Bad Request`: Invalid parameters or request format
- `401 Unauthorized`: Missing or invalid API key
- `404 Not Found`: Requested resource not found
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server-side error

### Error Response Format

```json
{
  "error": {
    "code": "string",
    "message": "string",
    "details": {}
  }
}
```

## Rate Limiting

- Rate limit: 100 requests per minute per API key
- Rate limit headers included in responses:
  - `X-RateLimit-Limit`: Total requests allowed per minute
  - `X-RateLimit-Remaining`: Remaining requests in the current window
  - `X-RateLimit-Reset`: Time until the rate limit resets (in seconds)

## Data Freshness

- Rent estimates: Updated monthly
- Vacancy index: Updated monthly
- Time on market data: Updated monthly
- Affordability indices: Updated monthly

## Support

For API support or to report issues, please:
1. Check the existing documentation
2. Create an issue in the GitHub repository
3. Contact the development team 