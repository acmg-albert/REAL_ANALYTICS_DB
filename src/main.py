"""Main application entry point."""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .database.supabase_client import SupabaseClient
from .utils.config import Config
from .utils.exceptions import DatabaseError

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(
    title="Real Estate Analytics API",
    description="API for accessing real estate market analytics data",
    version="1.0.0"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 加载配置
config = Config.from_env()

# 创建数据库客户端
db = SupabaseClient(
    url=config.supabase_url,
    key=config.supabase_anon_key
)

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Welcome to Real Estate Analytics API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # 检查数据库连接
        db.execute_sql("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail="Service unavailable"
        )

@app.get("/api/v1/latest-dates")
async def get_latest_dates():
    """Get latest dates for all data types."""
    try:
        dates = db.get_latest_dates()
        return {
            "status": "success",
            "data": dates,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get latest dates: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/api/v1/rent-estimates/{location_fips}")
async def get_rent_estimates(location_fips: str):
    """
    Get rent estimates for a specific location.
    
    Args:
        location_fips: Location FIPS code
    """
    try:
        result = db.execute_sql(
            """
            SELECT 
                location_name,
                location_type,
                state,
                county,
                metro,
                year,
                month,
                rent_estimate
            FROM apartment_list_rent_estimates
            WHERE location_fips_code = :fips
            ORDER BY year DESC, month DESC;
            """,
            {"fips": location_fips}
        )
        
        return {
            "status": "success",
            "data": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get rent estimates: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/api/v1/locations")
async def get_locations():
    """Get list of available locations."""
    try:
        result = db.execute_sql(
            """
            SELECT DISTINCT
                location_name,
                location_type,
                location_fips_code,
                state,
                county,
                metro
            FROM apartment_list_rent_estimates
            ORDER BY state, county, location_name;
            """
        )
        
        return {
            "status": "success",
            "data": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get locations: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 