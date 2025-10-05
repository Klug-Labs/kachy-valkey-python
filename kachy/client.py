"""
Main client module for Kachy Redis.
"""

import json
import time
import requests
from typing import Any, Optional, Dict, List, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import KachyConfig
from .pipeline import KachyPipeline


class KachyError(Exception):
    """Base exception for Kachy Redis client."""
    pass


class KachyConnectionError(KachyError):
    """Exception raised for connection errors."""
    pass


class KachyAuthenticationError(KachyError):
    """Exception raised for authentication errors."""
    pass


class KachyResponseError(KachyError):
    """Exception raised for API response errors."""
    pass


class KachyClient:
    """Main client for interacting with Kachy Redis."""
    
    def __init__(self, config: KachyConfig):
        """Initialize the Kachy client.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create and configure the requests session."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
            backoff_factor=self.config.retry_delay
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=self.config.pool_size)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_auth_token(self) -> str:
        """Get the access key for authentication."""
        return self.config.access_key
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Any:
        """Make an HTTP request to the Kachy API.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request data
            
        Returns:
            API response data
            
        Raises:
            KachyConnectionError: For connection issues
            KachyAuthenticationError: For authentication issues
            KachyResponseError: For API errors
        """
        url = f"{self.config.base_url}{endpoint}"
        headers = self.config.headers.copy()
        headers["Authorization"] = f"Bearer {self._get_auth_token()}"
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                timeout=self.config.timeout
            )
            
            if response.status_code == 401:
                raise KachyAuthenticationError("Authentication failed")
            elif response.status_code >= 400:
                raise KachyResponseError(f"API error {response.status_code}: {response.text}")
            
            return response.json() if response.content else None
            
        except requests.exceptions.RequestException as e:
            raise KachyConnectionError(f"Request failed: {e}")
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Set a key-value pair with optional expiration.
        
        Args:
            key: The key to set
            value: The value to store
            ex: Expiration time in seconds
            
        Returns:
            True if successful
        """
        data = {"key": key, "value": value}
        if ex is not None:
            data["ex"] = ex
        
        result = self._make_request("POST", "/valkey/set", data)
        return result.get("success", False)
    
    def get(self, key: str) -> Optional[str]:
        """Get a value by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The stored value, or None if not found
        """
        result = self._make_request("GET", f"/valkey/get/{key}")
        return result.get("value")
    
    def delete(self, key: str) -> bool:
        """Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was deleted, False if it didn't exist
        """
        result = self._make_request("DELETE", f"/valkey/del/{key}")
        return result.get("deleted", False)
    
    def exists(self, key: str) -> bool:
        """Check if a key exists.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists, False otherwise
        """
        result = self._make_request("GET", f"/valkey/exists/{key}")
        return result.get("exists", False)
    
    def expire(self, key: str, seconds: int) -> bool:
        """Set expiration for a key.
        
        Args:
            key: The key to set expiration for
            seconds: Expiration time in seconds
            
        Returns:
            True if expiration was set, False if key doesn't exist
        """
        data = {"key": key, "seconds": seconds}
        result = self._make_request("POST", "/valkey/expire", data)
        return result.get("success", False)
    
    def ttl(self, key: str) -> int:
        """Get time to live for a key.
        
        Args:
            key: The key to check
            
        Returns:
            Time to live in seconds, -1 if no expiration, -2 if key doesn't exist
        """
        result = self._make_request("GET", f"/valkey/ttl/{key}")
        return result.get("ttl", -2)
    
    def redis(self, command: str, *args) -> Any:
        """Execute any Redis command.
        
        Args:
            command: The Redis command to execute
            *args: Arguments for the command
            
        Returns:
            The result of the Redis command
        """
        data = {
            "command": command.upper(),
            "args": list(args)
        }
        
        result = self._make_request("POST", "/valkey/exec", data)
        return result.get("result")
    
    def pipeline(self) -> "KachyPipeline":
        """Create a pipeline for batch operations.
        
        Returns:
            A pipeline object for batch operations
        """
        return KachyPipeline(self)
    
    def close(self):
        """Close the connection and cleanup resources."""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
