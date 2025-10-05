"""
Configuration module for Kachy Redis client.
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class KachyConfig:
    """Configuration for the Kachy Redis client."""
    
    access_key: str
    base_url: str = field(default_factory=lambda: os.environ.get("KACHY_BASE_URL", "https://api.klache.net"))
    timeout: int = field(default_factory=lambda: int(os.environ.get("KACHY_TIMEOUT", "30")))
    max_retries: int = field(default_factory=lambda: int(os.environ.get("KACHY_MAX_RETRIES", "3")))
    retry_delay: float = field(default_factory=lambda: float(os.environ.get("KACHY_RETRY_DELAY", "1.0")))
    pool_size: int = field(default_factory=lambda: int(os.environ.get("KACHY_POOL_SIZE", "10")))
    user_agent: str = field(default="kachy-valkey-python/0.1.0")
    
    # Request headers
    headers: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate and set default values after initialization."""
        if not self.access_key:
            raise ValueError("KACHY_ACCESS_KEY is required")
        
        # Set default headers
        if not self.headers:
            self.headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "access_key": self.access_key,
            "base_url": self.base_url,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "pool_size": self.pool_size,
            "user_agent": self.user_agent,
            "headers": self.headers.copy()
        }
    
    @classmethod
    def from_env(cls) -> "KachyConfig":
        """Create configuration from environment variables."""
        return cls(
            access_key=os.environ["KACHY_ACCESS_KEY"],
            base_url=os.environ.get("KACHY_BASE_URL", "https://api.klache.net"),
            timeout=int(os.environ.get("KACHY_TIMEOUT", "30")),
            max_retries=int(os.environ.get("KACHY_MAX_RETRIES", "3")),
            retry_delay=float(os.environ.get("KACHY_RETRY_DELAY", "1.0")),
            pool_size=int(os.environ.get("KACHY_POOL_SIZE", "10"))
        )
