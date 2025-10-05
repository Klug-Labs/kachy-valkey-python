"""
Kachy Valkey Client

High-performance Valkey client with automatic authentication and multi-tenancy support.
"""

# Global client instance
_client = None

def init(access_key, **kwargs):
    """Initialize the Kachy client with your access key.
    
    Args:
        access_key (str): Your KACHY_ACCESS_KEY for authentication
        **kwargs: Additional configuration options
    """
    global _client
    from .config import KachyConfig
    from .client import KachyClient
    config = KachyConfig(access_key=access_key, **kwargs)
    _client = KachyClient(config)
    return _client

def get_client():
    """Get the current Kachy client instance.
    
    Returns:
        KachyClient: The initialized client instance
        
    Raises:
        RuntimeError: If client is not initialized
    """
    if _client is None:
        raise RuntimeError("Kachy client not initialized. Call kachy.init() first.")
    return _client

# Convenience functions that delegate to the global client
def set(key, value, ex=None):
    """Set a key-value pair with optional expiration.
    
    Args:
        key (str): The key to set
        value (str): The value to store
        ex (int, optional): Expiration time in seconds
    """
    return get_client().set(key, value, ex)

def get(key):
    """Get a value by key.
    
    Args:
        key (str): The key to retrieve
        
    Returns:
        str: The stored value, or None if not found
    """
    return get_client().get(key)

def delete(key):
    """Delete a key.
    
    Args:
        key (str): The key to delete
        
    Returns:
        bool: True if key was deleted, False if it didn't exist
    """
    return get_client().delete(key)

def exists(key):
    """Check if a key exists.
    
    Args:
        key (str): The key to check
        
    Returns:
        bool: True if key exists, False otherwise
    """
    return get_client().exists(key)

def expire(key, seconds):
    """Set expiration for a key.
    
    Args:
        key (str): The key to set expiration for
        seconds (int): Expiration time in seconds
        
    Returns:
        bool: True if expiration was set, False if key doesn't exist
    """
    return get_client().expire(key, seconds)

def ttl(key):
    """Get time to live for a key.
    
    Args:
        key (str): The key to check
        
    Returns:
        int: Time to live in seconds, -1 if no expiration, -2 if key doesn't exist
    """
    return get_client().ttl(key)

def valkey(command, *args):
    """Execute any Valkey command.
    
    Args:
        command (str): The Valkey command to execute
        *args: Arguments for the command
        
    Returns:
        The result of the Valkey command
    """
    return get_client().valkey(command, *args)

def pipeline():
    """Create a pipeline for batch operations.
    
    Returns:
        KachyPipeline: A pipeline object for batch operations
    """
    return get_client().pipeline()

def close():
    """Close the connection."""
    if _client:
        _client.close()

# Export main classes - these will be available after import
__all__ = [
    'init',
    'get_client',
    'set',
    'get',
    'delete',
    'exists',
    'expire',
    'ttl',
    'valkey',
    'pipeline',
    'close'
]
