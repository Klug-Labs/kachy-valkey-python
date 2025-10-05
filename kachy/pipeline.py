"""
Pipeline module for batch Redis operations.
"""

from typing import List, Any, Optional
from .client import KachyClient


class KachyPipeline:
    """Pipeline for batch Redis operations."""
    
    def __init__(self, client: KachyClient):
        """Initialize the pipeline.
        
        Args:
            client: The Kachy client instance
        """
        self.client = client
        self.commands = []
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> "KachyPipeline":
        """Add SET command to pipeline.
        
        Args:
            key: The key to set
            value: The value to store
            ex: Expiration time in seconds
            
        Returns:
            Self for method chaining
        """
        self.commands.append(("SET", key, value, ex))
        return self
    
    def get(self, key: str) -> "KachyPipeline":
        """Add GET command to pipeline.
        
        Args:
            key: The key to retrieve
            
        Returns:
            Self for method chaining
        """
        self.commands.append(("GET", key))
        return self
    
    def delete(self, key: str) -> "KachyPipeline":
        """Add DELETE command to pipeline.
        
        Args:
            key: The key to delete
            
        Returns:
            Self for method chaining
        """
        self.commands.append(("DEL", key))
        return self
    
    def exists(self, key: str) -> "KachyPipeline":
        """Add EXISTS command to pipeline.
        
        Args:
            key: The key to check
            
        Returns:
            Self for method chaining
        """
        self.commands.append(("EXISTS", key))
        return self
    
    def expire(self, key: str, seconds: int) -> "KachyPipeline":
        """Add EXPIRE command to pipeline.
        
        Args:
            key: The key to set expiration for
            seconds: Expiration time in seconds
            
        Returns:
            Self for method chaining
        """
        self.commands.append(("EXPIRE", key, seconds))
        return self
    
    def ttl(self, key: str) -> "KachyPipeline":
        """Add TTL command to pipeline.
        
        Args:
            key: The key to check
            
        Returns:
            Self for method chaining
        """
        self.commands.append(("TTL", key))
        return self
    
    def redis(self, command: str, *args) -> "KachyPipeline":
        """Add custom Redis command to pipeline.
        
        Args:
            command: The Redis command to execute
            *args: Arguments for the command
            
        Returns:
            Self for method chaining
        """
        self.commands.append((command.upper(),) + args)
        return self
    
    def execute(self) -> List[Any]:
        """Execute all commands in the pipeline.
        
        Returns:
            List of results for each command
            
        Raises:
            KachyError: If pipeline execution fails
        """
        if not self.commands:
            return []
        
        try:
            # Execute all commands in a single batch request
            data = {
                "commands": [
                    {
                        "command": cmd[0],
                        "args": list(cmd[1:])
                    }
                    for cmd in self.commands
                ]
            }
            
            result = self.client._make_request("POST", "/valkey/pipeline", data)
            results = result.get("results", [])
            
            # Clear commands after execution
            self.commands.clear()
            
            return results
            
        except Exception as e:
            # Clear commands on error
            self.commands.clear()
            raise e
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - execute pipeline if not empty."""
        if self.commands:
            self.execute()
