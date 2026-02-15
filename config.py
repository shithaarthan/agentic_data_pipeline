"""Configuration management for the Trading Assistant."""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Angel One SmartAPI
    angel_api_key: str = Field(default="", description="Angel One API Key")
    angel_client_id: str = Field(default="", description="Angel One Client ID")
    angel_password: str = Field(default="", description="Angel One Password")
    angel_totp_secret: str = Field(default="", description="TOTP Secret for 2FA")
    
    # LLM Configuration
    ollama_base_url: str = Field(default="http://localhost:11434", description="Ollama server URL")
    ollama_model: str = Field(default="qwen2.5:7b", description="Ollama model name")
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API Key")
    together_api_key: Optional[str] = Field(default=None, description="Together.ai API Key")
    openrouter_api_key: Optional[str] = Field(default=None, description="OpenRouter API Key")
    openrouter_model: str = Field(default="qwen/qwen-3-next-80b-a3b-instruct:free", description="OpenRouter model")
    
    # Database
    database_url: str = Field(default="sqlite:///./trading.db", description="Database connection URL")
    
    # App Settings
    use_mock_data: bool = Field(default=True, description="Use mock data instead of live API")
    log_level: str = Field(default="INFO", description="Logging level")
    
    # Paths
    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent)
    data_dir: Path = Field(default_factory=lambda: Path(__file__).parent / "data_files")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"
    
    @property
    def has_angel_credentials(self) -> bool:
        """Check if Angel One credentials are configured."""
        return all([
            self.angel_api_key,
            self.angel_client_id,
            self.angel_password,
            self.angel_totp_secret
        ])
    
    @property
    def has_openai_key(self) -> bool:
        """Check if OpenAI API key is configured."""
        return bool(self.openai_api_key)
    
    @property
    def has_together_key(self) -> bool:
        """Check if Together.ai API key is configured."""
        return bool(self.together_api_key)


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the settings instance."""
    return settings
