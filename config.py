#!/usr/bin/env python3

import os
import yaml
from typing import Dict, Any

class Config:
    """Configuration handler for runcmd_generator"""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from config.yaml"""
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config from {config_path}: {e}")
            self._config = {}
    
    @property
    def all(self) -> Dict[str, Any]:
        """Get all configuration values"""
        return self._config.copy()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key"""
        return self._config.get(key, default)