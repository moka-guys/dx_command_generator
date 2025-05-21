#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Any, Dict
from config import Config

class CommandGenerator(ABC):
    """Abstract base class for command generators"""

    def __init__(self):
        config = Config()
        self.config_values = config.all
        self.auth_token_path = self.config_values.get('dnanexus_auth_token_path')

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the command generator"""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a description of what this command generator does"""
        pass

    @abstractmethod
    def generate(self) -> None:
        """Generate the commands"""
        pass 