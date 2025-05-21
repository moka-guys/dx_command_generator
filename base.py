#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Any

class CommandGenerator(ABC):
    """Abstract base class for command generators"""

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