#!/usr/bin/env python3

import sys
import yaml
from typing import List
from base import CommandGenerator
from workflow import CP2WorkflowGenerator
from cov import CoverageCommandGenerator
from picard import PicardCommandGenerator
from fqc import FastQCCommandGenerator
from readcount import ReadcountCommandGenerator
from cnv import CNVCommandGenerator
from config import Config # Import Config to get version

def main():
    """Main function to select and run a command generator"""

    # Load config and version
    config_instance = Config()
    version = config_instance.get('version', 'unknown')

    # List of available command generators
    generators: List[CommandGenerator] = [
        CP2WorkflowGenerator(),
        CoverageCommandGenerator(),
        PicardCommandGenerator(),
        FastQCCommandGenerator(),
        ReadcountCommandGenerator(),
        CNVCommandGenerator(),
    ]

    print("==============================================")
    print(f"  DNAnexus Run Command Generator v{version}")
    print("==============================================")

    if not generators:
        print("No command generators are currently available. Exiting.")
        sys.exit(1)

    print("\nAvailable command generation workflows:")
    for i, generator in enumerate(generators, 1):
        print(f"\n  {i}. {generator.name}")
        print(f"      Description: {generator.description}")

    while True:
        try:
            choice_str = input(f"\nSelect command type (number 1-{len(generators)}) or 0 to exit: ").strip()
            if not choice_str:
                print("No choice entered. Please try again.")
                continue
            
            choice = int(choice_str)

            if choice == 0:
                print("Exiting program.")
                sys.exit(0)
            if 1 <= choice <= len(generators):
                selected_generator = generators[choice - 1]
                print(f"\n--- Starting: {selected_generator.name} ---")
                selected_generator.generate()
                print(f"--- Finished: {selected_generator.name} ---")
                break
            else:
                print(f"Invalid choice. Please enter a number between 0 and {len(generators)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except EOFError:
            print("\nInput cancelled. Exiting program.")
            sys.exit(0)
        except KeyboardInterrupt:
            print("\nOperation interrupted by user. Exiting program.")
            sys.exit(0)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print("Exiting program.")
            sys.exit(1)

if __name__ == "__main__":
    main()