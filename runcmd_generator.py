#!/usr/bin/env python3

import sys
from typing import List
from workflow import CommandGenerator, CP2WorkflowGenerator
from cov import CoverageCommandGenerator  # Import the new generator
from picard import PicardCommandGenerator  # Import the Picard generator
from fqc import FastQCCommandGenerator  # Import the FastQC generator

def main():
    """Main function to select and run a command generator"""

    # List of available command generators
    # Add new generator classes here as they are implemented
    generators: List[CommandGenerator] = [
        CP2WorkflowGenerator(),
        CoverageCommandGenerator(),  # Add the new generator
        PicardCommandGenerator(),  # Add the Picard generator
        FastQCCommandGenerator(),  # Add the FastQC generator
        # Example: AnotherWorkflowGenerator(),
    ]

    print("==============================================")
    print("  DNAnexus Genomics Run Command Generator")
    print("==============================================")

    if not generators:
        print("No command generators are currently available. Exiting.")
        sys.exit(1)

    print("\nAvailable command generation workflows:")
    for i, generator in enumerate(generators, 1):
        print(f"\n  {i}. {generator.name}")
        print(f"     Description: {generator.description}")

    while True:
        try:
            choice_str = input(f"\nSelect command type (number 1-{len(generators)}) or 0 to exit: ").strip()
            if not choice_str: # Handle empty input
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
                break # Exit after successful generation or if generator handles its own loop
            else:
                print(f"Invalid choice. Please enter a number between 0 and {len(generators)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except EOFError: # Handle Ctrl+D
            print("\nInput cancelled. Exiting program.")
            sys.exit(0)
        except KeyboardInterrupt: # Handle Ctrl+C
            print("\nOperation interrupted by user. Exiting program.")
            sys.exit(0)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print("Exiting program.")
            sys.exit(1)

if __name__ == "__main__":
    main()