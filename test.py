from pathlib import Path
from typing import Optional, Dict
import sys


class PathConfiguration:
    """
    Discovers and configures project paths dynamically.
    Can be used standalone or integrated into existing classes.
    """
    
    # Default folder names to search for
    DEFAULT_FOLDER_NAMES = {
        'csv_data': ['csv_data', 'csv', 'data'],
        'requirements': ['requirements', 'reqs'],
        'tca': ['TCA', 'tca'],
        'test_cases': ['TestCases', 'test_cases', 'testcases'],
        'output': ['bus_monitor_output', 'output', 'results']
    }
    
    # Lookup CSV can be in root or a specific location
    LOOKUP_CSV_NAMES = ['message_lookup.csv', 'lookup.csv']
    REQUIREMENT_TESTCASE_LOOKUP_NAMES = ['requirement_testcase_lookup.csv', 'req_tc_lookup.csv']
    
    def __init__(self, root_directory: Optional[Path] = None):
        """
        Initialize path configuration.
        
        Args:
            root_directory: Root directory to search in. If None, uses current directory.
        """
        self.root_directory = Path(root_directory) if root_directory else Path.cwd()
        
        if not self.root_directory.exists():
            raise ValueError(f"Root directory does not exist: {self.root_directory}")
        
        self.paths: Dict[str, Path] = {}
        self._discover_paths()
    
    def _discover_paths(self):
        """Discover all required paths in the root directory."""
        print(f"\n📁 Root: {self.root_directory}\n")
        
        # Find regular folders
        for key, possible_names in self.DEFAULT_FOLDER_NAMES.items():
            found_path = self._find_folder(possible_names)
            
            if key == 'output':
                if found_path is None:
                    found_path = self.root_directory / possible_names[0]
                found_path.mkdir(parents=True, exist_ok=True)
                print(f"📤 Output: {found_path}")
            elif found_path:
                print(f"✓ {key}: {found_path.name}")
            else:
                print(f"✗ {key}: NOT FOUND")
            
            self.paths[key] = found_path
        
        # Find lookup CSV files
        self.paths['lookup_csv'] = self._find_file(self.LOOKUP_CSV_NAMES)
        if self.paths['lookup_csv']:
            print(f"✓ lookup_csv: {self.paths['lookup_csv'].name}")
        else:
            print(f"✗ lookup_csv: NOT FOUND")
        
        self.paths['requirement_testcase_lookup'] = self._find_file(self.REQUIREMENT_TESTCASE_LOOKUP_NAMES)
        if self.paths['requirement_testcase_lookup']:
            print(f"✓ req_tc_lookup: {self.paths['requirement_testcase_lookup'].name}")
        else:
            print(f"✗ req_tc_lookup: NOT FOUND")
        
        print()
    
    def _find_folder(self, possible_names: list) -> Optional[Path]:
        """Find a folder with any of the possible names."""
        for name in possible_names:
            path = self.root_directory / name
            if path.exists() and path.is_dir():
                return path
        return None
    
    def _find_file(self, possible_names: list) -> Optional[Path]:
        """Find a file with any of the possible names in root directory."""
        for name in possible_names:
            path = self.root_directory / name
            if path.exists() and path.is_file():
                return path
        return None
    
    def get_path(self, key: str) -> Optional[Path]:
        """Get a discovered path by key."""
        return self.paths.get(key)
    
    def validate_required_paths(self, required_keys: list) -> bool:
        """Validate that all required paths were found."""
        missing = [key for key in required_keys if key not in self.paths or self.paths[key] is None]
        
        if missing:
            print(f"❌ Missing: {', '.join(missing)}\n")
            return False
        
        print("✅ All required paths found!\n")
        return True
    
    def print_summary(self):
        """Print a summary of all discovered paths."""
        print("\nPaths:")
        for key, path in self.paths.items():
            status = "✓" if path else "✗"
            print(f"  {status} {key}: {path}")
        print()


# Example integration with your existing class
class YourExistingClass:
    """Example of how to integrate PathConfiguration into your existing class."""
    
    def __init__(self, root_directory: Optional[Path] = None):
        # Discover paths
        self.path_config = PathConfiguration(root_directory)
        
        # Validate required paths
        required_paths = ['csv_data', 'requirements', 'tca', 'test_cases', 'lookup_csv']
        if not self.path_config.validate_required_paths(required_paths):
            raise ValueError("Missing required project folders")
        
        # Set instance variables
        self.csv_folder = self.path_config.get_path('csv_data')
        self.lookup_csv_path = self.path_config.get_path('lookup_csv')
        self.requirements_folder = self.path_config.get_path('requirements')
        self.output_folder = self.path_config.get_path('output')
        self.tca_folder = self.path_config.get_path('tca')
        self.test_cases_folder = self.path_config.get_path('test_cases')
        self.requirement_testcase_lookup_path = self.path_config.get_path('requirement_testcase_lookup')


# Example usage
if __name__ == "__main__":
    # Simple usage - just run it!
    config = PathConfiguration()  # Uses current directory
    
    # Validate required paths exist
    required = ['csv_data', 'requirements', 'tca', 'test_cases', 'lookup_csv']
    config.validate_required_paths(required)
