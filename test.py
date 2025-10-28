from pathlib import Path
from typing import Optional, Dict


class PathConfiguration:
    """
    Discovers and configures project paths dynamically.
    Lookup CSVs are expected in the script directory (GitHub repo).
    All other folders are discovered recursively in the user-provided project directory.
    """
    
    # Folder names to search for in project directory
    DEFAULT_FOLDER_NAMES = {
        'csv_data': ['csv_data', 'csv', 'data'],
        'requirements': ['requirements', 'reqs'],
        'tca': ['TCA', 'tca'],
        'test_cases': ['TestCases', 'test_cases', 'testcases'],
        'output': ['bus_monitor_output', 'output', 'results']
    }
    
    # Lookup CSVs in GitHub repo (script directory)
    LOOKUP_CSV_NAMES = ['message_lookup.csv', 'lookup.csv']
    REQUIREMENT_TESTCASE_LOOKUP_NAMES = ['requirement_testcase_lookup.csv', 'req_tc_lookup.csv']
    
    def __init__(self, project_directory: Path):
        """
        Initialize path configuration.
        
        Args:
            project_directory: User's project directory containing data folders
        """
        self.project_directory = Path(project_directory)
        self.script_directory = Path(__file__).parent  # GitHub repo location
        
        if not self.project_directory.exists():
            raise ValueError(f"Project directory does not exist: {self.project_directory}")
        
        self.paths: Dict[str, Path] = {}
        self._discover_paths()
    
    def _discover_paths(self):
        """Discover all required paths."""
        print(f"\n📁 Project Directory: {self.project_directory}")
        print(f"📁 Script Directory: {self.script_directory}\n")
        
        # Find folders in project directory (searches recursively)
        for key, possible_names in self.DEFAULT_FOLDER_NAMES.items():
            found_path = self._find_folder_recursive(possible_names, self.project_directory)
            
            if key == 'output':
                if found_path is None:
                    found_path = self.project_directory / possible_names[0]
                found_path.mkdir(parents=True, exist_ok=True)
                print(f"📤 Output: {found_path}")
            elif found_path:
                # Show relative path from project root
                try:
                    rel_path = found_path.relative_to(self.project_directory)
                    print(f"✓ {key}: {rel_path}")
                except ValueError:
                    print(f"✓ {key}: {found_path}")
            else:
                print(f"✗ {key}: NOT FOUND")
            
            self.paths[key] = found_path
        
        # Find lookup CSVs in script directory (GitHub repo)
        self.paths['lookup_csv'] = self._find_file(self.LOOKUP_CSV_NAMES, self.script_directory)
        if self.paths['lookup_csv']:
            print(f"✓ lookup_csv: {self.paths['lookup_csv'].name} (from repo)")
        else:
            print(f"✗ lookup_csv: NOT FOUND in {self.script_directory}")
        
        self.paths['requirement_testcase_lookup'] = self._find_file(
            self.REQUIREMENT_TESTCASE_LOOKUP_NAMES, self.script_directory
        )
        if self.paths['requirement_testcase_lookup']:
            print(f"✓ req_tc_lookup: {self.paths['requirement_testcase_lookup'].name} (from repo)")
        else:
            print(f"✗ req_tc_lookup: NOT FOUND in {self.script_directory}")
        
        print()
    
    def _find_folder_recursive(self, possible_names: list, search_dir: Path, max_depth: int = 3) -> Optional[Path]:
        """
        Recursively find a folder with any of the possible names.
        
        Args:
            possible_names: List of possible folder names
            search_dir: Directory to search in
            max_depth: Maximum depth to search (default 3 levels deep)
        """
        def search(current_dir: Path, depth: int) -> Optional[Path]:
            if depth > max_depth:
                return None
            
            # Check current directory
            for name in possible_names:
                path = current_dir / name
                if path.exists() and path.is_dir():
                    return path
            
            # Search subdirectories
            try:
                for item in current_dir.iterdir():
                    if item.is_dir() and not item.name.startswith('.'):
                        result = search(item, depth + 1)
                        if result:
                            return result
            except PermissionError:
                pass
            
            return None
        
        return search(search_dir, 0)
    
    def _find_file(self, possible_names: list, search_dir: Path) -> Optional[Path]:
        """Find a file with any of the possible names in specified directory."""
        for name in possible_names:
            path = search_dir / name
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
    
    def __init__(self, path_config: PathConfiguration):
        # Set instance variables from discovered paths
        self.csv_folder = path_config.get_path('csv_data')
        self.lookup_csv_path = path_config.get_path('lookup_csv')
        self.requirements_folder = path_config.get_path('requirements')
        self.output_folder = path_config.get_path('output')
        self.tca_folder = path_config.get_path('tca')
        self.test_cases_folder = path_config.get_path('test_cases')
        self.requirement_testcase_lookup_path = path_config.get_path('requirement_testcase_lookup')


# Example usage
if __name__ == "__main__":
    # User provides their project directory path
    project_path = input("Enter project directory path: ").strip()
    
    config = PathConfiguration(project_path)
    
    # Validate required paths exist
    required = ['csv_data', 'requirements', 'tca', 'test_cases', 'lookup_csv']
    config.validate_required_paths(required)
