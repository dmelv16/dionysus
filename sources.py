import os
import pandas as pd
from pathlib import Path

def process_sources_files(folder_path):
    """
    Process all *_Sources.csv files in the specified folder.
    Track station and save columns, flag files containing save values 3, 12, or 17.
    """
    folder = Path(folder_path)
    results = []
    
    # Find all files ending with _Sources.csv
    source_files = list(folder.glob('*_Sources.csv'))
    
    if not source_files:
        print("No files ending with '_Sources.csv' found in the folder.")
        return
    
    print(f"Found {len(source_files)} file(s) to process\n")
    
    for file_path in source_files:
        # Extract the name before _Sources
        file_name = file_path.stem  # Gets filename without extension
        name = file_name.replace('_Sources', '')
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Check if required columns exist
            if 'station' not in df.columns or 'save' not in df.columns:
                print(f"⚠️  {name}: Missing 'station' or 'save' column")
                continue
            
            # Get unique values from station and save columns
            stations = df['station'].unique().tolist()
            saves = df['save'].unique().tolist()
            
            # Check if any of the flagged save values are present
            flagged_saves = {3, 12, 17}
            found_flagged = set(saves) & flagged_saves
            is_flagged = len(found_flagged) > 0
            
            # Store results
            result = {
                'name': name,
                'file': file_path.name,
                'stations': stations,
                'saves': saves,
                'flagged': is_flagged,
                'flagged_saves': sorted(list(found_flagged))
            }
            results.append(result)
            
            # Print results for this file
            flag_indicator = "🚩 FLAGGED" if is_flagged else "✓"
            print(f"{flag_indicator} {name}")
            print(f"   Stations: {stations}")
            print(f"   Saves: {saves}")
            if is_flagged:
                print(f"   ⚠️  Contains flagged save values: {sorted(list(found_flagged))}")
            print()
            
        except Exception as e:
            print(f"❌ Error processing {name}: {str(e)}\n")
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    flagged_files = [r for r in results if r['flagged']]
    print(f"Total files processed: {len(results)}")
    print(f"Flagged files: {len(flagged_files)}")
    
    if flagged_files:
        print("\nFlagged files:")
        for r in flagged_files:
            print(f"  - {r['name']}: contains save values {r['flagged_saves']}")
    
    return results


# Example usage
if __name__ == "__main__":
    # Change this to your folder path
    folder_path = "."  # Current directory, or use "/path/to/your/folder"
    
    results = process_sources_files(folder_path)
