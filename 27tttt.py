import os
import csv
from pathlib import Path

def count_27t_in_csv_files(directory_path):
    """
    Loop through all CSV files in a directory and count occurrences of '27T' 
    in the decoded_description column.
    
    Args:
        directory_path (str): Path to the directory containing CSV files
    
    Returns:
        dict: Dictionary with file names as keys and counts as values
    """
    # Convert to Path object for easier handling
    dir_path = Path(directory_path)
    
    # Check if directory exists
    if not dir_path.exists():
        print(f"Error: Directory '{directory_path}' does not exist.")
        return {}
    
    # Dictionary to store results
    results = {}
    total_count = 0
    
    # Get all CSV files in the directory
    csv_files = list(dir_path.glob('*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in '{directory_path}'")
        return {}
    
    print(f"Found {len(csv_files)} CSV file(s) to process...\n")
    
    # Process each CSV file
    for csv_file in csv_files:
        file_count = 0
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Check if decoded_description column exists
                if 'decoded_description' not in reader.fieldnames:
                    print(f"Warning: '{csv_file.name}' does not have a 'decoded_description' column. Skipping...")
                    continue
                
                # Count rows containing '27T' in decoded_description
                for row in reader:
                    decoded_desc = row.get('decoded_description', '')
                    if decoded_desc and '27T' in decoded_desc:
                        file_count += 1
            
            results[csv_file.name] = file_count
            total_count += file_count
            print(f"  {csv_file.name}: {file_count} occurrences of '27T'")
            
        except Exception as e:
            print(f"Error processing '{csv_file.name}': {e}")
            results[csv_file.name] = 0
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"SUMMARY:")
    print(f"Total files processed: {len(results)}")
    print(f"Total '27T' occurrences: {total_count}")
    print(f"{'='*50}")
    
    return results

def main():
    # Specify the directory containing CSV files
    # You can modify this path or pass it as a command-line argument
    directory = input("Enter the directory path containing CSV files (or press Enter for current directory): ").strip()
    
    if not directory:
        directory = "."  # Use current directory if no input
    
    # Run the counter
    results = count_27t_in_csv_files(directory)
    
    # Optionally save results to a file
    save_results = input("\nSave results to a file? (y/n): ").strip().lower()
    if save_results == 'y':
        output_file = 'count_27t_results.txt'
        with open(output_file, 'w') as f:
            f.write("27T Count Results\n")
            f.write("="*50 + "\n")
            for filename, count in results.items():
                f.write(f"{filename}: {count}\n")
            f.write("="*50 + "\n")
            f.write(f"Total: {sum(results.values())}\n")
        print(f"\nResults saved to '{output_file}'")

if __name__ == "__main__":
    main()
