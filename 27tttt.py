import pandas as pd
import re
from typing import Dict, List, Tuple
import json

def parse_27t_message(message: str) -> Dict:
    """
    Parse the [27t] column message to extract fault information and gaps.
    """
    result = {
        'faulty_count': 0,
        'total_count': 0,
        'fault_rate': 0,
        'gaps': [],
        'timestamps': [],
        'has_data': False
    }
    
    if pd.isna(message) or not message:
        return result
    
    # Extract faulty/total counts from the fraction (e.g., "115/375 INSTANCES")
    # This captures the 115/375 part, where 375 is the total, not the later "432 TOTAL 27T MESSAGES"
    fault_pattern = r'(\d+)/(\d+)\s+INSTANCES'
    fault_match = re.search(fault_pattern, message, re.IGNORECASE)
    
    if fault_match:
        result['faulty_count'] = int(fault_match.group(1))
        result['total_count'] = int(fault_match.group(2))  # This is now 375, not 432
        if result['total_count'] > 0:
            result['fault_rate'] = result['faulty_count'] / result['total_count']
        result['has_data'] = True
    
    # Extract gaps array (e.g., "[1.0008345678, 1.0001234556]")
    gaps_pattern = r'GAPS.*?WERE\s*\[([\d.,\s]+)\]'
    gaps_match = re.search(gaps_pattern, message, re.IGNORECASE)
    
    if gaps_match:
        gaps_str = gaps_match.group(1)
        # Parse the gaps as floats
        gaps = [float(g.strip()) for g in gaps_str.split(',') if g.strip()]
        result['gaps'] = gaps
    
    # Extract timestamps array (e.g., "[1761076415.988962, 1761076517.988962]")
    timestamp_pattern = r'TIMESTAMPS\s*\[([\d.,\s]+)\]'
    timestamp_match = re.search(timestamp_pattern, message, re.IGNORECASE)
    
    if timestamp_match:
        timestamps_str = timestamp_match.group(1)
        timestamps = [float(t.strip()) for t in timestamps_str.split(',') if t.strip()]
        result['timestamps'] = timestamps
    
    return result

def parse_segment(segment_str: str) -> Tuple[str, float, float]:
    """
    Parse segment string to extract segment name and start/end timestamps.
    Format: "1: 1761071414.441524-1761072837.3551452"
    """
    if pd.isna(segment_str) or not segment_str:
        return None, None, None
    
    # Pattern to match "segment_name: start_timestamp-end_timestamp"
    pattern = r'^([^:]+):\s*([\d.]+)-([\d.]+)'
    match = re.match(pattern, str(segment_str).strip())
    
    if match:
        segment_name = match.group(1).strip()
        start_time = float(match.group(2))
        end_time = float(match.group(3))
        return segment_name, start_time, end_time
    
    return None, None, None

def analyze_excel_27t(file_path: str) -> Dict:
    """
    Main function to analyze the Excel file and extract 27T column information.
    """
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Check if required columns exist
    required_columns = ['save', 'station', 'segment', '[27t]']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Warning: Missing columns: {missing_columns}")
        # Try case-insensitive match
        df.columns = df.columns.str.lower()
        required_columns = [col.lower() for col in required_columns]
    
    # Initialize results dictionary
    results = {
        'summary': {
            'total_stations': 0,
            'total_saves': 0,
            'total_faulty_messages': 0,
            'total_messages': 0,
            'stations_with_gaps_over_2s': [],
            'stations_with_gaps_over_1ms': [],
            'segment_boundary_violations': []
        },
        'by_station_save': {},
        'gap_analysis': {
            'gaps_over_1ms': [],
            'gaps_over_2s': [],
            'min_gap_over_1ms': None,
            'max_gap_over_1ms': None,
            'min_gap_over_2s': None,
            'max_gap_over_2s': None
        },
        'segment_analysis': {
            'within_30s_of_start': [],
            'outside_30s_of_start': [],
            'total_violations': 0
        }
    }
    
    # Process each row
    for idx, row in df.iterrows():
        try:
            station = row.get('station', 'Unknown')
            save = row.get('save', 'Unknown')
            segment = row.get('segment', '')
            message_27t = row.get('[27t]', '')
            
            # Create unique key for station-save combination
            station_save_key = f"{station}_{save}"
            
            # Parse the 27t message
            parsed_data = parse_27t_message(message_27t)
            
            if not parsed_data['has_data']:
                continue
            
            # Parse segment information
            segment_name, segment_start, segment_end = parse_segment(segment)
            
            # Initialize station-save entry if not exists
            if station_save_key not in results['by_station_save']:
                results['by_station_save'][station_save_key] = {
                    'station': station,
                    'save': save,
                    'faulty_count': 0,
                    'total_count': 0,
                    'fault_rate': 0,
                    'gaps': [],
                    'gaps_over_1ms': [],
                    'gaps_over_2s': [],
                    'segment_violations': []
                }
            
            # Update station-save statistics
            station_data = results['by_station_save'][station_save_key]
            station_data['faulty_count'] += parsed_data['faulty_count']
            station_data['total_count'] += parsed_data['total_count']
            station_data['gaps'].extend(parsed_data['gaps'])
            
            # Update summary
            results['summary']['total_faulty_messages'] += parsed_data['faulty_count']
            results['summary']['total_messages'] += parsed_data['total_count']
            
            # Analyze gaps
            for gap in parsed_data['gaps']:
                # Check for gaps > 1.0001 seconds (1ms over 1 second)
                if gap > 1.0001:
                    gap_entry = {
                        'station': station,
                        'save': save,
                        'gap': gap,
                        'station_save': station_save_key
                    }
                    results['gap_analysis']['gaps_over_1ms'].append(gap_entry)
                    station_data['gaps_over_1ms'].append(gap)
                    
                    # Update min/max for gaps > 1.0001
                    if results['gap_analysis']['min_gap_over_1ms'] is None:
                        results['gap_analysis']['min_gap_over_1ms'] = gap
                        results['gap_analysis']['max_gap_over_1ms'] = gap
                    else:
                        results['gap_analysis']['min_gap_over_1ms'] = min(results['gap_analysis']['min_gap_over_1ms'], gap)
                        results['gap_analysis']['max_gap_over_1ms'] = max(results['gap_analysis']['max_gap_over_1ms'], gap)
                
                # Check for gaps > 2.0 seconds
                if gap > 2.0:
                    gap_entry = {
                        'station': station,
                        'save': save,
                        'gap': gap,
                        'station_save': station_save_key
                    }
                    results['gap_analysis']['gaps_over_2s'].append(gap_entry)
                    station_data['gaps_over_2s'].append(gap)
                    
                    # Check if within 30 seconds of segment start
                    if segment_start is not None and len(parsed_data['timestamps']) > 0:
                        # Find the timestamp corresponding to this gap
                        gap_idx = parsed_data['gaps'].index(gap)
                        if gap_idx < len(parsed_data['timestamps']):
                            gap_timestamp = parsed_data['timestamps'][gap_idx]
                            time_from_segment_start = gap_timestamp - segment_start
                            
                            violation_entry = {
                                'station': station,
                                'save': save,
                                'gap': gap,
                                'timestamp': gap_timestamp,
                                'segment_start': segment_start,
                                'time_from_start': time_from_segment_start,
                                'within_30s': time_from_segment_start <= 30
                            }
                            
                            if time_from_segment_start <= 30:
                                results['segment_analysis']['within_30s_of_start'].append(violation_entry)
                            else:
                                results['segment_analysis']['outside_30s_of_start'].append(violation_entry)
                            
                            station_data['segment_violations'].append(violation_entry)
                            results['segment_analysis']['total_violations'] += 1
                    
                    # Update min/max for gaps > 2.0
                    if results['gap_analysis']['min_gap_over_2s'] is None:
                        results['gap_analysis']['min_gap_over_2s'] = gap
                        results['gap_analysis']['max_gap_over_2s'] = gap
                    else:
                        results['gap_analysis']['min_gap_over_2s'] = min(results['gap_analysis']['min_gap_over_2s'], gap)
                        results['gap_analysis']['max_gap_over_2s'] = max(results['gap_analysis']['max_gap_over_2s'], gap)
            
            # Calculate fault rate for station
            if station_data['total_count'] > 0:
                station_data['fault_rate'] = station_data['faulty_count'] / station_data['total_count']
                
        except Exception as e:
            print(f"Error processing row {idx}: {e}")
            continue
    
    # Update summary with unique stations
    unique_stations_with_gaps_1ms = list(set([g['station_save'] for g in results['gap_analysis']['gaps_over_1ms']]))
    unique_stations_with_gaps_2s = list(set([g['station_save'] for g in results['gap_analysis']['gaps_over_2s']]))
    
    results['summary']['stations_with_gaps_over_1ms'] = unique_stations_with_gaps_1ms
    results['summary']['stations_with_gaps_over_2s'] = unique_stations_with_gaps_2s
    results['summary']['total_stations'] = len(results['by_station_save'])
    results['summary']['total_saves'] = len(set([data['save'] for data in results['by_station_save'].values()]))
    
    return results

def print_analysis_report(results: Dict):
    """
    Print a formatted analysis report.
    """
    print("\n" + "="*80)
    print("27T COLUMN ANALYSIS REPORT")
    print("="*80)
    
    # Summary
    print("\n--- SUMMARY ---")
    print(f"Total Stations Analyzed: {results['summary']['total_stations']}")
    print(f"Total Saves: {results['summary']['total_saves']}")
    print(f"Total Faulty Messages: {results['summary']['total_faulty_messages']}")
    print(f"Total Messages: {results['summary']['total_messages']}")
    if results['summary']['total_messages'] > 0:
        overall_fault_rate = results['summary']['total_faulty_messages'] / results['summary']['total_messages']
        print(f"Overall Fault Rate: {overall_fault_rate:.2%}")
    
    # Gap Analysis
    print("\n--- GAP ANALYSIS ---")
    print(f"Gaps > 1.0001s: {len(results['gap_analysis']['gaps_over_1ms'])} occurrences")
    if results['gap_analysis']['min_gap_over_1ms']:
        print(f"  Min Gap (>1.0001s): {results['gap_analysis']['min_gap_over_1ms']:.6f}s")
        print(f"  Max Gap (>1.0001s): {results['gap_analysis']['max_gap_over_1ms']:.6f}s")
    
    print(f"\nGaps > 2.0s: {len(results['gap_analysis']['gaps_over_2s'])} occurrences")
    if results['gap_analysis']['min_gap_over_2s']:
        print(f"  Min Gap (>2.0s): {results['gap_analysis']['min_gap_over_2s']:.6f}s")
        print(f"  Max Gap (>2.0s): {results['gap_analysis']['max_gap_over_2s']:.6f}s")
    
    # Segment Boundary Analysis
    print("\n--- SEGMENT BOUNDARY ANALYSIS (Gaps > 2.0s) ---")
    print(f"Total violations: {results['segment_analysis']['total_violations']}")
    print(f"Within 30s of segment start: {len(results['segment_analysis']['within_30s_of_start'])}")
    print(f"Outside 30s of segment start: {len(results['segment_analysis']['outside_30s_of_start'])}")
    
    # Station-Save Details
    print("\n--- STATION-SAVE DETAILS ---")
    for station_save, data in results['by_station_save'].items():
        if data['faulty_count'] > 0 or len(data['gaps_over_1ms']) > 0:
            print(f"\n{station_save}:")
            print(f"  Faulty/Total: {data['faulty_count']}/{data['total_count']} ({data['fault_rate']:.2%})")
            if data['gaps_over_1ms']:
                print(f"  Gaps > 1.0001s: {len(data['gaps_over_1ms'])}")
            if data['gaps_over_2s']:
                print(f"  Gaps > 2.0s: {len(data['gaps_over_2s'])}")
            if data['segment_violations']:
                within_30 = sum(1 for v in data['segment_violations'] if v['within_30s'])
                print(f"  Segment violations (within 30s): {within_30}/{len(data['segment_violations'])}")

def save_results_to_json(results: Dict, output_file: str = 'analysis_results.json'):
    """
    Save the analysis results to a JSON file.
    """
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {output_file}")

# Main execution
if __name__ == "__main__":
    # Replace with your Excel file path
    excel_file_path = "your_file.xlsx"
    
    try:
        # Perform analysis
        results = analyze_excel_27t(excel_file_path)
        
        # Print report
        print_analysis_report(results)
        
        # Save results to JSON
        save_results_to_json(results)
        
    except FileNotFoundError:
        print(f"Error: File '{excel_file_path}' not found. Please check the file path.")
    except Exception as e:
        print(f"An error occurred: {e}")
