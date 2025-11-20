#!/usr/bin/env python3
"""
Bus Flip Detector

Analyzes bus monitor data to identify rapid bus transitions (flips) between Bus A and Bus B.
A bus flip is defined as a bus change occurring within 100ms with identical message content.

Author: Bus Monitor Analysis Team
"""

import pandas as pd
import re
from pathlib import Path
from typing import List, Dict, Optional


class BusFlipDetector:
    """
    Detects bus flips in parquet-formatted bus monitor data.
    
    :param parquet_path: Path to input parquet file
    :type parquet_path: str
    
    :ivar parquet_path: Path to input parquet file
    :vartype parquet_path: Path
    :ivar df: Loaded data
    :vartype df: pd.DataFrame
    :ivar flips: Detected bus flips
    :vartype flips: List[Dict]
    """
    
    FLIP_THRESHOLD_MS = 100
    REQUIRED_COLUMNS = ['unit_id', 'station', 'save', 'bus', 'timestamp', 'decoded_description']
    
    def __init__(self, parquet_path: str):
        """
        Initialize the Bus Flip Detector.
        
        :param parquet_path: Path to the parquet file containing bus monitor data
        :type parquet_path: str
        """
        self.parquet_path = Path(parquet_path)
        self.df: Optional[pd.DataFrame] = None
        self.flips: List[Dict] = []
        
    def load_data(self) -> None:
        """
        Load and validate parquet data.
        
        :raises ValueError: If required columns are missing from the data
        """
        self.df = pd.read_parquet(self.parquet_path)
        
        missing_cols = [col for col in self.REQUIRED_COLUMNS if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
    
    @staticmethod
    def extract_message_type(decoded_desc: str) -> Optional[str]:
        """
        Extract message type from decoded description.
        
        :param decoded_desc: Decoded description string in format (X-[MSG_TYPE]-Y)
        :type decoded_desc: str
        :return: Message type string or None if not found
        :rtype: Optional[str]
        """
        if pd.isna(decoded_desc):
            return None
        
        pattern = r'\((\d+)-\[([^\]]+)\]-(\d+)\)'
        match = re.search(pattern, str(decoded_desc))
        
        return match.group(2) if match else None
    
    def check_dc_states(self, row: pd.Series) -> bool:
        """
        Validate DC state requirements.
        
        :param row: Data row to check
        :type row: pd.Series
        :return: True if at least one DC is active or DC columns don't exist
        :rtype: bool
        """
        if 'dc1_state' not in row.index and 'dc2_state' not in row.index:
            return True
        
        dc_active = False
        
        if 'dc1_state' in row.index:
            dc1_val = str(row['dc1_state']).strip().upper()
            dc_active |= dc1_val in ['1', 'TRUE', 'ON', 'YES']
        
        if 'dc2_state' in row.index:
            dc2_val = str(row['dc2_state']).strip().upper()
            dc_active |= dc2_val in ['1', 'TRUE', 'ON', 'YES']
        
        return dc_active
    
    def detect_flips_for_group(
        self, 
        group_df: pd.DataFrame, 
        unit_id: str, 
        station: str, 
        save: str
    ) -> List[Dict]:
        """
        Detect bus flips within a specific unit/station/save group.
        
        Flip criteria:
        
        1. Bus transition (A->B or B->A)
        2. Time difference < 100ms
        3. Identical decoded_description
        4. At least one DC active (if applicable)
        
        :param group_df: DataFrame subset for this group
        :type group_df: pd.DataFrame
        :param unit_id: Unit identifier
        :type unit_id: str
        :param station: Station identifier
        :type station: str
        :param save: Save identifier
        :type save: str
        :return: List of detected flips
        :rtype: List[Dict]
        """
        if len(group_df) < 2:
            return []
        
        df = group_df.sort_values('timestamp').reset_index(drop=True)
        
        df['prev_bus'] = df['bus'].shift(1)
        df['prev_timestamp'] = df['timestamp'].shift(1)
        df['prev_decoded'] = df['decoded_description'].shift(1)
        df['time_diff_ms'] = (df['timestamp'] - df['prev_timestamp']) * 1000
        
        flip_mask = (
            (df['bus'] != df['prev_bus']) &
            (df['time_diff_ms'] < self.FLIP_THRESHOLD_MS) &
            (df['time_diff_ms'].notna()) &
            (df['decoded_description'] == df['prev_decoded'])
        )
        
        flips = []
        for idx in df[flip_mask].index:
            curr_row = df.iloc[idx]
            prev_row = df.iloc[idx - 1]
            
            if not (self.check_dc_states(prev_row) and self.check_dc_states(curr_row)):
                continue
            
            msg_type = self.extract_message_type(curr_row['decoded_description'])
            bus_transition = f"{prev_row['bus']} to {curr_row['bus']}"
            
            timestamp_busA = prev_row['timestamp'] if prev_row['bus'] == 'A' else curr_row['timestamp']
            timestamp_busB = prev_row['timestamp'] if prev_row['bus'] == 'B' else curr_row['timestamp']
            
            flips.append({
                'unit_id': unit_id,
                'station': station,
                'save': save,
                'bus_transition': bus_transition,
                'msg_type': msg_type,
                'timestamp_busA': timestamp_busA,
                'timestamp_busB': timestamp_busB,
                'timestamp_diff_ms': round(curr_row['time_diff_ms'], 3),
                'decoded_description': curr_row['decoded_description']
            })
        
        return flips
    
    def detect_all_flips(self) -> None:
        """
        Detect bus flips across all unit/station/save combinations.
        
        :raises ValueError: If data not loaded
        """
        if self.df is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        grouped = self.df.groupby(['unit_id', 'station', 'save'])
        
        all_flips = []
        for (unit_id, station, save), group_df in grouped:
            flips = self.detect_flips_for_group(group_df, unit_id, station, save)
            all_flips.extend(flips)
        
        self.flips = all_flips
    
    def get_summary(self) -> pd.DataFrame:
        """
        Generate summary of bus flips by location.
        
        :return: DataFrame with columns: unit_id, station, save, flip_count
        :rtype: pd.DataFrame
        """
        if not self.flips:
            return pd.DataFrame(columns=['unit_id', 'station', 'save', 'flip_count'])
        
        df_flips = pd.DataFrame(self.flips)
        summary = df_flips.groupby(['unit_id', 'station', 'save']).size().reset_index(name='flip_count')
        
        return summary.sort_values('flip_count', ascending=False)
    
    def run(self, output_file: str = "bus_flips.csv", verbose: bool = True) -> pd.DataFrame:
        """
        Execute complete bus flip detection pipeline.
        
        :param output_file: Path for summary CSV output
        :type output_file: str
        :param verbose: Print summary to console
        :type verbose: bool
        :return: Summary DataFrame
        :rtype: pd.DataFrame
        """
        self.load_data()
        self.detect_all_flips()
        
        summary = self.get_summary()
        
        if not summary.empty:
            summary.to_csv(output_file, index=False)
            
            if verbose:
                print(f"Detected {len(self.flips):,} bus flips across {len(summary)} locations")
                print(f"Summary saved to: {output_file}\n")
                print("Top 5 locations:")
                print(summary.head().to_string(index=False))
        elif verbose:
            print("No bus flips detected")
        
        return summary


if __name__ == "__main__":
    # Example usage
    parquet_file = "bus_monitor_data.parquet"
    
    detector = BusFlipDetector(parquet_file)
    summary = detector.run()
