import pandas as pd

# Read the parquet file
df = pd.read_parquet('your_file.parquet')

# First, get the start time for each segment (from ALL messages, not just 27T)
segment_starts = df.groupby(['unit_id', 'station', 'save', 'segment'])['timestamp'].min().reset_index()
segment_starts.columns = ['unit_id', 'station', 'save', 'segment', 'segment_start']

# Filter for messages containing '27T'
df_27t = df[df['decoded_description'].str.contains('27T', na=False)].copy()

# Merge to get segment start times
df_27t = df_27t.merge(segment_starts, on=['unit_id', 'station', 'save', 'segment'])

# Filter out 27T messages in the first 30 seconds of each segment
df_27t = df_27t[df_27t['timestamp'] >= df_27t['segment_start'] + 30.0]

# Group by unit_id, station, save, and segment
groups = df_27t.groupby(['unit_id', 'station', 'save', 'segment'])

# Track overall stats
total_27t_count = 0
gaps_exceeding_threshold = []
all_gaps = []

# Process each group
for (unit_id, station, save, segment), group in groups:
    # Sort by timestamp
    group = group.sort_values('timestamp')
    
    # Need at least 2 messages to calculate gaps
    if len(group) < 2:
        continue
    
    # Count messages
    total_27t_count += len(group)
    
    # Calculate gaps between consecutive messages
    timestamps = group['timestamp'].values
    for i in range(1, len(timestamps)):
        gap = timestamps[i] - timestamps[i-1]
        all_gaps.append(gap)
        if gap > 1.1:
            gaps_exceeding_threshold.append(gap)

# Print results
print(f"\nTotal 27T messages (after 30s filter): {total_27t_count}")
print(f"Total gaps exceeding 1.1 seconds: {len(gaps_exceeding_threshold)}")

if all_gaps:
    highest_gaps = sorted(all_gaps, reverse=True)[:10]
    print(f"\nHighest gaps (seconds):")
    for i, gap in enumerate(highest_gaps, 1):
        print(f"  {i}. {gap:.6f}")
else:
    print("No gaps found")
