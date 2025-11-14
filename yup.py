def run_analysis(self, data: pd.DataFrame, voltage_col: str, output_folder: Path) -> pd.DataFrame:
    """
    Execute complete voltage analysis pipeline with dynamic thresholds.
    
    Performs a two-pass analysis:
    
    1. **First pass**: Collect metrics to establish baseline
    2. **Calculate thresholds**: ±50% of standard deviation for each group
    3. **Second pass**: Apply dynamic thresholds for anomaly detection
    
    :param data: DataFrame containing voltage measurements and metadata
    :type data: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param output_folder: Directory path for output files and plots
    :type output_folder: Path
    :return: DataFrame containing all segment metrics and flagging information
    :rtype: pd.DataFrame
    
    .. note::
       Dynamic thresholds require at least 3 samples per OFP/test_case group
    """
    self.output_folder = output_folder
    output_folder.mkdir(exist_ok=True)
    data['voltage_col'] = voltage_col
    
    unique_runs = data['run_id'].unique()
    
    # First pass: baseline metrics
    baseline_metrics = self._collect_baseline_metrics(data, voltage_col, unique_runs)
    if not baseline_metrics:
        return pd.DataFrame()
    
    # Calculate dynamic thresholds
    dynamic_thresholds = self._calculate_dynamic_thresholds(baseline_metrics)
    
    # Second pass: apply thresholds
    results, flagged_files, segments = self._apply_thresholds(
        data, voltage_col, unique_runs, dynamic_thresholds
    )
    
    if not results:
        return pd.DataFrame()
    
    results_df = pd.DataFrame(results)
    
    # Generate outputs
    self._save_outputs(results_df, flagged_files, segments, dynamic_thresholds, voltage_col)
    
    return results_df

def _collect_baseline_metrics(self, data: pd.DataFrame, voltage_col: str, unique_runs: np.ndarray) -> list:
    """
    Collect baseline metrics for dynamic threshold calculation.
    
    :param data: DataFrame containing voltage measurements
    :type data: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param unique_runs: Array of unique run IDs to process
    :type unique_runs: np.ndarray
    :return: List of metric dictionaries for all segments
    :rtype: list
    """
    all_metrics = []
    
    for run_id in unique_runs:
        group_df = data[data['run_id'] == run_id].copy()
        try:
            file_results, _ = self.analyze_group(group_df, voltage_col, run_id, dynamic_thresholds=None)
            all_metrics.extend(file_results)
        except Exception:
            continue
    
    return all_metrics

def _calculate_dynamic_thresholds(self, metrics: list) -> dict:
    """
    Calculate dynamic thresholds from steady state metrics.
    
    Computes thresholds as mean ± (50% × standard deviation) for each metric
    within OFP/test_case groups.
    
    :param metrics: List of metric dictionaries from baseline collection
    :type metrics: list
    :return: Dictionary mapping group keys to threshold values
    :rtype: dict
    
    .. note::
       Groups with fewer than 3 samples are skipped
    """
    metrics_df = pd.DataFrame(metrics)
    steady_df = metrics_df[metrics_df['label'] == 'Steady State']
    
    thresholds = {}
    if steady_df.empty:
        return thresholds
    
    for (ofp, test_case), group in steady_df.groupby(['ofp', 'test_case']):
        if len(group) < 3:  # Skip groups with insufficient samples
            continue
            
        group_thresholds = {}
        for metric in ['variance', 'std', 'abs_slope', 'iqr']:
            if metric in group.columns:
                mean_val = group[metric].mean()
                std_val = group[metric].std()
                
                # ±50% of standard deviation
                group_thresholds[f'max_{metric}'] = mean_val + (0.5 * std_val)
                group_thresholds[f'min_{metric}'] = max(0, mean_val - (0.5 * std_val))
        
        thresholds[f"{ofp}_{test_case}"] = group_thresholds
    
    return thresholds

def _apply_thresholds(self, data: pd.DataFrame, voltage_col: str, unique_runs: np.ndarray, 
                      dynamic_thresholds: dict) -> tuple:
    """
    Apply dynamic thresholds and identify flagged segments.
    
    :param data: DataFrame containing voltage measurements
    :type data: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param unique_runs: Array of unique run IDs to process
    :type unique_runs: np.ndarray
    :param dynamic_thresholds: Dictionary of calculated thresholds per group
    :type dynamic_thresholds: dict
    :return: Tuple of (results list, flagged files list, segments list)
    :rtype: tuple
    
    .. note::
       Only steady state segments are evaluated for flagging
    """
    all_results = []
    flagged_files = []
    all_segments = []
    
    for run_id in unique_runs:
        group_df = data[data['run_id'] == run_id].copy()
        
        try:
            file_results, analyzed_df = self.analyze_group(
                group_df, voltage_col, run_id, dynamic_thresholds=dynamic_thresholds
            )
            all_results.extend(file_results)
            
            # Check for steady state flags
            is_flagged = any(
                r.get('flagged', False) and r.get('label') == 'Steady State' 
                for r in file_results
            )
            
            group_df['is_flagged'] = is_flagged
            all_segments.append(group_df.copy())
            
            if is_flagged:
                flagged_files.append((analyzed_df, voltage_col, run_id))
                
        except Exception:
            continue
    
    return all_results, flagged_files, all_segments

def _save_outputs(self, results_df: pd.DataFrame, flagged_files: list, segments: list,
                  dynamic_thresholds: dict, voltage_col: str):
    """
    Save all output files and generate visualizations.
    
    Creates output directories and saves results, plots, and summaries.
    
    :param results_df: DataFrame containing analysis results
    :type results_df: pd.DataFrame
    :param flagged_files: List of tuples containing flagged segment data
    :type flagged_files: list
    :param segments: List of DataFrames containing all segment data
    :type segments: list
    :param dynamic_thresholds: Dictionary of calculated thresholds
    :type dynamic_thresholds: dict
    :param voltage_col: Name of voltage column being analyzed
    :type voltage_col: str
    """
    # Create output directories
    plots_folder = self.output_folder / 'flagged_plots'
    data_folder = self.output_folder / 'flagged_data'
    plots_folder.mkdir(exist_ok=True, parents=True)
    data_folder.mkdir(exist_ok=True, parents=True)
    
    # Save main results
    results_df.to_csv(data_folder / "all_results.csv", index=False)
    
    # Generate plots for flagged files
    self._generate_flagged_plots(flagged_files, plots_folder, voltage_col)
    
    # Save analysis outputs
    self._save_analysis_results(results_df, segments, dynamic_thresholds, data_folder)
    
    # Generate summary plot
    try:
        flagged_df = results_df[results_df["flagged"].notna()]
        if not flagged_df.empty:
            self.create_summary_plot(flagged_df)
    except Exception:
        pass

def _generate_flagged_plots(self, flagged_files: list, plots_folder: Path, voltage_col: str):
    """
    Generate plots for flagged segments with unique naming.
    
    :param flagged_files: List of tuples containing flagged segment data
    :type flagged_files: list
    :param plots_folder: Directory path for saving plots
    :type plots_folder: Path
    :param voltage_col: Name of voltage column being analyzed
    :type voltage_col: str
    
    .. note::
       Handles duplicate names by appending counter suffix
    """
    plot_counter = {}
    
    for df, voltage_col, grouping in flagged_files:
        try:
            # Generate unique plot name
            dc_name = voltage_col.replace("voltage_28v_", "").replace("_cal", "")
            base_name = (
                f"{grouping.get('unit_id', 'NA')}_"
                f"{grouping.get('test_case', 'NA')}_"
                f"{grouping.get('save', 'NA')}_"
                f"{grouping.get('station', 'NA')}_"
                f"run{grouping.get('test_run', 'NA')}_"
                f"{dc_name}"
            )
            
            if base_name in plot_counter:
                plot_counter[base_name] += 1
                plot_name = f"{base_name}_{plot_counter[base_name]}.png"
            else:
                plot_counter[base_name] = 0
                plot_name = f"{base_name}.png"
            
            self.create_plot(df, voltage_col, grouping, plots_folder / plot_name)
        except Exception:
            continue

def _save_analysis_results(self, results_df: pd.DataFrame, segments: list, 
                          dynamic_thresholds: dict, data_folder: Path):
    """
    Save all analysis results and statistical summaries.
    
    Generates multiple CSV outputs including flagged segments, summaries,
    thresholds, and combined segment data.
    
    :param results_df: DataFrame containing analysis results
    :type results_df: pd.DataFrame
    :param segments: List of DataFrames containing all segment data
    :type segments: list
    :param dynamic_thresholds: Dictionary of calculated thresholds
    :type dynamic_thresholds: dict
    :param data_folder: Directory path for saving data files
    :type data_folder: Path
    
    .. note::
       Creates the following files:
       
       - flagged_steady_state.csv: Only flagged steady state segments
       - summary_stats.csv: Statistics grouped by test case
       - dc_comparison.csv: Statistics grouped by DC folder
       - dynamic_thresholds.csv: Calculated threshold values
       - all_segments.csv: Combined segment data
    """
    # Flagged steady state results
    if 'flagged' in results_df.columns and 'label' in results_df.columns:
        flagged_steady = results_df[
            (results_df['flagged'] == True) & 
            (results_df['label'] == 'steady_state')
        ]
        flagged_steady.to_csv(data_folder / "flagged_steady_state.csv", index=False)
        
        # Test case summary statistics
        summary = (
            results_df.groupby(['test_case', 'label', 'ofp'])
            .agg({
                'mean_voltage': ['mean', 'std', 'min', 'max'],
                'variance': ['mean', 'max'],
                'cv': ['mean', 'max'],
                'n_points': 'sum',
                'flagged': 'sum'
            })
            .round(3)
        )
        summary.columns = ["_".join(col).strip() for col in summary.columns.values]
        summary.to_csv(data_folder / "summary_stats.csv")
        
        # DC folder comparison
        dc_summary = (
            results_df.groupby(['dc_folder', 'label'])
            .agg({
                'mean_voltage': ['mean', 'std'],
                'cv': 'mean',
                'flagged': 'sum',
                'n_points': 'sum'
            })
            .round(3)
        )
        dc_summary.columns = ['_'.join(col).strip() for col in dc_summary.columns.values]
        dc_summary.to_csv(data_folder / 'dc_comparison.csv')
    
    # Save dynamic thresholds
    thresh_data = []
    for group_key, thresholds in dynamic_thresholds.items():
        ofp, test_case = group_key.split('_', 1)
        for base_metric in ['variance', 'std', 'abs_slope', 'iqr']:
            min_key, max_key = f'min_{base_metric}', f'max_{base_metric}'
            if min_key in thresholds and max_key in thresholds:
                thresh_data.append({
                    'ofp': ofp,
                    'test_case': test_case,
                    'metric': base_metric,
                    'min_threshold': thresholds[min_key],
                    'max_threshold': thresholds[max_key]
                })
    
    if thresh_data:
        pd.DataFrame(thresh_data).to_csv(data_folder / "dynamic_thresholds.csv", index=False)
    
    # Save all segments
    if segments:
        combined = pd.concat(segments, ignore_index=True)
        grouping_cols = ["ofp", "test_case", "unit_id", "station", "save", "test_run", "voltage_col"]
        existing_cols = [col for col in grouping_cols if col in combined.columns]
        other_cols = [col for col in combined.columns if col not in existing_cols]
        combined = combined[existing_cols + other_cols]
        combined.to_csv(data_folder / "all_segments.csv", index=False)
