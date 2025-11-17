def run_analysis(self, data: pd.DataFrame, voltage_col: str, output_folder: Path) -> pd.DataFrame:
    """
    Execute complete voltage analysis pipeline with dynamic thresholds.
    
    Performs a two-pass analysis:
    
    1. **First pass**: Collect metrics to establish baseline
    2. **Calculate dynamic thresholds**: ±50% of standard deviation for each OFP/test_case group
    3. **Second pass**: Apply dynamic thresholds for anomaly detection
    
    :param data: DataFrame with columns from translate_model_segments
    :type data: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param output_folder: Directory path for output files
    :type output_folder: Path
    :return: Combined results DataFrame containing all segment metrics and flagging info
    :rtype: pd.DataFrame
    
    .. note::
        Dynamic thresholds are calculated as mean ± (50% * std) for each metric
        within OFP/test_case groups. Requires at least 3 samples per group.
    """
    self.output_folder = output_folder
    output_folder.mkdir(exist_ok=True)
    data['voltage_col'] = voltage_col
    
    unique_runs = data['run_id'].unique()
    
    # First pass: baseline collection
    baseline_metrics = self._collect_baseline_metrics(data, voltage_col, unique_runs)
    if not baseline_metrics:
        return pd.DataFrame()
    
    # Calculate thresholds
    dynamic_thresholds = self._calculate_dynamic_thresholds(baseline_metrics)
    
    # Second pass: apply thresholds
    results_df, flagged_files = self._apply_thresholds(
        data, voltage_col, unique_runs, dynamic_thresholds
    )
    
    if results_df.empty:
        return results_df
    
    # Generate outputs
    self._generate_outputs(results_df, flagged_files, voltage_col, dynamic_thresholds)
    self._print_summary(results_df)
    
    return results_df


def _collect_baseline_metrics(self, data: pd.DataFrame, voltage_col: str, 
                              unique_runs: np.ndarray) -> list:
    """
    Collect metrics without thresholds for baseline calculation.
    
    :param data: Input DataFrame with voltage measurements
    :type data: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param unique_runs: Array of unique run identifiers
    :type unique_runs: np.ndarray
    :return: List of dictionaries containing baseline metrics
    :rtype: list
    """
    metrics = []
    
    for run_id in unique_runs:
        group_df = data[data['run_id'] == run_id].copy()
        try:
            file_results, _ = self.analyze_group(
                group_df, voltage_col, run_id, dynamic_thresholds=None
            )
            metrics.extend(file_results)
        except Exception as e:
            continue
    
    return metrics


def _calculate_dynamic_thresholds(self, metrics: list) -> dict:
    """
    Calculate dynamic thresholds from steady state segments.
    
    :param metrics: List of metric dictionaries from baseline collection
    :type metrics: list
    :return: Dictionary mapping OFP_test_case keys to threshold values
    :rtype: dict
    
    .. note::
        Only processes steady state segments with at least 3 samples per group.
        Thresholds are computed as mean ± (50% * standard deviation).
    """
    metrics_df = pd.DataFrame(metrics)
    steady_df = metrics_df[metrics_df['label'] == 'Steady State']
    
    thresholds = {}
    if steady_df.empty:
        return thresholds
    
    for (ofp, test_case), group in steady_df.groupby(['ofp', 'test_case']):
        if len(group) < 3:  # Need minimum samples
            continue
            
        group_thresholds = self._compute_metric_thresholds(group)
        thresholds[f"{ofp}_{test_case}"] = group_thresholds
    
    return thresholds


def _compute_metric_thresholds(self, group: pd.DataFrame) -> dict:
    """
    Compute upper/lower thresholds for each metric.
    
    :param group: DataFrame group containing metrics for a single OFP/test_case
    :type group: pd.DataFrame
    :return: Dictionary with min/max thresholds for each metric
    :rtype: dict
    
    :Example:
        >>> thresholds = self._compute_metric_thresholds(group_df)
        >>> {'max_variance': 0.5, 'min_variance': 0.1, ...}
    """
    thresholds = {}
    metrics = ['variance', 'std', 'abs_slope', 'iqr']
    
    for metric in metrics:
        if metric not in group.columns:
            continue
            
        mean_val = group[metric].mean()
        std_val = group[metric].std()
        
        # ±50% of standard deviation
        thresholds[f'max_{metric}'] = mean_val + (0.5 * std_val)
        thresholds[f'min_{metric}'] = max(0, mean_val - (0.5 * std_val))
    
    return thresholds


def _apply_thresholds(self, data: pd.DataFrame, voltage_col: str, 
                      unique_runs: np.ndarray, dynamic_thresholds: dict) -> tuple:
    """
    Apply dynamic thresholds and identify flagged segments.
    
    :param data: Input DataFrame with voltage measurements
    :type data: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param unique_runs: Array of unique run identifiers
    :type unique_runs: np.ndarray
    :param dynamic_thresholds: Dictionary of calculated thresholds per OFP/test_case
    :type dynamic_thresholds: dict
    :return: Tuple of (results DataFrame, list of flagged files)
    :rtype: tuple[pd.DataFrame, list]
    """
    all_results = []
    flagged_files = []
    
    for run_id in unique_runs:
        group_df = data[data['run_id'] == run_id].copy()
        
        try:
            file_results, analyzed_df = self.analyze_group(
                group_df, voltage_col, run_id, dynamic_thresholds=dynamic_thresholds
            )
            all_results.extend(file_results)
            
            # Check for flagged steady state segments
            if self._has_flagged_steady_state(file_results):
                group_df['is_flagged'] = True
                flagged_files.append((analyzed_df, voltage_col, run_id))
                
        except Exception:
            continue
    
    VoltageAnalyzer.results = all_results
    results_df = pd.DataFrame(all_results) if all_results else pd.DataFrame()
    
    return results_df, flagged_files


def _has_flagged_steady_state(self, results: list) -> bool:
    """
    Check if any steady state segment is flagged.
    
    :param results: List of result dictionaries from analysis
    :type results: list
    :return: True if any steady state segment is flagged, False otherwise
    :rtype: bool
    """
    return any(
        r.get('flagged', False) and r.get('label') == 'Steady State' 
        for r in results
    )


def _generate_outputs(self, results_df: pd.DataFrame, flagged_files: list, 
                     voltage_col: str, dynamic_thresholds: dict):
    """
    Generate plots and CSV outputs for analysis results.
    
    :param results_df: DataFrame containing analysis results
    :type results_df: pd.DataFrame
    :param flagged_files: List of tuples containing flagged data and metadata
    :type flagged_files: list
    :param voltage_col: Name of voltage column analyzed
    :type voltage_col: str
    :param dynamic_thresholds: Dictionary of calculated thresholds
    :type dynamic_thresholds: dict
    
    .. note::
        Creates 'flagged_plots' and 'flagged_data' subdirectories in output folder.
    """
    plots_folder = self.output_folder / 'flagged_plots'
    data_folder = self.output_folder / 'flagged_data'
    
    plots_folder.mkdir(exist_ok=True, parents=True)
    data_folder.mkdir(exist_ok=True, parents=True)
    
    # Generate plots for flagged files
    self._create_flagged_plots(flagged_files, voltage_col, plots_folder)
    
    # Save analysis results
    self._save_analysis_csvs(results_df, dynamic_thresholds, data_folder)
    
    # Create summary visualization
    try:
        summary_df = results_df[results_df["flagged"].notna()]
        self.create_summary_plot(summary_df)
    except Exception:
        pass


def _create_flagged_plots(self, flagged_files: list, voltage_col: str, 
                         plots_folder: Path):
    """
    Generate plots for flagged segments.
    
    :param flagged_files: List of tuples containing (DataFrame, voltage_col, grouping)
    :type flagged_files: list
    :param voltage_col: Name of voltage column
    :type voltage_col: str
    :param plots_folder: Path to directory for saving plots
    :type plots_folder: Path
    """
    plot_counter = {}
    
    for df, volt_col, grouping in flagged_files:
        try:
            plot_name = self._generate_plot_name(volt_col, grouping, plot_counter)
            plot_path = plots_folder / plot_name
            self.create_plot(df, volt_col, grouping, plot_path)
        except Exception:
            continue


def _generate_plot_name(self, voltage_col: str, grouping: dict, 
                       plot_counter: dict) -> str:
    """
    Generate unique plot filename.
    
    :param voltage_col: Name of voltage column
    :type voltage_col: str
    :param grouping: Dictionary containing grouping information (unit_id, test_case, etc.)
    :type grouping: dict
    :param plot_counter: Dictionary tracking duplicate names for uniqueness
    :type plot_counter: dict
    :return: Unique filename for the plot
    :rtype: str
    
    :Example:
        >>> name = self._generate_plot_name('voltage_28v_dc1_cal', {'unit_id': 'U1'}, {})
        >>> 'U1_NA_NA_NA_runNA_dc1.png'
    """
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
        return f"{base_name}_{plot_counter[base_name]}.png"
    
    plot_counter[base_name] = 0
    return f"{base_name}.png"


def _save_analysis_csvs(self, results_df: pd.DataFrame, 
                        dynamic_thresholds: dict, data_folder: Path):
    """
    Save various analysis results to CSV files.
    
    :param results_df: DataFrame containing all analysis results
    :type results_df: pd.DataFrame
    :param dynamic_thresholds: Dictionary of calculated thresholds
    :type dynamic_thresholds: dict
    :param data_folder: Path to directory for saving CSV files
    :type data_folder: Path
    
    .. note::
        Generates the following CSV files:
        
        - all_results.csv: Complete analysis results
        - flagged_steady_state.csv: Only flagged steady state segments
        - summary_stats.csv: Aggregated statistics by test case
        - dynamic_thresholds.csv: Calculated threshold values
        - dc_comparison.csv: DC-level comparison summary
    """
    # All results
    results_df.to_csv(data_folder / "all_results.csv", index=False)
    
    # Flagged steady state segments
    self._save_flagged_steady_state(results_df, data_folder)
    
    # Summary statistics
    self._save_summary_stats(results_df, data_folder)
    
    # Dynamic thresholds
    self._save_thresholds(dynamic_thresholds, data_folder)
    
    # DC comparison
    self._save_dc_comparison(results_df, data_folder)


def _save_flagged_steady_state(self, results_df: pd.DataFrame, data_folder: Path):
    """
    Save flagged steady state segments.
    
    :param results_df: DataFrame containing analysis results
    :type results_df: pd.DataFrame
    :param data_folder: Path to directory for saving CSV file
    :type data_folder: Path
    """
    if 'flagged' not in results_df.columns or 'label' not in results_df.columns:
        return
    
    flagged = results_df[
        (results_df['flagged'] == True) & 
        (results_df['label'] == 'steady_state')
    ]
    
    if not flagged.empty:
        flagged.to_csv(data_folder / "flagged_steady_state.csv", index=False)


def _save_summary_stats(self, results_df: pd.DataFrame, data_folder: Path):
    """
    Save summary statistics by test case and label.
    
    :param results_df: DataFrame containing analysis results
    :type results_df: pd.DataFrame
    :param data_folder: Path to directory for saving CSV file
    :type data_folder: Path
    
    .. note::
        Aggregates mean, std, min, max for voltage metrics and sums for counts.
    """
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


def _save_thresholds(self, dynamic_thresholds: dict, data_folder: Path):
    """
    Save dynamic thresholds to CSV.
    
    :param dynamic_thresholds: Dictionary mapping OFP_test_case to threshold values
    :type dynamic_thresholds: dict
    :param data_folder: Path to directory for saving CSV file
    :type data_folder: Path
    
    :Example:
        Output CSV format::
        
            ofp,test_case,metric,min_threshold,max_threshold
            OFP1,TEST1,variance,0.1,0.5
            OFP1,TEST1,std,0.05,0.25
    """
    thresh_data = []
    
    for group_key, thresholds in dynamic_thresholds.items():
        ofp, test_case = group_key.split('_', 1)
        
        for metric in ['variance', 'std', 'abs_slope', 'iqr']:
            min_key, max_key = f'min_{metric}', f'max_{metric}'
            
            if min_key in thresholds and max_key in thresholds:
                thresh_data.append({
                    'ofp': ofp,
                    'test_case': test_case,
                    'metric': metric,
                    'min_threshold': thresholds[min_key],
                    'max_threshold': thresholds[max_key]
                })
    
    if thresh_data:
        pd.DataFrame(thresh_data).to_csv(
            data_folder / "dynamic_thresholds.csv", index=False
        )


def _save_dc_comparison(self, results_df: pd.DataFrame, data_folder: Path):
    """
    Save DC comparison summary.
    
    :param results_df: DataFrame containing analysis results with dc_folder column
    :type results_df: pd.DataFrame
    :param data_folder: Path to directory for saving CSV file
    :type data_folder: Path
    
    .. note::
        Groups results by dc_folder and label, computing mean and sum statistics.
    """
    if 'dc_folder' not in results_df.columns:
        return
    
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


def _print_summary(self, results_df: pd.DataFrame):
    """
    Print analysis summary to console.
    
    :param results_df: DataFrame containing analysis results
    :type results_df: pd.DataFrame
    
    .. note::
        Only prints information about flagged steady state segments.
    """
    if 'flagged' not in results_df.columns:
        return
    
    steady_flagged = results_df[
        (results_df['flagged']) & (results_df['label'] == 'Steady State')
    ]
    
    n_flagged = len(steady_flagged)
    print(f"\nFlagged steady-state segments: {n_flagged}")
    
    if n_flagged > 0:
        print("\nBreakdown by test case:")
        for tc in steady_flagged['test_case'].unique():
            count = len(steady_flagged[steady_flagged['test_case'] == tc])
            print(f"  {tc}: {count}")
