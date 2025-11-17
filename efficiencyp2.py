def check_thresholds(self, metrics: Dict[str, float], thresholds: Dict[str, float], 
                     threshold_type: str = 'dynamic', round_digits: int = 4) -> List[str]:
    """
    Check metrics against thresholds for anomaly detection.
    
    Evaluates variance, std, abs_slope, and IQR against provided thresholds.
    Flags anomaly only if ALL 4 metrics fail their respective thresholds.
    
    :param metrics: Dictionary of calculated metrics
    :type metrics: Dict[str, float]
    :param thresholds: Dictionary of threshold values (min/max for dynamic, max only for fixed)
    :type thresholds: Dict[str, float]
    :param threshold_type: Type of threshold check ('dynamic' or 'fixed'), defaults to 'dynamic'
    :type threshold_type: str, optional
    :param round_digits: Number of decimal digits for rounding, defaults to 4
    :type round_digits: int, optional
    :return: List of failure reasons (empty if not all checks failed)
    :rtype: List[str]
    
    .. note::
       Returns reasons only if ALL 4 metrics fail their thresholds
    """
    failed_checks = 0
    reasons = []
    
    # Define metrics to check
    check_metrics = ['variance', 'std', 'abs_slope', 'iqr']
    
    for metric in check_metrics:
        value = metrics.get(metric, 0)
        value_rounded = round(value, round_digits)
        
        if threshold_type == 'dynamic':
            # Dynamic thresholds have both min and max
            min_threshold = thresholds.get(f'min_{metric}')
            max_threshold = thresholds.get(f'max_{metric}')
            
            if min_threshold is not None and max_threshold is not None:
                min_rounded = round(min_threshold, round_digits)
                max_rounded = round(max_threshold, round_digits)
                
                if value_rounded < min_rounded:
                    failed_checks += 1
                    reasons.append(f"{metric.replace('_', ' ').title()} {value:.4f} < {min_threshold:.4f} (dynamic)")
                elif value_rounded > max_rounded:
                    failed_checks += 1
                    reasons.append(f"{metric.replace('_', ' ').title()} {value:.4f} > {max_threshold:.4f} (dynamic)")
        else:
            # Fixed thresholds only have max
            max_threshold = thresholds.get(f'max_{metric}', 
                                         self.steady_state_thresholds.get(f'max_{metric}', 1.0))
            max_rounded = round(max_threshold, round_digits)
            
            if value_rounded > max_rounded:
                failed_checks += 1
                reasons.append(f"{metric.replace('_', ' ').title()} {value:.4f} > {max_threshold:.4f}")
    
    # Only return reasons if ALL 4 thresholds failed
    return reasons if failed_checks == 4 else []

def process_label_metrics(self, df: pd.DataFrame, voltage_col: str, label: str, 
                         grouping: Dict[str, str], 
                         dynamic_thresholds: Optional[Dict[str, Dict[str, float]]] = None) -> Optional[Dict]:
    """
    Process metrics for a specific predicted_status label.
    
    Calculates statistical metrics for voltage values with a given status
    and checks for anomalies if status is steady_state.
    
    :param df: DataFrame containing voltage data
    :type df: pd.DataFrame
    :param voltage_col: Name of voltage column to analyze
    :type voltage_col: str
    :param label: Predicted status value to filter by
    :type label: str
    :param grouping: Dictionary containing group identification info
    :type grouping: Dict[str, str]
    :param dynamic_thresholds: Optional dynamic thresholds dictionary, defaults to None
    :type dynamic_thresholds: Optional[Dict[str, Dict[str, float]]], optional
    :return: Dictionary of metrics including statistics and flagging info, or None if no data
    :rtype: Optional[Dict]
    
    .. note::
       Only performs anomaly checking for steady_state status
    """
    label_data = df[df['predicted_status'] == label]
    voltage_values = label_data[voltage_col].values
    
    # Remove NaN values
    voltage_values = voltage_values[~np.isnan(voltage_values)]
    
    if len(voltage_values) == 0:
        return None
    
    # Calculate all metrics
    metrics = {**grouping, 'label': label, 'voltage_column': voltage_col}
    metrics.update(self.calculate_basic_metrics(voltage_values))
    metrics.update(self.calculate_slope_metrics(voltage_values))
    
    # Only flag anomalies for steady state
    if label == 'steady_state':
        flag_reasons = []
        
        if dynamic_thresholds:
            group_key = f"{grouping.get('ofp', 'NA')}_{grouping.get('test_case', 'NA')}"
            if group_key in dynamic_thresholds:
                flag_reasons = self.check_thresholds(
                    metrics, 
                    dynamic_thresholds[group_key],
                    threshold_type='dynamic'
                )
        else:
            flag_reasons = self.check_thresholds(
                metrics, 
                self.steady_state_thresholds,
                threshold_type='fixed'
            )
        
        should_flag = bool(flag_reasons)
        metrics['flagged'] = should_flag
        metrics['flags'] = 'all_thresholds_failed' if should_flag else ''
        metrics['flag_reasons'] = '; '.join(flag_reasons)
    else:
        metrics['flagged'] = False
        metrics['flags'] = ''
        metrics['flag_reasons'] = ''
    
    return metrics
