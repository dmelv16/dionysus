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
    # Extract and clean voltage values
    voltage_values = df[df['predicted_status'] == label][voltage_col].values
    voltage_values = voltage_values[~np.isnan(voltage_values)]
    
    if len(voltage_values) == 0:
        return None
    
    # Build metrics dictionary
    metrics = {
        **grouping, 
        'label': label, 
        'voltage_column': voltage_col,
        **self.calculate_basic_metrics(voltage_values),
        **self.calculate_slope_metrics(voltage_values),
        'flagged': False,
        'flags': '',
        'flag_reasons': ''
    }
    
    # Check thresholds only for steady state
    if label == 'steady_state':
        # Select appropriate thresholds
        if dynamic_thresholds:
            group_key = f"{grouping.get('ofp', 'NA')}_{grouping.get('test_case', 'NA')}"
            thresholds = dynamic_thresholds.get(group_key, self.steady_state_thresholds)
            threshold_type = 'dynamic'
        else:
            thresholds = self.steady_state_thresholds
            threshold_type = 'fixed'
        
        # Check thresholds and update flags
        flag_reasons = self.check_thresholds(metrics, thresholds, threshold_type)
        
        if flag_reasons:
            metrics['flagged'] = True
            metrics['flags'] = 'all_thresholds_failed'
            metrics['flag_reasons'] = '; '.join(flag_reasons)
    
    return metrics
