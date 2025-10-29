def check_threshold(
        self, 
        value: float, 
        min_threshold: float, 
        max_threshold: float,
        metric_name: str,
        round_digits: int = 4
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if a value is within threshold bounds.
        
        :param value: Value to check against thresholds
        :type value: float
        :param min_threshold: Minimum acceptable value
        :type min_threshold: float
        :param max_threshold: Maximum acceptable value
        :type max_threshold: float
        :param metric_name: Name of metric for error message generation
        :type metric_name: str
        :param round_digits: Number of decimal digits for rounding, defaults to 4
        :type round_digits: int, optional
        :return: Tuple of (failed: bool, reason: Optional[str])
        :rtype: Tuple[bool, Optional[str]]
        
        :Example:
        
        >>> analyzer = SimplifiedVoltageAnalyzer()
        >>> failed, reason = analyzer.check_threshold(1.5, 0.0, 1.0, "Variance")
        >>> print(failed, reason)
        True, "Variance 1.5000 > 1.0000 (dynamic)"
        """
        value_rounded = round(value, round_digits)
        min_rounded = round(min_threshold, round_digits)
        max_rounded = round(max_threshold, round_digits)
        
        if value_rounded < min_rounded:
            reason = f"{metric_name} {value:.4f} < {min_threshold:.4f} (dynamic)"
            return True, reason
        elif value_rounded > max_rounded:
            reason = f"{metric_name} {value:.4f} > {max_threshold:.4f} (dynamic)"
            return True, reason
        
        return False, None

    def check_dynamic_thresholds(
        self, 
        metrics: Dict[str, float], 
        thresholds: Dict[str, float],
        round_digits: int = 4
    ) -> List[str]:
        """
        Check metrics against dynamic thresholds for anomaly detection.
        
        Evaluates variance, std, slope, and IQR against provided thresholds.
        Flags anomaly only if ALL 4 metrics fail their respective thresholds.
        
        :param metrics: Dictionary of calculated metrics
        :type metrics: Dict[str, float]
        :param thresholds: Dictionary of threshold values (min/max for each metric)
        :type thresholds: Dict[str, float]
        :param round_digits: Number of decimal digits for rounding, defaults to 4
        :type round_digits: int, optional
        :return: List of failure reasons (empty if not all checks failed)
        :rtype: List[str]
        
        .. note::
           Required threshold keys: min_variance, max_variance, min_std, max_std,
           min_slope, max_slope, min_iqr, max_iqr
        """
        failed_checks = 0
        reasons = []
        
        # Define metrics to check with their threshold keys and labels
        checks = [
            ('variance', 'Variance'),
            ('std', 'Std'),
            ('slope', 'Slope'),
            ('iqr', 'IQR')
        ]
        
        for metric_key, metric_label in checks:
            min_key = f'min_{metric_key}'
            max_key = f'max_{metric_key}'
            
            if min_key in thresholds and max_key in thresholds:
                failed, reason = self.check_threshold(
                    metrics[metric_key], 
                    thresholds[min_key], 
                    thresholds[max_key],
                    metric_label,
                    round_digits
                )
                if failed:
                    failed_checks += 1
                    reasons.append(reason)
        
        # Only return reasons if ALL 4 thresholds failed
        if failed_checks < 4:
            reasons = []
        
        return reasons
    
    def check_fixed_thresholds(self, metrics: Dict[str, float]) -> List[str]:
        """
        Check metrics against fixed thresholds for steady-state anomaly detection.
        
        Evaluates variance, std, abs_slope, and IQR against fixed thresholds.
        Flags anomaly only if ALL 4 metrics exceed their thresholds.
        
        :param metrics: Dictionary of calculated metrics
        :type metrics: Dict[str, float]
        :return: List of failure reasons (empty if not all checks failed)
        :rtype: List[str]
        
        .. warning::
           All 4 thresholds must fail for flagging to occur
        """
        failed_checks = 0
        reasons = []
        
        if metrics['variance'] > self.steady_state_thresholds['max_variance']:
            failed_checks += 1
            reasons.append(f"Variance {metrics['variance']:.4f} > {self.steady_state_thresholds['max_variance']:.4f}")
        
        if metrics['std'] > self.steady_state_thresholds['max_std']:
            failed_checks += 1
            reasons.append(f"Std {metrics['std']:.4f} > {self.steady_state_thresholds['max_std']:.4f}")
        
        if metrics['abs_slope'] > self.steady_state_thresholds['max_slope']:
            failed_checks += 1
            reasons.append(f"Slope {metrics['abs_slope']:.4f} > {self.steady_state_thresholds['max_slope']:.4f}")
        
        # Check IQR threshold (adding this as 4th check)
        max_iqr = self.steady_state_thresholds.get('max_iqr', 1.0)  # Default to 1.0 if not set
        if metrics.get('iqr', 0) > max_iqr:
            failed_checks += 1
            reasons.append(f"IQR {metrics['iqr']:.4f} > {max_iqr:.4f}")
        
        # Return reasons only if ALL 4 checks failed
        if failed_checks < 4:
            reasons = []
        
        return reasons
    
    def process_label_metrics(
        self, 
        df: pd.DataFrame,
        voltage_col: str,
        label: str, 
        grouping: Dict[str, str],
        dynamic_thresholds: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Optional[Dict]:
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
        valid_mask = ~np.isnan(voltage_values)
        voltage_values = voltage_values[valid_mask]
        
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
                    flag_reasons = self.check_dynamic_thresholds(
                        metrics, 
                        dynamic_thresholds[group_key]
                    )
            else:
                flag_reasons = self.check_fixed_thresholds(metrics)
            
            should_flag = bool(flag_reasons)  # flag_reasons is not empty
            metrics['flagged'] = should_flag
            metrics['flags'] = 'all_thresholds_failed' if should_flag else ''
            metrics['flag_reasons'] = '; '.join(flag_reasons)
        else:
            metrics['flagged'] = False
            metrics['flags'] = ''
            metrics['flag_reasons'] = ''
        
        return metrics
