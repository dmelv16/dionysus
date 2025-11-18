import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from voltage_analyzer import VoltageAnalyzer  # Updated import name


@pytest.fixture
def analyzer():
    """Create a VoltageAnalyzer instance with default thresholds."""
    thresholds = {
        'max_variance': 1.5,
        'max_std': 2.0,
        'max_abs_slope': 0.5,  # Changed from max_slope
        'max_iqr': 1.0
    }
    return VoltageAnalyzer(thresholds)


@pytest.fixture
def sample_voltage_data():
    """Create sample voltage data for testing - typical range 15-30V."""
    return np.array([24.0, 24.5, 24.2, 24.8, 24.3, 24.7, 24.1, 24.6])


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing - typical voltage range."""
    return pd.DataFrame({
        'voltage_28v_dc1_cal': [24.0, 24.5, 24.2, 24.8, 24.3, 24.7, 24.1, 24.6],
        'timestamp': range(8),
        'predicted_cluster': [1, 1, 1, 1, 2, 2, 2, 2],
        'predicted_status': ['Steady State'] * 8,
        'run_id': ['run1'] * 8
    })


@pytest.fixture
def steady_state_dataframe():
    """Create a DataFrame with steady state data - very stable voltage."""
    return pd.DataFrame({
        'voltage_28v_dc1_cal': [24.0, 24.1, 24.0, 24.1, 24.0, 24.1, 24.0, 24.1, 24.0, 24.1],
        'timestamp': range(10),
        'predicted_cluster': [1] * 10,
        'predicted_status': ['Steady State'] * 10,
        'run_id': ['run1'] * 10
    })


@pytest.fixture
def high_variance_dataframe():
    """Create a DataFrame with high variance data - unstable voltage."""
    return pd.DataFrame({
        'voltage_28v_dc1_cal': [15, 30, 18, 28, 16, 29, 17, 27, 20, 25],
        'timestamp': range(10),
        'predicted_cluster': [1] * 10,
        'predicted_status': ['Steady State'] * 10,
        'run_id': ['run1'] * 10
    })


class TestCalculateBasicMetrics:
    """Tests for calculate_basic_metrics method."""
    
    def test_basic_metrics_normal_data(self, analyzer, sample_voltage_data):
        metrics = analyzer.calculate_basic_metrics(sample_voltage_data)
        
        assert metrics['n_points'] == 8
        assert metrics['mean_voltage'] == pytest.approx(24.4, rel=1e-4)
        assert metrics['median_voltage'] == pytest.approx(24.4, rel=1e-4)
        assert 'std' in metrics
        assert 'variance' in metrics
        assert metrics['min_voltage'] == 24.0
        assert metrics['max_voltage'] == 24.8
        assert metrics['range'] == pytest.approx(0.8, rel=1e-4)
    
    def test_basic_metrics_empty_array(self, analyzer):
        metrics = analyzer.calculate_basic_metrics(np.array([]))
        assert metrics == {}
    
    def test_basic_metrics_single_value(self, analyzer):
        metrics = analyzer.calculate_basic_metrics(np.array([24.0]))
        
        assert metrics['n_points'] == 1
        assert metrics['mean_voltage'] == 24.0
        assert metrics['std'] == 0.0
        assert metrics['variance'] == 0.0
        assert metrics['cv'] == 0.0


class TestCalculateSlopeMetrics:
    """Tests for calculate_slope_metrics method."""
    
    def test_slope_with_trend(self, analyzer):
        data = np.array([20.0, 21.0, 22.0, 23.0, 24.0])
        metrics = analyzer.calculate_slope_metrics(data)
        
        assert metrics['slope'] == pytest.approx(1.0, rel=1e-4)
        assert metrics['abs_slope'] == pytest.approx(1.0, rel=1e-4)
        assert metrics['r_squared'] == pytest.approx(1.0, rel=1e-4)
    
    def test_slope_flat_data(self, analyzer):
        data = np.array([24.0, 24.0, 24.0, 24.0])
        metrics = analyzer.calculate_slope_metrics(data)
        
        assert metrics['slope'] == pytest.approx(0.0, abs=1e-10)
        assert metrics['abs_slope'] == pytest.approx(0.0, abs=1e-10)


class TestCheckThresholds:
    """Tests for the combined check_thresholds method."""
    
    def test_dynamic_thresholds_all_pass(self, analyzer):
        metrics = {
            'variance': 0.8,
            'std': 1.2,
            'abs_slope': 0.05,
            'iqr': 0.9
        }
        thresholds = {
            'min_variance': 0.1, 'max_variance': 1.5,
            'min_std': 0.2, 'max_std': 2.0,
            'min_abs_slope': 0, 'max_abs_slope': 0.5,
            'min_iqr': 0.1, 'max_iqr': 1.5
        }
        
        reasons = analyzer.check_thresholds(metrics, thresholds, threshold_type='dynamic')
        assert len(reasons) == 0
    
    def test_dynamic_thresholds_all_four_fail(self, analyzer):
        metrics = {
            'variance': 10.0,
            'std': 8.0,
            'abs_slope': 2.0,
            'iqr': 5.0
        }
        thresholds = {
            'min_variance': 0.1, 'max_variance': 1.5,
            'min_std': 0.2, 'max_std': 2.0,
            'min_abs_slope': 0, 'max_abs_slope': 0.5,
            'min_iqr': 0.1, 'max_iqr': 1.5
        }
        
        reasons = analyzer.check_thresholds(metrics, thresholds, threshold_type='dynamic')
        assert len(reasons) == 4
    
    def test_fixed_thresholds_all_pass(self, analyzer):
        metrics = {
            'variance': 0.8,
            'std': 1.2,
            'abs_slope': 0.05,
            'iqr': 0.5
        }
        
        reasons = analyzer.check_thresholds(metrics, analyzer.steady_state_thresholds, threshold_type='fixed')
        assert len(reasons) == 0
    
    def test_fixed_thresholds_all_fail(self, analyzer):
        metrics = {
            'variance': 10.0,
            'std': 8.0,
            'abs_slope': 2.0,
            'iqr': 5.0
        }
        
        reasons = analyzer.check_thresholds(metrics, analyzer.steady_state_thresholds, threshold_type='fixed')
        assert len(reasons) == 4
    
    def test_three_fail_no_flag(self, analyzer):
        """Only 3 metrics fail - should NOT flag."""
        metrics = {
            'variance': 10.0,  # Fails
            'std': 8.0,        # Fails
            'abs_slope': 2.0,  # Fails
            'iqr': 0.5         # Passes
        }
        
        reasons = analyzer.check_thresholds(metrics, analyzer.steady_state_thresholds, threshold_type='fixed')
        assert len(reasons) == 0  # Should be empty since not all 4 failed


class TestProcessLabelMetrics:
    """Tests for process_label_metrics method."""
    
    def test_process_steady_state(self, analyzer, sample_dataframe):
        grouping = {'ofp': 'test_ofp', 'test_case': 'test1', 'run_id': 'run1'}
        
        metrics = analyzer.process_label_metrics(
            sample_dataframe, 
            'voltage_28v_dc1_cal',
            'Steady State', 
            grouping
        )
        
        assert metrics is not None
        assert metrics['label'] == 'Steady State'
        assert 'flagged' in metrics
        assert 'flags' in metrics
        assert 'flag_reasons' in metrics
    
    def test_process_non_steady_state(self, analyzer, sample_dataframe):
        grouping = {'ofp': 'test_ofp', 'test_case': 'test1', 'run_id': 'run1'}
        df = sample_dataframe.copy()
        df['predicted_status'] = 'Stabilizing'
        
        metrics = analyzer.process_label_metrics(
            df,
            'voltage_28v_dc1_cal',
            'Stabilizing',
            grouping
        )
        
        assert metrics is not None
        assert metrics['label'] == 'Stabilizing'
        assert metrics['flagged'] is False
        assert metrics['flags'] == ''
        assert metrics['flag_reasons'] == ''
    
    def test_process_empty_label(self, analyzer, sample_dataframe):
        grouping = {'ofp': 'test_ofp', 'test_case': 'test1', 'run_id': 'run1'}
        
        metrics = analyzer.process_label_metrics(
            sample_dataframe,
            'voltage_28v_dc1_cal',
            'nonexistent_label',
            grouping
        )
        
        assert metrics is None


class TestRunAnalysis:
    """Tests for run_analysis and related methods."""
    
    @patch('pathlib.Path.mkdir')
    def test_collect_baseline_metrics(self, mock_mkdir, analyzer):
        data = pd.DataFrame({
            'run_id': ['run1', 'run1', 'run2', 'run2'],
            'voltage_28v_dc1_cal': [24.0, 24.1, 24.2, 24.3],
            'timestamp': range(4),
            'predicted_status': ['Steady State'] * 4,
            'predicted_cluster': [1, 1, 2, 2]
        })
        
        with patch.object(analyzer, 'analyze_group', return_value=([], None)):
            metrics = analyzer._collect_baseline_metrics(
                data,
                'voltage_28v_dc1_cal',
                np.array(['run1', 'run2'])
            )
        
        assert isinstance(metrics, list)
    
    def test_calculate_dynamic_thresholds(self, analyzer):
        metrics = [
            {'label': 'Steady State', 'ofp': 'ofp1', 'test_case': 'tc1',
             'variance': 0.5, 'std': 0.7, 'abs_slope': 0.01, 'iqr': 0.3},
            {'label': 'Steady State', 'ofp': 'ofp1', 'test_case': 'tc1',
             'variance': 0.6, 'std': 0.8, 'abs_slope': 0.02, 'iqr': 0.4},
            {'label': 'Steady State', 'ofp': 'ofp1', 'test_case': 'tc1',
             'variance': 0.4, 'std': 0.6, 'abs_slope': 0.015, 'iqr': 0.35}
        ]
        
        thresholds = analyzer._calculate_dynamic_thresholds(metrics)
        
        assert 'ofp1_tc1' in thresholds
        assert 'min_variance' in thresholds['ofp1_tc1']
        assert 'max_variance' in thresholds['ofp1_tc1']
    
    def test_calculate_dynamic_thresholds_insufficient_samples(self, analyzer):
        metrics = [
            {'label': 'Steady State', 'ofp': 'ofp1', 'test_case': 'tc1',
             'variance': 0.5, 'std': 0.7, 'abs_slope': 0.01, 'iqr': 0.3},
            {'label': 'Steady State', 'ofp': 'ofp1', 'test_case': 'tc1',
             'variance': 0.6, 'std': 0.8, 'abs_slope': 0.02, 'iqr': 0.4}
        ]
        
        thresholds = analyzer._calculate_dynamic_thresholds(metrics)
        assert thresholds == {}  # Should be empty with < 3 samples


class TestPlottingFunctions:
    """Tests for plotting functions."""
    
    @patch('matplotlib.pyplot.savefig')
    @patch('matplotlib.pyplot.close')
    def test_create_plot(self, mock_close, mock_savefig, analyzer):
        analyzer.results = [
            {'label': 'Steady State', 'run_id': 'run1', 'voltage_col': 'voltage_28v_dc1_cal',
             'flagged': False, 'mean_voltage': 24.0, 'std': 0.5, 'variance': 0.25, 'abs_slope': 0.01}
        ]
        analyzer.deenergized_max = 10.0
        
        df = pd.DataFrame({
            'timestamp': range(10),
            'voltage_28v_dc1_cal': [24.0] * 10,
            'predicted_status': ['Steady State'] * 10,
            'predicted_cluster': [1] * 10
        })
        
        grouping = {'run_id': 'run1', 'unit_id': 'unit1', 'test_case': 'tc1', 
                   'save': 'save1', 'test_run': 'run1', 'station': 'station1'}
        
        analyzer.create_plot(df, 'voltage_28v_dc1_cal', grouping, Path('test.png'))
        
        assert mock_savefig.called
        assert mock_close.called


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
