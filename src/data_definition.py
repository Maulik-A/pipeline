import pandas as pd
from typing import Dict, Any


class F1TelemetryValidator:
    """
    Validates CSV files against F1 telemetry data schema.
    """

    def __init__(self):
        self.required_columns = [
            'timeUtc', 'driverNumber', 'rpm', 'speed', 'gear', 'throttle', 'brake', 'drs'
        ]

        self.column_types = {
            'timeUtc': 'datetime64[ns]',
            'driverNumber': 'int64',
            'rpm': 'int64',
            'speed': 'int64',
            'gear': 'int64',
            'throttle': 'int64',
            'brake': 'int64',
            'drs': 'int64'
        }
        
        # Intitial observation. Noticed some data not in the range
        self.validation_rules = {
            'driverNumber': {'min': 1, 'max': 99},
            'rpm': {'min': 0, 'max': 20000},
            'speed': {'min': 0, 'max': 400},
            'gear': {'min': 0, 'max': 8},
            'throttle': {'min': 0, 'max': 110},
            'brake': {'min': 0, 'max': 1},
            'drs': {'min': 0, 'max': 20}
        }

    def validate_csv_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validates DataFrame content against schema and rules.
        """
        validation_result = {
            "is_valid": True,
            "errors": []
        }

        # Drop extra columns
        extra_columns = [col for col in df.columns if col not in self.required_columns]
        if extra_columns:
            df.drop(columns=extra_columns, inplace=True)

        # Add missing columns with NaNs
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        for col in missing_columns:
            df[col] = pd.NA
            validation_result["errors"].append(f"Missing column '{col}' added with nulls.")
            validation_result["is_valid"] = False

        # Reorder columns to match required schema
        df = df[self.required_columns]

        # Convert timeUtc to datetime
        try:
            df['timeUtc'] = pd.to_datetime(df['timeUtc'], errors='coerce')
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"timeUtc column cannot be updated to datetime: {e}")
            return validation_result

        # Check for datetime parsing failures
        if df['timeUtc'].isnull().any():
            validation_result["is_valid"] = False
            validation_result["errors"].append("Some timeUtc values could not be changed to datetime.")

        # Fixing column types. 
        for column, expected_dtype in self.column_types.items():
            if column != 'timeUtc':
                try:
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(expected_dtype)
                except Exception as e:
                    validation_result["is_valid"] = False
                    validation_result["errors"].append(f"Failed to cast '{column}' to {expected_dtype}: {e}")

        # Check for missing values
        if df.isnull().any().any():
            validation_result["is_valid"] = False
            validation_result["errors"].append("Missing or invalid values found in the data.")

        # Check value ranges
        for column, rules in self.validation_rules.items():
            if column in df.columns:
                # Should check if the data can be varied over time
                if not df[column].dropna().between(rules['min'], rules['max']).all():
                    validation_result["is_valid"] = False
                    validation_result["errors"].append(
                        f"Values out of range in column '{column}'. Expected between {rules['min']} and {rules['max']}."
                    )

        return validation_result
