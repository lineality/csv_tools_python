"""
String Pattern Matching Module

This module provides functionality for matching various patterns in text data,
with support for required terms and optional terms using a cascading filtering approach.
Results can be added to pandas DataFrames safely after parallel processing.
"""

import csv
import argparse
import os
import re
import logging
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from collections import Counter
from datetime import datetime, UTC as datetime_UTC
from typing import List, Dict, Tuple, Any, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define pattern matching rule sets
REQUIRED_TERMS_LIST__1 = [
    "cat",
]
OPTIONAL_TERMS__1 = [
    "pets",
    "animal rights",
]

REQUIRED_TERMS_LIST__2 = [
    "eggs",
]
OPTIONAL_TERMS__2 = [
    "toast",
    "oj",
]

# Match set definitions with descriptions
MATCH_SETS = [
    ("1", "Cat-related content"),
    ("2", "Breakfast-related content"),
]


def clean_string(input_text: Any) -> str:
    """
    Remove all non-alphabetic characters and convert to lowercase.
    
    Args:
        input_text: The text to clean, expected to be a string
        
    Returns:
        str: The cleaned text as lowercase with only alphabetic characters and spaces
    """
    try:
        if not isinstance(input_text, str):
            logger.warning(f"Expected string input but got {type(input_text)}. Returning as is.")
            return str(input_text)
            
        # Remove all non-alphabetic characters (keeping spaces)
        cleaned = re.sub(r'[^a-zA-Z\s]', '', input_text)
        # Convert to lowercase and strip extra whitespace
        return ' '.join(cleaned.lower().split())
    except Exception as e:
        logger.error(f"Error cleaning string: {str(e)}")
        return str(input_text)


def has_required_terms_boolean(input_string: str, substrings: List[str]) -> bool:
    """
    Check if any required terms are present in the input string.
    
    Args:
        input_string: The text to search within
        substrings: List of terms to search for
        
    Returns:
        bool: True if any substring is found in the input string
    """
    try:
        input_string = clean_string(input_string)
        
        for substring in substrings:
            substring = clean_string(substring)
            
            if substring in input_string:
                logger.debug(f"Required term found: {substring}")
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"Error checking required terms: {str(e)}")
        return False


def has_n_optional_terms_boolean(
    input_string: str, 
    substrings: List[str], 
    n_terms: int = 2
) -> bool:
    """
    Check if at least N optional terms are present in the input string.
    
    Args:
        input_string: The text to search within
        substrings: List of terms to search for
        n_terms: Minimum number of terms that must be found
        
    Returns:
        bool: True if at least n_terms substrings are found in the input string
    """
    try:
        input_string = clean_string(input_string)
        
        n_term_counter = 0
        for substring in substrings:
            substring = clean_string(substring)
            
            if substring in input_string:
                n_term_counter += 1
                logger.debug(f"Optional term found: {substring}")
                
                if n_term_counter >= n_terms:
                    return True
                    
        return False
        
    except Exception as e:
        logger.error(f"Error checking optional terms: {str(e)}")
        return False


def count_pattern_matches_in_text_boolean(
    required_substring_list: List[str], 
    optional_substring_list: List[str], 
    input_text: str
) -> bool:
    """
    Check if text matches pattern criteria based on required and optional terms.
    
    Args:
        required_substring_list: List of required terms, any of which must be present
        optional_substring_list: List of optional terms, of which at least 2 must be present
        input_text: The text to analyze
        
    Returns:
        bool: True if the text matches the pattern criteria
    """
    try:
        # Track failures locally rather than with a global counter
        failed_hard_filter = False
        
        # Check for required terms
        hard_term_check = has_required_terms_boolean(
            input_text, 
            required_substring_list
        )
        
        # Check for optional terms
        optional_term_check = has_n_optional_terms_boolean(
            input_text, 
            optional_substring_list, 
            n_terms=2
        )
        
        # Content passes if either required terms are found or enough optional terms are found
        content_checks = hard_term_check or optional_term_check

        if not content_checks:
            logger.debug("Text failed pattern matching filters")
            failed_hard_filter = True
            
        return content_checks

    except Exception as e:
        logger.error(f"Error in pattern matching: {str(e)}")
        return False


def run_sets_of_match_tests(
    match_sets: List[Tuple[str, str]],
    input_text: str
) -> Dict[str, bool]:
    """
    Run multiple sets of pattern matching tests on an input text.
    
    Args:
        match_sets: List of tuples (set_id, set_description) defining rule sets to apply
        input_text: Text to analyze with all pattern matching rule sets
        
    Returns:
        dict: Dictionary with match set IDs as keys and boolean match results as values
    """
    try:
        # Initialize results dictionary
        results = {}
        
        # Process each match set
        for set_id, set_description in match_sets:
            # Construct the variable names for the required and optional term lists
            required_terms_var_name = f"REQUIRED_TERMS_LIST__{set_id}"
            optional_terms_var_name = f"OPTIONAL_TERMS__{set_id}"
            
            # Get the lists from the global namespace
            try:
                required_terms_list = globals().get(required_terms_var_name)
                optional_terms_list = globals().get(optional_terms_var_name)
                
                # Validate that we found the term lists
                if required_terms_list is None:
                    raise ValueError(f"Could not find required terms list: {required_terms_var_name}")
                
                # Optional terms list might be empty, but should not be None
                if optional_terms_list is None:
                    optional_terms_list = []
                    logger.warning(f"No optional terms list found for {set_id}, using empty list")
                
                # Run the pattern matching test for this set
                match_result = count_pattern_matches_in_text_boolean(
                    required_terms_list,
                    optional_terms_list,
                    input_text
                )
                
                # Store the result with the set ID as the key
                results[set_id] = match_result
                
                logger.debug(f"Match set {set_id} ({set_description}): {match_result}")
                
            except Exception as e:
                # Log the error but continue processing other match sets
                logger.error(f"Error processing match set {set_id}: {str(e)}")
                results[set_id] = False  # Default to False on error
        
        return results
        
    except Exception as e:
        logger.error(f"Error in run_sets_of_match_tests: {str(e)}")
        return {}  # Return empty dict on error


def process_batch(
    df_batch: pd.DataFrame, 
    text_column: str, 
    match_sets: List[Tuple[str, str]]
) -> pd.DataFrame:
    """
    Process a batch of DataFrame rows, adding pattern match results as new columns.
    
    Args:
        df_batch: Pandas DataFrame batch to process
        text_column: Column name containing text to analyze
        match_sets: List of match sets to apply
        
    Returns:
        pd.DataFrame: DataFrame with new match result columns added
    """
    try:
        # Create a copy to avoid modifying the original
        result_df = df_batch.copy()
        
        # Process each row
        for idx, row in result_df.iterrows():
            text_to_analyze = row[text_column]
            match_results = run_sets_of_match_tests(match_sets, text_to_analyze)
            
            # Add results as new columns
            for set_id, result in match_results.items():
                result_df.at[idx, f'match_set_{set_id}'] = result
                
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return df_batch  # Return original on error


def add_pattern_match_columns(
    df: pd.DataFrame, 
    text_column: str, 
    match_sets: List[Tuple[str, str]],
    n_workers: int = 4
) -> pd.DataFrame:
    """
    Add pattern match result columns to a DataFrame in a parallel-safe manner.
    
    Args:
        df: Input DataFrame
        text_column: Column containing text to analyze
        match_sets: Match sets to apply
        n_workers: Number of parallel workers
        
    Returns:
        pd.DataFrame: DataFrame with added match result columns
    """
    try:
        # Validate inputs
        if text_column not in df.columns:
            raise ValueError(f"Text column '{text_column}' not found in DataFrame")
            
        # Split DataFrame into batches for parallel processing
        df_splits = np.array_split(df, n_workers * 2)
        
        # Create a partial function with fixed parameters
        process_func = partial(process_batch, text_column=text_column, match_sets=match_sets)
        
        # Process batches in parallel
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            results = list(executor.map(process_func, df_splits))
            
        # Combine results
        result_df = pd.concat(results, ignore_index=False)
        
        # Ensure the result has the same index as the input
        return result_df
    
    except Exception as e:
        logger.error(f"Error in parallel processing: {str(e)}")
        # Fallback to serial processing
        logger.info("Falling back to serial processing")
        return process_batch(df, text_column, match_sets)


# Example usage for pandas integration
def example_pandas_usage():
    """
    Example of how to use this module with pandas DataFrames.
    """
    # Sample data
    data = {
        'id': [1, 2, 3, 4],
        'text': [
            "My cat loves to play with yarn",
            "I had eggs and toast for breakfast with orange juice",
            "Animal rights advocates and pet owners agree on many issues",
            "No matching content here"
        ]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add pattern match columns
    result_df = add_pattern_match_columns(
        df=df,
        text_column='text',
        match_sets=MATCH_SETS,
        n_workers=2
    )
    
    print("Original DataFrame:")
    print(df)
    print("\nResulting DataFrame with match columns:")
    print(result_df)


if __name__ == "__main__":
    # Your test code here
    import numpy as np  # For DataFrame splitting
    example_pandas_usage()
