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

from collections import Counter
from datetime import datetime, UTC as datetime_UTC
# # get time
# sample_time = datetime.now(datetime_UTC)
# # make readable string
# readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')

FAIL_HARD_FILTER_COUNTER = []
"""
Uses cascading layers of matching,
first using two layers of required
or multiple-optional terms,
"""
####################
# Name & List ( , )
####################


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

# TODO append column-term to REQUIRED_TERMS_LIST__ and OPTIONAL_TERMS__ to get the term lists
MATCH_SETS = [
    ("1",""),
    ("2",""),
]


def clean_string(input_text):    
    """Remove all non-alphabetic characters and convert to lowercase."""
    try:
        if isinstance(input_text, str):
            # Remove all non-alphabetic characters (keeping spaces)
            cleaned = re.sub(r'[^a-zA-Z\s]', '', input_text)
            # Convert to lowercase and strip extra whitespace
            return ' '.join(cleaned.lower().split())
        else:
            print("in clean_string() warning, input not string, input returned be default")
            return input_text 
    except Exception as e:
        # debug_log(f"Exception clean_string() {str(e)}")
        return input_text


###################
# hard term filter 
###################
def has_required_terms_boolean(input_string, substrings):
    """
    This function checks if any of the substrings in a list are in a string.
    It stops at the first 'yes' and returns True.

    :param input_string: The main string to check.
    :param substrings: A list of substrings to search for in the input_string.
    :return: True if any substring is found in the input_string, False otherwise.
    """
    try:
        
        input_string = clean_string(input_string)
        
        for substring in substrings:
            
            substring = clean_string(substring)
            
            if substring in input_string:
                print(f"has_required_terms_boolean(), found -> {substring}")
                
                # If found:
                print(substring)
                return True

        # else False
        return False
        
    except Exception as e:
        # debug_log(f"Exception has_required_terms_boolean() {str(e)}")
        return False


def has_n_optional_terms_boolean(
    input_string, 
    substrings, 
    n_terms=2,
):
    """
    This function checks if N of the substrings in a list are in a string.
    It stops at the first 'yes' and returns True.

    :param input_string: The main string to check.
    :param substrings: A list of substrings to search for in the input_string.
    :return: True if any substring is found in the input_string, False otherwise.
    """
    try:
        
        input_string = clean_string(input_string)
        
        n_term_counter = 0
        for substring in substrings:
            substring = clean_string(substring)
            
            if substring in input_string:

                n_term_counter +=1
                print(f"has_n_optional_terms_boolean(), found -> {substring}")
                
                if n_term_counter >= n_terms:
                    # If found:
                    print(substring)
                    return True
                    
        # else False
        return False
        
    except Exception as e:
        # debug_log(f"Exception has_n_optional_terms_boolean() {str(e)}")
        return False


# Example of a specific row processor function
def count_pattern_matches_in_text_boolean(
    required_substring_list, 
    optional_substring_list, 
    input_text,
):
    """
    Counts pattern matches in a text string using global pattern lists.
    
    Args:
        text (str): Text string to analyze
        
    return boolean
    """
    try:
        
        ##############
        # Hard Filter
        ##############
        """
        """
        content_checks = False

        hard_term_check = False
        hard_term_check = has_required_terms_boolean(
            input_text, 
            required_substring_list
        )
        
        optional_term_check = False
        optional_term_check = has_n_optional_terms_boolean(
            input_text, 
            optional_substring_list, 
            n_terms=2
        )
        
        if optional_term_check or hard_term_check:
            content_checks = True       

        if content_checks is True:
        
            return True
            
        else:
            print("failed hard test")
            FAIL_HARD_FILTER_COUNTER.append(1)
            return False

    except Exception as e:
        print(f"Error in pattern matching: {str(e)}")
        return False


def run_sets_of_match_tests(
    match_sets,
    input_text,
):
    """
    Runs multiple sets of pattern matching tests on an input text and returns 
    a dictionary with the results.
    
    This function iterates through each match set (identified by its set name and description),
    fetches the corresponding required and optional term lists, and runs the pattern matching
    test on the input text. Results are collected in a dictionary.
    
    Args:
        match_sets (list): List of tuples, each containing (set_id, set_description)
                          that identify pattern matching rule sets to apply
        input_text (str): Text string to analyze with all pattern matching rule sets
        
    Returns:
        dict: Dictionary with match set IDs as keys and boolean match results as values
              Example: {'1': True, '2': False}
    
    Raises:
        ValueError: If required term lists can't be found for a match set
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
            # Using globals() to access variables dynamically by name
            try:
                required_terms_list = globals().get(required_terms_var_name)
                optional_terms_list = globals().get(optional_terms_var_name)
                
                # Validate that we found the term lists
                if required_terms_list is None:
                    raise ValueError(f"Could not find required terms list: {required_terms_var_name}")
                
                # Optional terms list might be empty, but should not be None
                if optional_terms_list is None:
                    optional_terms_list = []
                    print(f"Warning: No optional terms list found for {set_id}, using empty list")
                
                # Run the pattern matching test for this set
                match_result = count_pattern_matches_in_text_boolean(
                    required_terms_list,
                    optional_terms_list,
                    input_text
                )
                
                # Store the result with the set ID as the key
                results[set_id] = match_result
                
                # Log the result for debugging
                print(f"Match set {set_id} ({set_description}): {match_result}")
                
            except Exception as e:
                # Log the error but continue processing other match sets
                print(f"Error processing match set {set_id}: {str(e)}")
                results[set_id] = False  # Default to False on error
        
        return results
        
    except Exception as e:
        # Handle unexpected errors in the overall function
        print(f"Error in run_sets_of_match_tests: {str(e)}")
        return {}  # Return empty dict on error


#########
# test 1
#########
required_substring_list = REQUIRED_TERMS_LIST__1
optional_substring_list = OPTIONAL_TERMS__1

input_text = "cats and dogs"
input_text = "pets and animal animal rights"

match_result = count_pattern_matches_in_text_boolean(
    required_substring_list, 
    optional_substring_list, 
    input_text,
)

print(match_result)


#########
# Test 2
#########
# Define your match sets
MATCH_SETS = [
    ("1", "Cat-related content"),
    ("2", "Breakfast-related content"),
]

# Test with different input texts
test_text_1 = "My cat loves to play with yarn"
test_text_2 = "I had eggs and toast for breakfast with orange juice" 

# Run the tests
results_1 = run_sets_of_match_tests(MATCH_SETS, test_text_1)
results_2 = run_sets_of_match_tests(MATCH_SETS, test_text_2)

print(f"Results for text 1: {results_1}")
print(f"Results for text 2: {results_2}")
