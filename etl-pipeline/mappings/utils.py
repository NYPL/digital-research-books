import re


def clean_formatted_string(formatted_string):
    # Remove extra spaces from combining multiple subfields
    str_no_dupe_spaces = re.sub(r"\s{2,}", " ", formatted_string)

    # Remove spaces at ends of strings, including extraneous punctuation
    # The negative lookbehind preserves punctuation with initialisms
    return re.sub(r"((?<![A-Z]{1}))[ .,;:]+(\||$)", r"\1\2", str_no_dupe_spaces)
