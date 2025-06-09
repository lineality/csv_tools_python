# csv_tools_python
Basic general utilities for .csv in python


Roadmap:
1. proof of concepts (POC)
2. production save Rust versions
3. challenges: balancing parellel processing of rows with 
(tool to add index row?)
Is there a reliable way to ~dynamically (not with an index row)
track what the index rows are?
(or is it easier to add an index row?)

if you are sending chunks/batches of rows to be processed
in paraellel, why not track and send what slice of rows
those are, and be able to associate with each row
which row-index that is,
e.g. if you find X in the row,
export either the field or row and the row index
or alternately just the row index.

saving results to a file

maybe or maybe not parallel

modular steps

stack of filters/queries/searches/selections

search cell extractor...
1. any
2. parallel
"""
iterates through all rows,
if all matches/filters/searches/sorts/queries are true
saves file-name and row-index with one or more fields to a 
directory of results:
A. as results csv (machine readable)
B. as appended text file
C. as directory of max-items per txt file (human readable)

other way around...
row id and saving TO the working filter functions...

A. make filter module...

"""


look at SQL type syntax wrapper...
