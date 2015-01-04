pst
===

pst is a command line tool for processing and combining columns across column oriented files.

usage
-----

    usage: pst <options> file1 file2 ...

    options:
      -c=false: compute statistics across column values in each output row.
         Please note that each value in the output has to be convertible into a float
         for this to work. Currently the mean and standard deviation are computed.
      -e="": specify the input columns to extract.
         The spec format is "<column list file1>|<column list file2>|..."
         where each column specifier is of the form col_i,col_j,col_k-col_n, ....
         If the number of specifiers is less than the number of files, the last
         specifier i will be applied to files i through N, where N is the total
         number of files provided.
      -i="": column separator for input files. The default separator is whitespace.
      -o=" ": column separator for output files. The default separator is a single space.

Notes
------

    The output file is assembled in memory and thus requires sufficient storage
    to hold the complete final output data.

    The input column specifiers are zero based and can include ranges. The end
    of a range is not included in the output, i.e. the range 2-5 selects columns
    2, 3, and 4.

Examples
---------

    pst -e "0,1" file1 file2 file3 > outfile

    This command selects columns 0 and 1 from each of file1, file2, and file3
   	and outputs them to outfile (which thus contains 6 columns).


    pst -e "0,1|3" file1 file2 file3 > outfile

    This invocation selects columns 0 and 1 from file1, and column 3 from file2
    and file3. outfile contains 4 columns.


    pst -e "0,1|3|4-6" file1 file2 file3 > outfile

    This command selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns.


    pst -o "," -i ";" -e "0,1|3|4-6" file1 file2 file3 > outfile

    This command splits the input files into columns with ';' as
    separator. It selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns each separated
    by ','.


    pst -c -o "," -i ";" -e "0,1|3|4-6" file1 file2 file3 > outfile

    Same as above but instead of outputting 5 columns, it computes and prints
    for each row the mean and variance across each 5 columns. Please note that
    this assumes that each column entry can be converted into a float value.
