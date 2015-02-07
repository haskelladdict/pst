pst
===

pst is a command line tool for processing and combining columns across column oriented 
files. pst can also compute row-wise statistics (such as mean, median, variance, etc).

usage
-----

    usage: pst <options> file1 file2 ...

    options:
      -c="": compute statistics across column values in each output row.
        Please note that each value in the output has to be convertible into a float
        for this to work. The computed statistics are determined by a comma separated
        list of actions. The result of each action is printed as a separate column value.
        Currently supported compute actions are:
            - mean  : compute row mean
            - std   : compute row standard deviation
            - var   : compute row variance
            - median: compute row median
            - max   : compute maximum value of row
            - min   : compute minimum value of row
        Thus, "mean, std, median" will result in three columns per row, with the
        mean, standard deviation and median of the raw column values.
      -h=false: show basic usage info
      -i="": specify the input columns to extract. This flag is optional.
        The spec format is "<column list file1>|<column list file2>|..."
        where each column specifier is of the form col_i,col_j,col_k-col_n, ....
        If the number of specifiers is less than the number of files, the last
        specifier i will be applied to files i through N, where N is the total
        number of files provided. If this flag is not provided all input columns
        will be extracted.
      -n=1: number of threads (default: 1)
      -o="": specify the order in which to print the output columns. This flag is optional.
        The spec format is "i,j,k-l,m,..", where 0 < i,j,k,l,m, ... < numCol, and
        numCol is the total number of columns extracted from the input files.
        Columns can be specified multiple times and ranges are accepted. If this
        option is not provided the columns are pasted in the order in which they
        are extracted.
      -r="": specify which rows to process and output. This flag is optional.
        If not specified all rows will be output. Rows can be specified by a comma
        separated list of row IDs or row ID ranges. E.g., "1,2,4-8,22" will process
        rows 1, 2, 4, 5, 7, 22.
      -s="": column separator for input files. The default separator is whitespace.
      -t=" ": column separator for output files. The default separator is a single space.

Notes
------

    The output file is assembled in memory and thus requires sufficient storage
    to hold the complete final output data.

    The input column specifiers are zero based and can include ranges. The end
    of a range is not included in the output, i.e. the range 2-5 selects columns
    2, 3, and 4.

Examples
---------

    pst -i "0,1" file1 file2 file3 > outfile

    This command selects columns 0 and 1 from each of file1, file2, and file3
     and outputs them to outfile (which thus contains 6 columns).


    pst -i "0,1|3" file1 file2 file3 > outfile

    This invocation selects columns 0 and 1 from file1, and column 3 from file2
    and file3. outfile contains 4 columns.


    pst -i "0,1|3|4-5" file1 file2 file3 > outfile

    This command selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns.


    pst -t "," -s ";" -i "0,1|3|4-5" file1 file2 file3 > outfile

    This command splits the input files into columns with ';' as
    separator. It selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns each separated
    by ','.


    pst -c -t "," -s ";" -i "0,1|3|4-5" file1 file2 file3 > outfile

    Same as above but instead of outputting 5 columns, it computes and prints
    for each row the mean and variance across each 5 columns. Please note that
    this assumes that each column entry can be converted into a float value.
