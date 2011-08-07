========
PyDBCopy
========

*PyDBCopy* is a python script for copying MySQL database table sets using multi-processing.
Simply pass the source DB info, the target DB info, and the list of tables and *pydbcopy*
will spin up a number of processes (num-cpus minus one) to copy the tables. Read on for
details on how the copy is performed, what sort of validation is done, and ways to optimize.

How tables are copied
---------------------

Tables are copied by using the MySQL *select into outfile* command, then the file is
transfered from the remote machine using *scp*, and finally the file is imported using the
MySQL *load data infile* command.

Validation
----------

Validation is performed before copying by first checking that the source table is not shorter
than the target table by a certain threhold (25% by default).

Optimization
------------

*PyDBCopy* can take advantage of a fieldHash column on your database. If such a column exists
pydbcopy can use this column to copy only the differences to the target table. Warning: if
two tables are more than 40% different then this *incremental copy* algorithm actually results
in a longer copy *PyDBCopy* will detect this and use a full copy instead.
