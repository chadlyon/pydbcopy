#!/usr/bin/python

"""
  Copies a MySQL database from one host to another.
"""
import re
import tempfile
import os
import MySQLdb as Database
import multiprocessing

logger = multiprocessing.get_logger()
        
class MySQLHost(object):

    def __init__(self, host=None, user=None, password=None, database=None):
        """
            Connects to a database if all keyword arguments are given.

            Keyword arguments:
                host -- host to connect to MySQL server on
                user -- username to connect with
                password -- password to connect with
        """
        self.conn = None
        self.user = None
        self.password = None
        
        # empty passwords allowed
        if host and user and password is not None:
            self.connect(host, user, password, database)

    def connect(self, host, user, password, database):
        """
            Set up this object's connection to a MySQL server.

            Keyword arguments:
                host -- host to connect to MySQL server on
                user -- username to connect with
                password -- password to connect with
        """
        logger.debug("Connecting as %s to %s (%s)..." % (user, host, database))
        self.conn = Database.connect(host=host, user=user, passwd=password, db=database)
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def __del__(self):
        """
            Closes connection to MySQL server when done.
        """
        if self.conn:
            logger.debug("Closing DB connection to %s." % self.host)
            self.conn.close()

    #
    # Check whether a table with a given name exists on in this database on this host.
    #        
    # Keyword arguments:
    #   table -- case insensitive name of table to look for on this host
    #
    # Returns: 
    #   True if table exists, False otherwise
    def table_exists(self, table):
        """
            Checks for existence of table in the configured DB on the configured host.

            Keyword arguments:
                table -- the table to check for existence
                
            returns -- True if the table exists, false otherwise
        """
        found = False

        logger.debug("Checking if %s exists on host..." % (table))

        cursor = self.conn.cursor()
        cursor.execute('SHOW TABLES LIKE %s;', table)
        if cursor.fetchone():
            found = True
        cursor.close()

        return found

    def select_into_outfile(self, table, hash_set, dump_dir):
        """ 
            Use select into outfile to dump a database table into a CSV file.
            
            Keyword arguments:
               table -- name of the table to dump
               hash_set -- a set of hashes to dump (from the fieldHash column), 
                           if None all records are selected
               dump_dir -- the file system path to the dir to dump the file to 
                           (attempts to make th dir if not exists).
            
            returns -- a string containing the full path to the file
        """
        c = self.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
            
        logger.debug('Dumping %s.%s to CSV file with select into outfile...' % (self.database, table))

        csv_file = tempfile.NamedTemporaryFile(dir=dump_dir)
            
        csvfilename = csv_file.name
        logger.debug("Temp file %s created." % csvfilename)
        
        csv_file.delete = True
        csv_file.close()

        if hash_set is not None:
            ''' Then we must select into a temp table first
                Before we drop suppress warnings 
            '''
            from warnings import filterwarnings
            filterwarnings( 'ignore', category = Warning )

            c.execute("drop table if exists %s.tmp_pydbcopy_%s" % (self.database, table))

            from warnings import resetwarnings
            resetwarnings()
            
            c.execute("create table %s.tmp_pydbcopy_%s like %s.%s" % (self.database, table, self.database, table))
            
            try:
                batch_size = 20000
                batch = set()
                batches_complete = False
                while not batches_complete:
                    batch.add(hash_set.pop())
                    batches_complete = len(hash_set) == 0
                    
                    if len(batch) == batch_size or batches_complete:
                        query = "insert into %s.tmp_pydbcopy_%s (select * from %s.%s where fieldHash in ('%s'))" % \
                                (self.database, table, self.database, table, "','".join(str(hash) for hash in batch))
                        logger.debug(query);
                        c.execute(query)
                        batch.clear()
    
                logger.debug("Executing select into outfile command...")
                c.execute("select * from %s.tmp_pydbcopy_%s into outfile '%s'" % (self.database, table, csvfilename))
                
            finally:
                logger.debug("Cleaning up temp table...")
                c.execute("drop table %s.tmp_pydbcopy_%s" % (self.database, table))
        else:
            logger.debug("Executing select into outfile command...")
            c.execute("select * from %s.%s into outfile '%s'" % (self.database, table, csvfilename))
        
        c.close()
    
        return csvfilename
    
    def load_data_in_file(self, table, filename):
        """
            Load the specified file into the specified table using the LOAD DATA INFILE SQL statement.

            Keyword arguments:
                table -- Table to load the data into.
                filename -- The full path to the CSV file to be loaded
        """
        logger.debug("Loading %s into %s.%s.%s..." % (filename, self.host, self.database, table))
        
        file = open(filename, 'r')
        file.seek(0)
        max_lines = 5
        i = 0
        for line in file:
            if i < max_lines:
                logger.debug(re.sub(r'\n', '', line))
                i += 1
            else:
                break

        # get the size of the file in human friendly format
        bytes = os.stat(file.name).st_size
        if bytes < 1024:
            file_size = "%f bytes" % bytes
        elif bytes < 1048576: # 1024^2
            file_size = "%.2f Kb" % (bytes / 1024.0)
        else:
            file_size = "%.2f Mb" % (bytes / 1048576.0)

        logger.debug("----------------------------------------------------")
        logger.debug("The first %s lines of the CSV to be loaded are shown above" % max_lines)
        logger.debug("loading %s CSV file into %s.%s on %s..." % (file_size, self.database, table, self.host))

        c = self.conn.cursor()
        c.execute("load data local infile '%s' into table %s.%s" % (filename, self.database, table))
        self.conn.commit()
        c.close()

    def truncate_table(self, table):
        """ 
            Deletes all data in the specified table using the TRUNCATE TABLE SQL statement.
            
            Keyword arguments:
               table -- name of the table to truncate
        """
        c = self.conn.cursor()
        c.execute("truncate table %s.%s" % (self.database, table))
        self.conn.commit()
        c.close()
        
    def get_table_structure(self, table):
        """ 
            Get the table schema as reported by the SHOW CREATE TABLE SQL statement. This routine strips
            the AUTO_INCREMENT part of the table schema out.
            
            Keyword arguments:
               table -- name of the table to get the schema for
               
            returns -- a string representing the table schema, suitable SQL to be used to 
                       recreate the schema for this table
        """
        c = self.conn.cursor()
        logger.debug('Determining the structure of %s.%s on %s' % (self.database, table, self.host))
        c.execute("show create table %s" % (table))
        rows = c.fetchone()
        struct = re.sub('AUTO\_INCREMENT=\d+ ', '', rows[1])
        c.close()
        
        return struct
    
    def get_table_max_modified(self, table):
        """ 
            Get the maximum value of the lastModifiedDate column for this table if it exists.
            
            Keyword arguments:
               table -- name of the table to get the max lastModifiedDate for.
               
            returns -- a date representing the last time a record was modified in the table
                       or -1 if anything goes (ie if this table has no lastModifedDate column)
        """
        try:
            c = self.conn.cursor()
            logger.debug('Determining the max lastModified of %s.%s on %s' % (self.database, table, self.host))
            c.execute("select max(lastModifiedDate) from %s" % (table))
            rows = c.fetchone()
            c.close()
            return rows[0]
        except:
            return - 1
    
    def create_table_with_schema(self, table, schema):
        """ 
            Drops and then recreates the specified table with the specified schema.
            
            Keyword arguments:
               table -- name of the table to drop and recreate the schema for
               schema -- SQL statement to use to create the schema
        """
        c = self.conn.cursor()
        logger.debug('Initializing the structure of %s.%s on %s' % (self.database, table, self.host))
        c.execute("drop table if exists %s" % (table))
        c.execute(schema)
        self.conn.commit()
        c.close()
    
    def get_current_hash_set(self, table):
        """ 
            Gets the current set of values for the fieldHash column in the table
            
            Keyword arguments:
               table -- name of the table from which to get the set of values of fieldHash
               
            returns -- a set containing all values of the fieldHash column in this table
        """
        c = self.conn.cursor()
        logger.debug('Fetching the field hash set for %s' % table)
        c.execute("select fieldHash from %s" % (table))
        rows = c.fetchall()
        
        hashSet = set()
        for row_data in rows:
            hash = row_data[0]
            hashSet.add(hash)
            
        c.close()
        return hashSet
    
    def delete_records(self, table, hashSet):
        """ 
            Delete records from the specified table that have fieldHash in the specified set of values in hashSet.
            The delete is done in batches of 20,000 so as to not exceed the max_allowed_packet_size on a MySQL TCP/IP 
            connection. If hashSet is None then nothing is deleted.
            
            Keyword arguments:
               table -- name of the table from which to delete the set of values of fieldHash
               hashSet -- the set of hash values to find in the fieldHash column in the table to delete
        """
        if hashSet is None or len(hashSet) == 0:
            return
        
        c = self.conn.cursor()
        c.execute("alter table %s disable keys" % table)       
        
        batch_size = 20000
        batch = set()
        batches_complete = False
        while not batches_complete:
            batch.add(hashSet.pop())
            batches_complete = len(hashSet) == 0
            
            if len(batch) == batch_size or batches_complete:
                query = "delete from %s where fieldHash in ('%s')" % (table, "','".join(str(hash) for hash in batch))
                logger.debug(query);
                c.execute(query)
                batch.clear()
        
        c.execute("alter table %s enable keys" % table)    
        self.conn.commit()
        c.close()
    
    def get_row_count(self, table):
        """ 
            Gets the number of rows in the specified table
            
            Keyword arguments:
               table -- name of the table from which to get the row count
               
            returns -- a count of the number of rows in the table, None if table
                       does not exist.
        """
        query = "select count(*) from %s" % (table)
        count = self.__execute_count_query(query)
        if count is None:
            logger.debug("table %s.%s does not exist on host %s" % (self.database, table, self.host))
        else:
            logger.debug("row count for %s.%s in %s is %d" % (self.database, table, self.host, count)) 
        return count
    
    def __execute_count_query(self, query):
        count = None
        try :
            c = self.conn.cursor()
            c.execute(query)
            rows = c.fetchone()
            count = rows[0]
            c.close()
        except:
            pass
        return count
