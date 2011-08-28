from optparse import OptionParser
from config import settings
from dbutils import MySQLHost
import re
import sys
import os
import stat
import multiprocessing
import logging

logger = multiprocessing.get_logger()

def main(argv=None):
    """
        This is the main routine for pydbcopy. Pydbcopy copies a set of tables from one 
        database (possibly remote) to a local database. If possible an incremental copy is 
        performed (see the perform_incremental_copy routine below). If an 
        incremental copy cannot be performed a full copy is performed (see the perform_full_copy
        routine below).
        
        This routine reads settings from a prop file and then overrides any properties therein
        with command line args. See the usage output for a description of the arguments. 
        
        This program is multi-process by default but can be switched into single process mode
        for debugging (see the --debug option in the usage). Pydbcopy will use a number of processes 
        equal to the number of cpus detected on the system minus one.
    """
    if argv is None:
        argv = sys.argv
        
    parser = get_option_parser()
    options = parser.parse_args(argv[1:])[0]
    
    if options.properties:
        settings.read_properties(options.properties)

    """ Command Line options override properties from the prop file """
    if options.source_host is not None: settings.source_host = options.source_host 
    if options.source_user is not None: settings.source_user = options.source_user 
    if options.source_password is not None: settings.source_password = options.source_password 
    if options.source_database is not None: settings.source_database = options.source_database 
    if options.target_host is not None: settings.target_host = options.target_host 
    if options.target_user is not None: settings.target_user = options.target_user 
    if options.target_password is not None: settings.target_password = options.target_password 
    if options.target_database is not None: settings.target_database = options.target_database 
    if options.scp_user is not None: settings.scp_user = options.scp_user 
    if options.dump_dir is not None: settings.dump_dir = options.dump_dir 
    if options.verify_threshold is not None: settings.verify_threshold = options.verify_threshold 
    if options.force_full is not None: settings.force_full = options.force_full 
    if options.no_last_mod_check is not None: settings.no_last_mod_check = options.no_last_mod_check 
    if options.debug is not None: settings.debug = options.debug 

    if options.tables is not None: settings.tables = options.tables.split()
    if options.tables_to_skip_verification is not None: settings.tables_to_skip_verification = options.tables_to_skip_verification.split()

    if options.num_processes is not None and options.num_processes != '':
        settings.num_processes = int(options.num_processes)
        
    if settings.num_processes == 0:
        settings.num_processes = multiprocessing.cpu_count() - 1
            
    settings.verbosity = 0
    if options.verbose is not None and options.verbose is True:
        settings.verbosity = 1
        
    # check that we have a list of tables to copy
    if len(settings.tables) == 0:
        sys.stderr.write("Error: No tables specified.\n")
        return 1
        
    # check that user specified dump dir is readable/writable by all
    if not os.path.isdir(settings.dump_dir):
        os.makedirs(settings.dump_dir)
    
    if os.stat(settings.dump_dir)[stat.ST_MODE] != 0777:
        try:
            os.chmod(settings.dump_dir, 0777)
        except OSError:
            sys.stderr.write("Warning: unable to chmod 777 on '%s'. Pydbcopy may not be able to clean up after itself!\n" \
                             % settings.dump_dir)
            pass
        
    if not os.access(settings.dump_dir, os.F_OK | os.R_OK | os.W_OK):
        sys.stderr.write("Error: unable to find or create a writable dump dir at '%s'\n" \
                         % settings.dump_dir)
        return 1
    
    # Configure the logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if settings.verbosity else logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(processName)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG if settings.verbosity else logging.INFO)
        
    if not settings.debug and settings.num_processes > 1:
        pool = multiprocessing.Pool(settings.num_processes)
        result_list = pool.map(verify_and_copy_table, settings.tables, 1)
    else:
        result_list = map(verify_and_copy_table, settings.tables)
    
    failed_tables = set()
    invalid_tables = set()
    skipped_tables = set()
    copied_tables = set()
    for result, table in map(None, result_list, settings.tables):
        if result == 1:
            skipped_tables.add(table)
        elif result == -1:
            invalid_tables.add(table)
        elif result < -1:
            failed_tables.add(table)
        else:
            copied_tables.add(table)
    
    logger.info('Summary for copy from source database %s on %s to target database %s on %s:' % \
                (settings.source_database, settings.source_host, settings.target_database, settings.target_host))
    logger.info('--------------------------------------')
    if len(skipped_tables) > 0:
        logger.info(' Skipped: %s' % ', '.join(skipped_tables))
    if len(copied_tables) > 0:
        logger.info('  Copied: %s' % ', '.join(copied_tables))
    if len(invalid_tables) > 0:
        logger.error('Invalid: %s' % ', '.join(invalid_tables))
    if len(failed_tables) > 0:
        logger.error(' Failed: %s' % ', '.join(failed_tables))
    logger.info('--------------------------------------')
    
    if len(invalid_tables) > 0 or len(failed_tables) > 0:
        return -1
    
    return 0

def verify_and_copy_table(table):
    """
        This routine verifies the specified table's row count on the source is within a certain 
        configurable threshold and if it is copies it to the local database. If the tables schema
        on the source and target are equal and the source has not been moddified more recently
        than the target then the copy is skipped. An incremental copy is attempted and if failed 
        then a full copy is attempted.
        
        This is the main routine that the set of tables is mapped through by the multi-processing 
        pool. This routine returns the following codes:
        
         0 = successful copy
         1 = skipped copying table due to no change detected.
        -1 = failed validity check, source row count is too different than target
        
        Any other return value is an unknown failure.
    """
    logging.getLogger('PyDBCopy')
    
    # set up DB connections to both source and target DB
    source_host = MySQLHost(settings.source_host, settings.source_user, \
                            settings.source_password, settings.source_database)
    dest_host = MySQLHost(settings.target_host, settings.target_user, \
                          settings.target_password, settings.target_database)
    
    if table not in settings.tables_to_skip_verification:
        if not perform_validity_check(table, source_host, dest_host, settings.verify_threshold):
            return - 1
    if settings.no_last_mod_check \
       or not dest_host.table_exists(table) \
       or not schema_compare(table, source_host, dest_host, True) \
       or not is_last_mod_same(table, source_host, dest_host):
        
        copied = False
        try:
            if not settings.force_full:
                logger.info("Starting incremental copy of table %s from %s(%s) to %s(%s)" % \
                       (table, source_host.database, source_host.host, dest_host.database, dest_host.host))
                copied = perform_incremental_copy(table, source_host, dest_host, settings.scp_user, settings.dump_dir)
                if copied:
                    logger.info("Successful incremental copy of table %s" % table)
                else:
                    logger.warn("Failed incremental copy of table %s" % table)
                    
            if not copied:
                logger.info("Starting full copy of table %s from %s(%s) to %s(%s)" % \
                       (table, source_host.database, source_host.host, dest_host.database, dest_host.host))
                copied = perform_full_copy(table, source_host, dest_host, settings.scp_user, settings.dump_dir)
                if copied:
                    logger.info("Successful full copy of table %s" % table)
                else:
                    logger.error("Failed full copy of table %s" % table)
        except:
            logger.error("Failed copy of table %s", table, exc_info=1)
                
        if not copied:
            return - 3
    else:
        logger.info("Skipping copying of table %s (source/dest have same row count and last mod date)" % table)
        return 1
    return 0

def perform_incremental_copy(table, source_host, dest_host, scp_user, dump_dir):
    """
        Performs an incremental copy of the specified table from source to destination by using a 
        fieldHash column on the source and target that is to be a hash of all data in the row. The 
        incremental copy copies only the difference of rows on the target from the source and deletes 
        the difference of rows on the source from the target. The deletes are done in batches of 20,000 
        so as to not exceed the max_allowed_packet size for the MySQL server. The copied rows are copied
        in a similar fashion as the full copy (see perform_full_copy routine below).
        
        This routine fails if it detects schema differences in source and target or if the fieldHash
        column is missing or if the percent difference in number of rows from source to dest is > 40% (a
        full copy makes more sense in this case). Percent diff is calculated as:
        
        (number of rows to be added to dest + number of rows to be removed from dest) / total rows in dest
        
        Keyword arguments:
            table -- String name of the table to copy
            source_host -- MySQLHost source host to copy from (can be remote)
            dest_host -- MySQLHost destination host to copy to (must be local)
            scp_user -- String representing the user to connect remotely as when SCPing the file
            dump_dir -- String containing the location on the source and dest to store the file
               
        returns --  True if the copy succeeds, false otherwise.
    """
    if not dest_host.table_exists(table):
        logger.debug("Sync Error: Table %s does not exist in target DB." % table)
        return False
        
    if not schema_compare(table, source_host, dest_host, False):
        logger.debug("Sync Error: Table structures do not match.")
        return False
    
    if re.search('fieldhash', source_host.get_table_structure(table), re.I) is None:
        logger.debug("Sync Error: Tables do not hash fields.")
        return False

    logger.debug("Syncing table %s" % table)
    
    sourceHashSet = source_host.get_current_hash_set(table)
    targetHashSet = dest_host.get_current_hash_set(table)
    
    targetHashesToDel = targetHashSet.difference(sourceHashSet)
    targetHashesToAdd = sourceHashSet.difference(targetHashSet)
    
    lenTargetHashes = 1 if targetHashSet is None or len(targetHashSet) == 0 else len(targetHashSet)
    lenTargetHashesToDel = 0 if targetHashesToDel is None else len(targetHashesToDel)
    lenTargetHashesToAdd = 0 if targetHashesToAdd is None else len(targetHashesToAdd)
    if ((lenTargetHashesToAdd + lenTargetHashesToDel) / lenTargetHashes) > .4:
        logger.debug("Sync Error: tables too different (>40%), try full copy.")
        return False
    
    dest_host.delete_records(table, targetHashesToDel)
    
    if targetHashesToAdd is not None and lenTargetHashesToAdd > 0:
        csvfilename = source_host.select_into_outfile(table, targetHashesToAdd, dump_dir)
        retrieve_remote_dumpfile(source_host, scp_user, csvfilename, csvfilename)
        dest_host.load_data_in_file(table, csvfilename)
        
    logger.debug("Tables should now be in sync.")
    
    return True

def perform_full_copy(table, source_host, dest_host, scp_user, dump_dir):
    """
        Performs a full copy of the specified table from source to destination by selecting 
        all rows into an outfile, SCPing the file from the remote machine (iff source is remote), 
        truncating the data in the destination table and then loading the file into the local 
        destination table. If the destination schema differs from the source schema then it will 
        be dropped and recreated, if the target schema does not exist it will be created.
         
        Keyword arguments:
            table -- String name of the table to copy
            source_host -- MySQLHost source host to copy from (can be remote)
            dest_host -- MySQLHost destination host to copy to (must be local)
            scp_user -- String representing the user to connect remotely as when SCPing the file
            dump_dir -- String containing the location on the source and dest to store the file
               
        returns --  True if the copy succeeds, false otherwise.
    """
    if not source_host.table_exists(table):
        logger.error("Source table %s does not exist in database %s on %s" % \
                          (table, source_host.database, source_host))
        return False
    
    init_target_schema = False
    if not dest_host.table_exists(table):
        logger.debug("Table %s does not exist in target DB...it will be created." % table)
        init_target_schema = True
    elif not schema_compare(table, source_host, dest_host, True): 
        logger.debug("Target table structure does not match source...it will be re-created.")
        init_target_schema = True
    
    if init_target_schema:
        dest_host.create_table_with_schema(table, source_host.get_table_structure(table))
    
    csvfilename = source_host.select_into_outfile(table, None, dump_dir)
    
    if not retrieve_remote_dumpfile(source_host, scp_user, csvfilename, csvfilename):
        logger.error("Error retrieving remote file %s, check ssh config and remote permissions for %s on %s" % \
                          (csvfilename, scp_user, source_host))
        return False

    dest_host.truncate_table(table)
    dest_host.load_data_in_file(table, csvfilename)
    os.remove(csvfilename)

    return True

def perform_validity_check(table, source_host, dest_host, threshold):
    '''
        Performs a row count threshold check. 
        
        Keyword arguments:
            table -- String name of the table to check
            source_host -- MySQLHost source host to check
            dest_host -- MySQLHost destination host to check
            threshold -- an integer percent threshold value indicating the acceptable
                         reduction in the number of rows in the source table. This check
                         fails if the source is short by this percent.
               
        returns --  If the source host has less rows than the target by "threshold" percent 
                    then this check fails by returning 0. If source contains more rows than 
                    target return 1. If target table does not exist return 1.
    '''
    if (threshold == None or threshold == 0):
        return 1
    if not dest_host.table_exists(table):
        return 1
    source_row_count = source_host.get_row_count(table)
    dest_row_count = dest_host.get_row_count(table)
    if source_row_count >= dest_row_count:
        return 1

    ratioInPct = (float(source_row_count) / float(dest_row_count)) * 100
    
    if 100 - ratioInPct < threshold:
        logger.debug('Validity check for table %s: source has %d rows, dest has %d rows, %d percent diff, threshold is %d percent)' % \
                     (table, source_row_count, dest_row_count, 100 - ratioInPct, threshold))
        return 1

    logger.error('Table %s failed validation: source has %d rows, destination has %d rows, %d percent diff, threshold is %d percent' % \
                 (table, source_row_count, dest_row_count, 100 - ratioInPct, threshold))
    return 0

def schema_compare(table, source_host, dest_host, include_keys=False):
    '''
        Performs a schema comparison for the specified table possibly ignoring indexes/keys. 
        
        Keyword arguments:
            table -- String name of the table to compare
            source_host -- MySQLHost source host to compare schema from
            dest_host -- MySQLHost destination host to compare schema to
            include_keys -- boolean switch to cause the comparison to exclude indexes/keys
                            from the comparison (defaults to False).
               
        returns --  True if schemas are identical sans keys (unless include_keys is True)
    '''
    pattern = r',?\n *key.*?\),?| |\t'
    if include_keys:
        pattern = r' |\t'
    
    sourceStruct = re.sub(pattern, '', source_host.get_table_structure(table).lower())
    targetStruct = re.sub(pattern, '', dest_host.get_table_structure(table).lower())
    
    if sourceStruct != targetStruct:
        logger.debug("The following source structure:")
        logger.debug(sourceStruct)
        logger.debug("...is not equal to the following target structure:")
        logger.debug(targetStruct)
        return 0
    
    return 1

def is_last_mod_same(table, source_host, dest_host):
    '''
        Compares for equality the max lastModifiedDate (if it exists) for the specified 
        table on source and destination. This check is contingent on row counts being the 
        same. If row counts do not match then the precondition fails and this check fails.
        
        Keyword arguments:
            source_host -- MySQLHost from which to compare the max lastModifiedDate
            dest_host -- MySQLHost to which to compare the max lastModifiedDate
               
        returns -- If either table does not have a lastModifiedField, return 0.
                   If destination lastMod is >= source lastMod and row counts are equal
                   then return 1.
                   Otherwise return 0.
    '''
    if source_host.get_row_count(table) == dest_host.get_row_count(table):
        src_last_mod = source_host.get_table_max_modified(table)
        dest_last_mod = dest_host.get_table_max_modified(table)
        if(src_last_mod is None or src_last_mod == -1 or 
           dest_last_mod is None or dest_last_mod == -1):
            return 0
        if dest_last_mod == src_last_mod:
            return 1
    return 0

def retrieve_remote_dumpfile(source_host, scp_user, remote_filename, local_filename):
    """
        Retrieves a file from a remote host via SCP.
        
        Keyword arguments:
            source_host -- String hostname or IP of the host from which to retrieve the file
            scp_user -- String of the username to connect to source_host as
            remote_filename -- String containing the remote filesystem path of the file to retrieve
            local_filename -- String containing the local filesystem path to which to store the file 
               
        returns -- True if file retrieval was successful, False otherwise
    """
    if source_host.host != 'localhost':
        logger.debug("Retrieving remote file %s@%s:%s to %s" % (scp_user, source_host.host, remote_filename, local_filename))
        quietOpt = "" if settings.verbosity > 0 else "-q"
        exit_status = os.system('scp -c arcfour -C -B %s "%s@%s:%s" "%s"' % (quietOpt, scp_user, source_host.host, remote_filename, local_filename))
        if exit_status != 0:
            logger.debug("Error retrieving remote file, check ssh config!")
            return False
        # cleanup
        logger.debug("Removing remote file %s from %s" % (remote_filename, source_host.host))
        quietOpt = "" if settings.verbosity > 0 else " &> /dev/null"
        exit_status = os.system('echo "rm \"%s\"" | sftp -b - "%s@%s" %s' % (remote_filename, scp_user, source_host.host, quietOpt))
        if exit_status != 0:
            logger.warn("Error removing remote dump file %s, check remote permissions for %s on %s!" % \
                             (remote_filename, scp_user, source_host.host))
        else:
            logger.debug("Successfully removed file %s on host %s" % (remote_filename, source_host.host))
    return True

def get_option_parser():
    """Get a handler for command line arguments"""
    
    usage = "%prog [OPTIONS] \n" \
          + "   Copies a MySQL database from one MySQL server to \n" \
          + "   another. If this DB has been copied before performs a \n" \
          + "   magical incremental copy. If -f is specified then command \n" \
          + "   line args take precedence over properties in the property file"
    
    parser = OptionParser(usage=usage)
    
    parser.add_option('-f', '--properties',
                      action='store', type='string', dest='properties', metavar='PROPS',
                      help='The properties file that specifies DB connection info and other properties')
    
    parser.add_option('-G', '--sourcehost',
                      action='store', type='string', dest='source_host', metavar='HOST',
                      help='specify database host to copy from [default: %s]' % settings.source_host)
    
    parser.add_option('-U', '--sourceuser',
                      action='store', type='string', dest='source_user', metavar='USER',
                      help='specify source database user [default: %s]' % settings.source_user)
    
    parser.add_option('-W', '--sourcepassword',
                      action='store', type='string', dest='source_password', metavar='PASSWORD',
                      help='specify source database password')
    
    parser.add_option('-D', '--sourcedb',
                      action='store', type='string', dest='source_database', metavar='DBNAME',
                      help='specify source database schema [default: %s]' % settings.source_database)
    
    parser.add_option('-I', '--targethost',
                      action='store', type='string', dest='target_host', metavar='HOST',
                      help='specify database host to copy to [default: %s]' % settings.target_host)
    
    parser.add_option('-u', '--targetuser',
                      action='store', type='string', dest='target_user', metavar='USER',
                      help='specify target database user [default: %s]' % settings.target_user)
    
    parser.add_option('-w', '--targetpassword',
                      action='store', type='string', dest='target_password', metavar='PASSWORD',
                      help='specify target database password')
    
    parser.add_option('-d', '--targetdb',
                      action='store', type='string', dest='target_database', metavar='DBNAME',
                      help='specify target database schema [default: %s]' % settings.target_database)
    
    parser.add_option('-s', '--scpuser',
                      action='store', type='string', dest='scp_user', metavar='USER',
                      help='specify remote user to authenticate over scp as if source host is not localhost [default: %s]' % settings.scp_user)
    
    parser.add_option('-t', '--tables',
                      action='store', type='string', dest='tables', metavar='\"TABLE1 TABLE2 ...\"',
                      help='specify space delimited list of tables to be copied')
    
    parser.add_option('-T', '--skiptables',
                      action='store', type='string', dest='tables_to_skip_verification', metavar='\"TABLE1 TABLE2 ...\"',
                      help='specify space delimited list of tables to skip verification step')

    parser.add_option('-m', '--dumpdir',
                      action='store', type='string', dest='dump_dir', metavar='DIR',
                      help='specify mysql dump dir, this needs to be world writable so we can cleanup [default: %s]' % settings.dump_dir)
    
    parser.add_option('-v', '--verbose',
                      action='store_true',
                      dest='verbose',
                      help='Print verbose output [default: %s]' % settings.verbosity)
    
    parser.add_option('-V', '--verify',
                      action='store',
                      type='int',
                      dest='verify_threshold',
                      help='Verify all tables are within a certain percentage of original size. Don\'t copy if any are not [default: %s]' % settings.verify_threshold)

    parser.add_option('-F', '--forcefull',
                      action='store_true',
                      dest='force_full',
                      help='Forces a full copy rather than first attempting an incremental copy [default: %s]' % settings.force_full)
    
    parser.add_option('-n', '--nolastmodcheck',
                      action='store_true',
                      dest='no_last_mod_check',
                      help='Skips checking the last modified times of the source and dest [default: %s]' % settings.no_last_mod_check)
    
    parser.add_option('-p', '--numprocesses',
                      action='store',
                      type='int',
                      dest='num_processes',
                      help='Set the number of processes to fork for copying tables [default: number of CPUs - 1]. Ignored if debug mode is true.')
    
    parser.add_option('-g', '--debug',
                      action='store_true',
                      dest='debug',
                      help='Run in debug mode, turns off multi-processing [default: %s]' % settings.debug)

    return parser
 
if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.stderr.write('\nCancelled by keyboard interrupt.\n')
        sys.exit(1)
