import unittest
import pydbcopy
from dbutils import MySQLHost
from config import settings
import multiprocessing
import logging
import sys


logger = multiprocessing.get_logger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(processName)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

class PyDBCopyTest(unittest.TestCase):
    """
        These tests need a world writable dump dir. The default directory is /share/mysql_dumps.
        Please keep in mind that issuing the SQL 'select into outfile' will cause mysql to write
        files as the user mysql is running as. The dump dir needs to be world writable so that 
        these tests can clean up the temp files that the mysql user writes. 
    """
    
    def setUp(self):
        settings.read_properties("pydbcopy.conf")
        
        self.source_host = MySQLHost(settings.source_host, settings.source_user, \
                            settings.source_password, settings.source_database)
        self.dest_host = MySQLHost(settings.target_host, settings.target_user, \
                          settings.target_password, settings.target_database)

        #
        # Bring up the fixture
        #
        c = self.source_host.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
        # Create tmp_pydbcopy_test table and a single row
        c.execute("create table if not exists tmp_pydbcopy_test ( id integer primary key, test_string varchar(50) )")
        c.execute("insert into tmp_pydbcopy_test (id,test_string) values (1,'test')")
        # Create tmp_hashed_pydbcopy_test table and three rows
        c.execute("create table if not exists tmp_hashed_pydbcopy_test ( id integer primary key, test_string varchar(50), fieldHash varchar(50) )")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (1,'test','123')")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (2,'test1','234')")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (3,'test2','345')")
        # Create tmp_pydbcopy_modified_table and a single row
        c.execute("create table if not exists tmp_pydbcopy_modified_test ( id integer primary key, test_string varchar(50), lastModifiedDate timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP )")
        c.execute("insert into tmp_pydbcopy_modified_test (id,test_string, lastModifiedDate) values (1,'test', '2010-11-23 05:00:00')")
        c.close()
        
        c = self.dest_host.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
        c.execute("create table if not exists tmp_pydbcopy_modified_test ( id integer primary key, test_string varchar(50), lastModifiedDate timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP )")
        c.execute("insert into tmp_pydbcopy_modified_test (id,test_string, lastModifiedDate) values (1,'test', '2010-11-22 05:00:00')")
        c.close()


    def tearDown(self):
        #
        # Tear down the fixture
        #
        c = self.source_host.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
        c.execute("drop table if exists tmp_pydbcopy_test")
        c.execute("drop table if exists tmp_hashed_pydbcopy_test")
        c.execute("drop table if exists tmp_pydbcopy_modified_test")
        c.close()
        
        c = self.dest_host.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
        c.execute("drop table if exists tmp_pydbcopy_test")
        c.execute("drop table if exists tmp_hashed_pydbcopy_test")
        c.execute("drop table if exists tmp_pydbcopy_modified_test")
        c.close()


    def testPerformIncrementalCopy(self):
        
        sc = self.source_host.conn.cursor()
        sc.execute("SET AUTOCOMMIT=1")
        
        dc = self.dest_host.conn.cursor()
        dc.execute("SET AUTOCOMMIT=1")
        
        # First test our expected fail cases...
        self.assertFalse(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("create table if not exists tmp_hashed_pydbcopy_test ( id integer primary key, test_string varchar(50) )")
        
        self.assertFalse(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))

        dc.execute("create table if not exists tmp_pydbcopy_test ( id integer primary key, test_string varchar(50) )")
        
        self.assertFalse(pydbcopy.perform_incremental_copy('tmp_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))

        # Special fail case: if target is > 40% different fail (prefer a full copy which is faster)
        sc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = sc.fetchall()
        
        self.assertEquals(len(rows), 3)
        
        self.assertEquals(rows[0][0], 1)
        self.assertEquals(rows[0][1], 'test')
        self.assertEquals(rows[0][2], '123')
        self.assertEquals(rows[1][0], 2)
        self.assertEquals(rows[1][1], 'test1')
        self.assertEquals(rows[1][2], '234')
        self.assertEquals(rows[2][0], 3)
        self.assertEquals(rows[2][1], 'test2')
        self.assertEquals(rows[2][2], '345')
        
        dc.execute("drop table if exists tmp_hashed_pydbcopy_test")
        dc.execute("create table if not exists tmp_hashed_pydbcopy_test ( id integer primary key, test_string varchar(50), fieldHash varchar(50) )")
        
        self.assertFalse(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))

        # Now test the expected behavior of the incremental copy.
        sc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = sc.fetchall()
        
        self.assertEquals(len(rows), 3)
        
        self.assertEquals(rows[0][0], 1)
        self.assertEquals(rows[0][1], 'test')
        self.assertEquals(rows[0][2], '123')
        self.assertEquals(rows[1][0], 2)
        self.assertEquals(rows[1][1], 'test1')
        self.assertEquals(rows[1][2], '234')
        self.assertEquals(rows[2][0], 3)
        self.assertEquals(rows[2][1], 'test2')
        self.assertEquals(rows[2][2], '345')
        
        sc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-1,'test-1','111')")
        sc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-2,'test-2','222')")
        sc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-3,'test-3','333')")
        sc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-4,'test-4','444')")
        
        dc.execute("drop table if exists tmp_hashed_pydbcopy_test")
        dc.execute("create table if not exists tmp_hashed_pydbcopy_test ( id integer primary key, test_string varchar(50), fieldHash varchar(50) )")
        dc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-1,'test-1','111')")
        dc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-2,'test-2','222')")
        dc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-3,'test-3','333')")
        dc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (-4,'test-4','444')")
                
        self.assertTrue(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))

        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 7)
        
        self.assertEquals(rows[0][0], -1)
        self.assertEquals(rows[0][1], 'test-1')
        self.assertEquals(rows[0][2], '111')
        self.assertEquals(rows[1][0], -2)
        self.assertEquals(rows[1][1], 'test-2')
        self.assertEquals(rows[1][2], '222')
        self.assertEquals(rows[2][0], -3)
        self.assertEquals(rows[2][1], 'test-3')
        self.assertEquals(rows[2][2], '333')
        self.assertEquals(rows[3][0], -4)
        self.assertEquals(rows[3][1], 'test-4')
        self.assertEquals(rows[3][2], '444')
        
        self.assertEquals(rows[4][0], 1)
        self.assertEquals(rows[4][1], 'test')
        self.assertEquals(rows[4][2], '123')
        self.assertEquals(rows[5][0], 2)
        self.assertEquals(rows[5][1], 'test1')
        self.assertEquals(rows[5][2], '234')
        self.assertEquals(rows[6][0], 3)
        self.assertEquals(rows[6][1], 'test2')
        self.assertEquals(rows[6][2], '345')
        
        # Now add a row to the source
        sc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (4,'test3','456')")

        self.assertTrue(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 8)
        
        self.assertEquals(rows[0][0], -1)
        self.assertEquals(rows[0][1], 'test-1')
        self.assertEquals(rows[0][2], '111')
        self.assertEquals(rows[1][0], -2)
        self.assertEquals(rows[1][1], 'test-2')
        self.assertEquals(rows[1][2], '222')
        self.assertEquals(rows[2][0], -3)
        self.assertEquals(rows[2][1], 'test-3')
        self.assertEquals(rows[2][2], '333')
        self.assertEquals(rows[3][0], -4)
        self.assertEquals(rows[3][1], 'test-4')
        self.assertEquals(rows[3][2], '444')
        
        self.assertEquals(rows[4][0], 1)
        self.assertEquals(rows[4][1], 'test')
        self.assertEquals(rows[4][2], '123')
        self.assertEquals(rows[5][0], 2)
        self.assertEquals(rows[5][1], 'test1')
        self.assertEquals(rows[5][2], '234')
        self.assertEquals(rows[6][0], 3)
        self.assertEquals(rows[6][1], 'test2')
        self.assertEquals(rows[6][2], '345')
        self.assertEquals(rows[7][0], 4)
        self.assertEquals(rows[7][1], 'test3')
        self.assertEquals(rows[7][2], '456')

        # Now delete one
        sc.execute("delete from tmp_hashed_pydbcopy_test where id=4")

        self.assertTrue(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 7)
        
        self.assertEquals(rows[0][0], -1)
        self.assertEquals(rows[0][1], 'test-1')
        self.assertEquals(rows[0][2], '111')
        self.assertEquals(rows[1][0], -2)
        self.assertEquals(rows[1][1], 'test-2')
        self.assertEquals(rows[1][2], '222')
        self.assertEquals(rows[2][0], -3)
        self.assertEquals(rows[2][1], 'test-3')
        self.assertEquals(rows[2][2], '333')
        self.assertEquals(rows[3][0], -4)
        self.assertEquals(rows[3][1], 'test-4')
        self.assertEquals(rows[3][2], '444')
        
        self.assertEquals(rows[4][0], 1)
        self.assertEquals(rows[4][1], 'test')
        self.assertEquals(rows[4][2], '123')
        self.assertEquals(rows[5][0], 2)
        self.assertEquals(rows[5][1], 'test1')
        self.assertEquals(rows[5][2], '234')
        self.assertEquals(rows[6][0], 3)
        self.assertEquals(rows[6][1], 'test2')
        self.assertEquals(rows[6][2], '345')
        
        # Now update one
        sc.execute("update tmp_hashed_pydbcopy_test set test_string='new_test1',fieldHash='012' where id=2")

        self.assertTrue(pydbcopy.perform_incremental_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 7)
        
        self.assertEquals(rows[0][0], -1)
        self.assertEquals(rows[0][1], 'test-1')
        self.assertEquals(rows[0][2], '111')
        self.assertEquals(rows[1][0], -2)
        self.assertEquals(rows[1][1], 'test-2')
        self.assertEquals(rows[1][2], '222')
        self.assertEquals(rows[2][0], -3)
        self.assertEquals(rows[2][1], 'test-3')
        self.assertEquals(rows[2][2], '333')
        self.assertEquals(rows[3][0], -4)
        self.assertEquals(rows[3][1], 'test-4')
        self.assertEquals(rows[3][2], '444')
        
        self.assertEquals(rows[4][0], 1)
        self.assertEquals(rows[4][1], 'test')
        self.assertEquals(rows[4][2], '123')
        self.assertEquals(rows[5][0], 2)
        self.assertEquals(rows[5][1], 'new_test1')
        self.assertEquals(rows[5][2], '012')
        self.assertEquals(rows[6][0], 3)
        self.assertEquals(rows[6][1], 'test2')
        self.assertEquals(rows[6][2], '345')
        
        sc.close()
        dc.close()
        
    def testPerformFullCopy(self):
        '''
        In order to fully stress the SCP part of this code run this test with the source_host configured to an external host. 
        '''
        sc = self.source_host.conn.cursor()
        sc.execute("SET AUTOCOMMIT=1")
        
        dc = self.dest_host.conn.cursor()
        dc.execute("SET AUTOCOMMIT=1")
        
        self.assertTrue(pydbcopy.perform_full_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))

        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 3)
        
        self.assertEquals(rows[0][0], 1)
        self.assertEquals(rows[0][1], 'test')
        self.assertEquals(rows[0][2], '123')
        self.assertEquals(rows[1][0], 2)
        self.assertEquals(rows[1][1], 'test1')
        self.assertEquals(rows[1][2], '234')
        self.assertEquals(rows[2][0], 3)
        self.assertEquals(rows[2][1], 'test2')
        self.assertEquals(rows[2][2], '345')
        
        # Now add a row to the source
        sc.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (4,'test3','456')")

        self.assertTrue(pydbcopy.perform_full_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 4)
        
        self.assertEquals(rows[0][0], 1)
        self.assertEquals(rows[0][1], 'test')
        self.assertEquals(rows[0][2], '123')
        self.assertEquals(rows[1][0], 2)
        self.assertEquals(rows[1][1], 'test1')
        self.assertEquals(rows[1][2], '234')
        self.assertEquals(rows[2][0], 3)
        self.assertEquals(rows[2][1], 'test2')
        self.assertEquals(rows[2][2], '345')
        self.assertEquals(rows[3][0], 4)
        self.assertEquals(rows[3][1], 'test3')
        self.assertEquals(rows[3][2], '456')
        
        # Now delete one
        sc.execute("delete from tmp_hashed_pydbcopy_test where id=4")

        self.assertTrue(pydbcopy.perform_full_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 3)
        
        self.assertEquals(rows[0][0], 1)
        self.assertEquals(rows[0][1], 'test')
        self.assertEquals(rows[0][2], '123')
        self.assertEquals(rows[1][0], 2)
        self.assertEquals(rows[1][1], 'test1')
        self.assertEquals(rows[1][2], '234')
        self.assertEquals(rows[2][0], 3)
        self.assertEquals(rows[2][1], 'test2')
        self.assertEquals(rows[2][2], '345')
        
        # Now update one
        sc.execute("update tmp_hashed_pydbcopy_test set test_string='new_test1',fieldHash='012' where id=2")

        self.assertTrue(pydbcopy.perform_full_copy('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, settings.scp_user, settings.dump_dir))
        
        dc.execute("select * from tmp_hashed_pydbcopy_test")
        rows = dc.fetchall()
        
        self.assertEquals(len(rows), 3)
        
        self.assertEquals(rows[0][0], 1)
        self.assertEquals(rows[0][1], 'test')
        self.assertEquals(rows[0][2], '123')
        self.assertEquals(rows[1][0], 2)
        self.assertEquals(rows[1][1], 'new_test1')
        self.assertEquals(rows[1][2], '012')
        self.assertEquals(rows[2][0], 3)
        self.assertEquals(rows[2][1], 'test2')
        self.assertEquals(rows[2][2], '345')
        
        sc.close()
        dc.close()
        
    def testPerformValidityCheck(self):
        c = self.dest_host.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
        
        # First test our expected valid cases...
        self.assertTrue(pydbcopy.perform_validity_check('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, 0))

        self.assertTrue(pydbcopy.perform_validity_check('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, 100))

        c.execute("create table if not exists tmp_hashed_pydbcopy_test ( id integer primary key, test_string varchar(50), fieldHash varchar(50) )")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (1,'test','123')")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (2,'test1','234')")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (3,'test2','345')")
        
        self.assertTrue(pydbcopy.perform_validity_check('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, 1))
        
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (4,'test3','456')")
        
        self.assertTrue(pydbcopy.perform_validity_check('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, 100))
        
        self.assertFalse(pydbcopy.perform_validity_check('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, 25))
        
        c.close()
    
    def testSchemaCompare(self):
        c = self.dest_host.conn.cursor()
        c.execute("SET AUTOCOMMIT=1")
        
        c.execute("create table if not exists tmp_hashed_pydbcopy_test ( id integer primary key, test_string varchar(50), fieldHash varchar(50) )")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (1,'test','123')")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (2,'test1','234')")
        c.execute("insert into tmp_hashed_pydbcopy_test (id,test_string,fieldHash) values (3,'test2','345')")
        
        self.assertTrue(pydbcopy.schema_compare('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, False))
        
        c.execute("alter table tmp_hashed_pydbcopy_test add index idx_fieldHash (fieldHash)")
        
        self.assertTrue(pydbcopy.schema_compare('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, False))
        
        self.assertFalse(pydbcopy.schema_compare('tmp_hashed_pydbcopy_test', self.source_host, self.dest_host, True))
        
        c.close()
        
    def testDestLastModifiedIsAsRecent(self):
        ''' The only condition that should cause the is_last_mod_same check is when the max(lastModifiedDate)
            of both tables equates and the row counts equate.
        
            Also, test to make sure the comparison of a table without modified dates is false.
        '''
        sc = self.source_host.conn.cursor()
        sc.execute("SET AUTOCOMMIT=1")
        
        dc = self.dest_host.conn.cursor()
        dc.execute("SET AUTOCOMMIT=1")
        
        # No lastModified date on the table
        self.assertFalse(pydbcopy.is_last_mod_same('tmp_pydbcopy_test', self.source_host, self.dest_host))

        # max(lastModfiedDate) does not equate        
        self.assertFalse(pydbcopy.is_last_mod_same('tmp_pydbcopy_modified_test', self.source_host, self.dest_host))
        
        sc.execute("truncate table tmp_pydbcopy_modified_test")
        sc.execute("insert into tmp_pydbcopy_modified_test (id,test_string,lastModifiedDate) values (2,'test2','2010-11-23 05:00:00')")
        
        dc.execute("truncate table tmp_pydbcopy_modified_test")
        dc.execute("insert into tmp_pydbcopy_modified_test (id,test_string,lastModifiedDate) values (2,'test2','2010-11-23 05:00:00')")
        
        # Now everything equates
        self.assertTrue(pydbcopy.is_last_mod_same('tmp_pydbcopy_modified_test', self.source_host, self.dest_host))
        
        sc.execute("insert into tmp_pydbcopy_modified_test (id,test_string,lastModifiedDate) values (3,'test3','2010-11-23 05:00:00')")
        
        # Now row counts differ
        self.assertFalse(pydbcopy.is_last_mod_same('tmp_pydbcopy_modified_test', self.source_host, self.dest_host))
        
        sc.close()
        dc.close()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
