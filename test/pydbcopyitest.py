import unittest
import pydbcopy


class Test(unittest.TestCase):
    """
        These integration tests are meant to test the main routine, argument passing/parsing, 
        argument validation, etc. 
    """

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testEmptyTableListReturnsNonZero(self):
        args = ["--sourcehost", "localhost",
                "--sourceuser", "guest",
                "--sourcepassword", "",
                "--sourcedb", "testsource",
                "--targethost", "localhost",
                "--targetuser", "guest",
                "--targetpassword", "",
                "--targetdb", "testtarget",
                "--tables", "",]
        assert pydbcopy.main(args) != 0


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()