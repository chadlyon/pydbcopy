import re
class Settings:
    def __init__(self):
        
        #
        # Defaults: override in pydbcopy.conf
        #
        
        # path to dump directory
        self.dump_dir = '/share/mysql_dumps'
        
        # 0 = normal, -1 = quiet, 1 = verbose
        self.verbosity = 0
        
        self.source_host = 'localhost'
        self.source_user = 'guest'
        self.source_password = ''
        self.source_database = 'test'
        
        self.tables = []
        self.tables_to_skip_verification = []
        
        self.verify_threshold = 25
        
        self.force_full = False
        
        self.no_last_mod_check = False
        
        self.target_host = 'localhost'
        self.target_user = 'guest'
        self.target_password = ''
        self.target_database = 'test_copy'
        
        self.scp_user = 'guest'
        
        self.num_processes = 0
        
        self.debug = False
        
    def read_properties(self, propFileLoc):
        propFile= file( propFileLoc, "rU" )
        propDict= dict()
        for propLine in propFile:
            propDef= propLine.strip()
            if len(propDef) == 0:
                continue
            if propDef[0] in ( '!', '#' ):
                continue
            punctuation= [ propDef.find(c) for c in '=' ] + [ len(propDef) ]
            found= min( [ pos for pos in punctuation if pos != -1 ] )
            name= re.sub(r'\.', '_', propDef[:found].rstrip().lower()) 
            value= propDef[found:].lstrip("= ").rstrip()
            propDict[name]= value
        propFile.close()
     
        if propDict.has_key('pydbcopy_source_host'):
            self.source_host = propDict['pydbcopy_source_host']
             
        if propDict.has_key('pydbcopy_source_jdbc_user'):
            self.source_user = propDict['pydbcopy_source_jdbc_user']
             
        if propDict.has_key('pydbcopy_source_jdbc_password'):
            self.source_password = propDict['pydbcopy_source_jdbc_password']
            
        if propDict.has_key('pydbcopy_source_jdbc_database'):
            self.source_database = propDict['pydbcopy_source_jdbc_database']
    
        if propDict.has_key('pydbcopy_tables'):
            self.tables = propDict['pydbcopy_tables'].split(" ")
     
        if propDict.has_key('pydbcopy_tables_to_skip_verification'):
            self.tables_to_skip_verification = propDict['pydbcopy_tables_to_skip_verification'].split(" ")
            
        if propDict.has_key('pydbcopy_target_host'):
            self.target_host = propDict['pydbcopy_target_host']
            
        if propDict.has_key('pydbcopy_target_jdbc_user'):
            self.target_user = propDict['pydbcopy_target_jdbc_user']
            
        if propDict.has_key('pydbcopy_target_jdbc_password'):
            self.target_password = propDict['pydbcopy_target_jdbc_password']
            
        if propDict.has_key('pydbcopy_target_jdbc_database'):
            self.target_database = propDict['pydbcopy_target_jdbc_database']
        
        if propDict.has_key('pydbcopy_scp_user'):
            self.scp_user = propDict['pydbcopy_scp_user']
            
        if propDict.has_key('pydbcopy_dump_dir'):
            self.dump_dir = propDict['pydbcopy_dump_dir']
        
        if propDict.has_key('pydbcopy_verbosity'):
            self.verbosity = propDict['pydbcopy_verbosity']
            
        if propDict.has_key('pydbcopy_verbose'):
            self.verbosity = propDict['pydbcopy_verbose'].lower() == 'true'
            
        if propDict.has_key('pydbcopy_verify_threshold'):
            self.verify_threshold = propDict['pydbcopy_verify_threshold']

        if propDict.has_key('pydbcopy_force_full'):
            self.force_full = propDict['pydbcopy_force_full'].lower() == 'true'
            
        if propDict.has_key('pydbcopy_no_last_mod_check'):
            self.no_last_mod_check = propDict['pydbcopy_no_last_mod_check'].lower() == 'true'
            
        if propDict.has_key('pydbcopy_num_processes'):
            if propDict['pydbcopy_num_processes'] is not None and propDict['pydbcopy_num_processes'] != '':
                self.num_processes = int(propDict['pydbcopy_num_processes'])
            
        if propDict.has_key('pydbcopy_debug'):
            self.debug = propDict['pydbcopy_debug']

settings = Settings()