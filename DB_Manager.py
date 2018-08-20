class DB_MANAGER:
    class __DB_READER_DECS:
        __DB_FILE = None
        def __get__(self, instance, owner):
            return self.__DB_FILE
        def __set__(self, instance, data):
            self.__DB_FILE = data
    class __DB_TABLES_DECS:
        __DB_TABLES = None
        def __get__(self, instance, owner):
            return self.__DB_TABLES
        def __set__(self, instance, TABLES):
            self.__DB_TABLES = TABLES
    class __Security:
        @staticmethod
        def Encrypt(data):
            return data
        def Decrypt(data):
            return data
    class __Exceptions:
        class InvalidDB(Exception):
            __module__ = Exception.__module__
        class AuthError(Exception):
            __module__ = Exception.__module__
        class InvalidTable(Exception):
            __module__ = Exception.__module__
        class InvalidRowData(Exception):
            __module__ = Exception.__module__
    #<config>
    __AUTH_PATTERN = r'(?x)(?<=\<auth\>username\=\")[\w\d]{4,}(?=\",password\=\")|(?<=,password\=\").{4,}(?=\"\<\/auth\>)'
    __DB_FILE_PATTERN = r'(?x)(?<=\<DB\>).*(?=\<\/DB\>)'
    __DB_TABLES_PATTERN = r'(?x)(?<=\<table\>).*?(?=\<\/table\>)'
    __DB_TABLE_NAME_PATTERN = r'(?x)(?<=\<table_name\=\').*?(?=\'\/>)'
    __DB_TABLE_FIELDS_PATTERN = r'(?x)(?<=\<table_fields\=\[).*?(?=\]\/>)'
    __DB_TABLE_ROWS_PATTERN = r'(?x)(?<=\<row\>).*(?=\<\/row\>)'
    __TABLE_QUERY = lambda self, table_name, table_fields: "\t<table><table_name='%s'/><table_fields=%s/></table>\n" % (table_name, str(table_fields))
    __ROW_QUERY = lambda self, values: "\t<row>%s</row>\n" % values
    __Tables_Names, __Tables_Fields, __Tables_Rows = [], [], []
    __DB_FILE = __DB_READER_DECS()
    __DB_FILE_NAME = None
    __Logged = False
    #</config>
    #<imports>
    from os import path, F_OK, access, R_OK, W_OK, O_RDONLY
    import re
    #</imports>
    def load(self, DB_File, username=None, password=None):
        if self.path.split(DB_File)[-1].split('.')[-1].lower() != 'db':
            raise self.__Exceptions.InvalidDB('This Is Not Valid DB File Extension')
        if self.access(DB_File, self.F_OK | self.R_OK):
            with open(DB_File, 'r+') as db_reader:
                self.__DB_FILE = self.__Security.Decrypt(db_reader.read())
            try:
                user, passwd = self.re.findall(self.__AUTH_PATTERN, self.__DB_FILE)
                if isinstance(passwd, list) or user != username or passwd != password:
                    raise self.__Exceptions.AuthError("Please enter Valid DB File with this authenitication values")
                self.__DB_FILE_NAME = DB_File
                self.__Logged = True
                tables = self.re.findall(self.__DB_TABLES_PATTERN, self.__DB_FILE, flags=self.re.DOTALL)
                [(self.__Tables_Names.append(self.re.search(self.__DB_TABLE_NAME_PATTERN, table, self.re.DOTALL).group()), self.__Tables_Fields.append(self.re.search(self.__DB_TABLE_FIELDS_PATTERN, table, self.re.DOTALL).group().replace(' ', '').replace("'", '').split(',')), self.__Tables_Rows.append(self.re.findall(self.__DB_TABLE_ROWS_PATTERN, table))) for table in tables]
            except ValueError:
                raise self.__Exceptions.AuthError("Please enter Valid DB File with this authenitication values") from None
    def create_table(self, Table_Name, Table_Fields=[]):
        if self.__Logged and self.__DB_FILE != None:
            if Table_Name not in self.__Tables_Names and Table_Fields:
                DB = self.re.search('\<\/DB\>', self.__DB_FILE)
                table_query = self.__TABLE_QUERY(Table_Name, Table_Fields)
                self.__DB_FILE = self.__DB_FILE[:DB.start()] + table_query + self.__DB_FILE[DB.start():]
                self.__Tables_Names.append(Table_Name)
                self.__Tables_Fields.append(Table_Fields)
                self.__Tables_Rows.append([])
                with open(self.__DB_FILE_NAME, 'r+') as Table_Writer:
                    Table_Writer.seek(DB.start())
                    Table_Writer.write(table_query)
                    Table_Writer.write('</DB>')
                print(self.__DB_FILE)
            else:
                raise self.__Exceptions.InvalidTable("Either table name is duplicated or empty Fields")
        else:
            raise self.__Exceptions.InvalidDB("Wrong Auth or Wrong DB File")
        pass
    def create_row(self, Table_Name, Values=[]):
        table_index = self.__Tables_Names.index(Table_Name)
        if self.__Logged and table_index != -1:
            values = '|'.join(Values)
            if values in self.__Tables_Rows[table_index]:
                raise self.__Exceptions.InvalidRowData("This Row Is Duplicated")
            fields = self.__Tables_Fields[table_index]
            if len(Values) == len(fields):
                row = self.__ROW_QUERY(values)
                self.__Tables_Rows[self.__Tables_Names.index(Table_Name)].append(row)
                table_pos = self.re.search(r'(?x)(?<=\<table\>\<table_name=\'%s\'\/\>).*?(?=\<\/table\>)' % Table_Name, self.__DB_FILE, self.re.DOTALL)
                with open(self.__DB_FILE_NAME, 'r+') as Row_Writer:
                    Row_Writer.seek(table_pos.end())
                    Row_Writer.write(row)
                    Row_Writer.write(self.__DB_FILE[table_pos.end():])
                self.__DB_FILE = self.__DB_FILE[:table_pos.end()] + row + self.__DB_FILE[table_pos.end():]
            else:
                raise self.__Exceptions.InvalidRowData("%s is needed but your entered %s" % (fields, Values))
        else:
            raise self.__Exceptions.InvalidTable('This Table either not exists or You are not logged')
    def get_table(self, Table_Name):
        table_index = self.__Tables_Names.index(Table_Name)
        if self.__Logged and table_index != -1:
            table_fields = self.__Tables_Fields[table_index]
            table_rows = [x.split('|') for x in self.__Tables_Rows[table_index]]
            return (table_fields, table_rows)
        else:
            raise self.__Exceptions.InvalidTable('This Table Doesn\'t exists')
    def get_row(self, Table_Name, searching_by={}):
        table_index = self.__Tables_Names.index(Table_Name)
        if self.__Logged and table_index != -1:
            table_fields = self.__Tables_Fields[table_index]
            indeces = [(table_fields.index(x), str(searching_by[x])) for x in searching_by]
            table_rows = self.__Tables_Rows[table_index]
            result = []
            for row in table_rows:
                mrow = row.split('|')
                result += [row] if all([True if mrow[index] == value else False for index, value in indeces]) else []
            return result
        else:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in")
    def __init__(self, DB_FILE=None, username=None, password=None):
        self.__username, self.__password = username, password
        if DB_FILE:
            self.load(DB_FILE, username, password)
