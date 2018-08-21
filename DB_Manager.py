class DB_MANAGER:
    from threading import Thread, Lock
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
        class OverwritingExistingDB(Exception):
            __module__ = Exception.__module__
    #<config>
    __AUTH_PATTERN = r'(?x)(?<=\<auth\ username\=\")[\w\d]{4,}(?=\",password\=\")|(?<=,password\=\").{4,}(?=\"\/\>)'
    __DB_FILE_PATTERN = r'(?x)(?<=\<DB\>).*(?=\<\/DB\>)'
    __DB_TABLES_PATTERN = r'(?x)(?<=\<table\>).*?(?=\<\/table\>)'
    __DB_TABLE_NAME_PATTERN = r'(?x)(?<=\<table_name\=\').*?(?=\'\/>)'
    __DB_TABLE_FIELDS_PATTERN = r'(?x)(?<=\<table_fields\=\[).*?(?=\]\/>)'
    __DB_TABLE_ROWS_PATTERN = r'(?x)(?<=\<row\s).*(?=\>)'
    __TABLE_QUERY = lambda self, table_name, table_fields: "\n\t<table>\n\t\t<table_name='%s'/>\n\t\t<table_fields=%s/>\n\t</table>\n" % (table_name, str(table_fields))
    __ROW_QUERY = lambda self, values: "\n\t\t<row %s>" % values
    __Tables_Names, __Tables_Fields, __Tables_Rows = [], [], []
    __DB_FILE = __DB_READER_DECS()
    __DB_FILE_NAME = None
    __Logged = False
    #</config>
    #<imports>
    from os import path, F_OK, access, R_OK, W_OK, O_RDONLY, chmod
    import re
    #</imports>
    def load(self, DB_File, username=None, password=None):
        DB_File = DB_File if DB_File.endswith('.db') else '%s.db' % DB_File
        if self.access(DB_File, self.F_OK | self.R_OK):
            with open(DB_File, 'r+') as db_reader:
                self.__DB_FILE = self.__Security.Decrypt(db_reader.read())
            try:
                user, passwd = self.re.findall(self.__AUTH_PATTERN, self.__DB_FILE)
                if isinstance(passwd, list) or user != username or passwd != password:
                    raise self.__Exceptions.AuthError("Please enter Valid DB File with this authenitication values")
                #self.__Writer_Manger = self.__ThreadingROW()
                self.__DB_FILE_NAME = DB_File
                self.__Logged = True
                tables = self.re.findall(self.__DB_TABLES_PATTERN, self.__DB_FILE, flags=self.re.DOTALL)
                [(self.__Tables_Names.append(self.re.search(self.__DB_TABLE_NAME_PATTERN, table, self.re.DOTALL).group()), self.__Tables_Fields.append(self.re.search(self.__DB_TABLE_FIELDS_PATTERN, table, self.re.DOTALL).group().replace(' ', '').replace("'", '').split(',')), self.__Tables_Rows.append(self.re.findall(self.__DB_TABLE_ROWS_PATTERN, table))) for table in tables]
            except ValueError:
                raise self.__Exceptions.AuthError("Please enter Valid DB File with this authenitication values") from None
        else:
            raise self.__Exceptions.AuthError("Please enter Valid DB File with this authenitication values") from None
    global_locker = Lock()
    def __Writer(self, file_name, data, seek=0):
        while self.global_locker.locked():
            continue
        global_locker.acquire()
        with open(file_name, 'r+') as writer:
            writer.seek(seek)
            writer.write(data)
        global_locker.release()

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
            else:
                raise self.__Exceptions.InvalidTable("Either table name is duplicated or empty Fields")
        else:
            raise self.__Exceptions.InvalidDB("Wrong Auth or Wrong DB File")
        pass
    def create_row(self, Table_Name, Values=[]):
        try:
            table_index = self.__Tables_Names.index(Table_Name)
        except ValueError:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in") from None
        if self.__Logged and table_index != -1:
            values = ','.join([str(x) for x in Values])
            if values in self.__Tables_Rows[table_index]:
                raise self.__Exceptions.InvalidRowData("This Row Is Duplicated")
            fields = self.__Tables_Fields[table_index]
            if len(Values) == len(fields):
                row = self.__ROW_QUERY(values)
                self.__Tables_Rows[self.__Tables_Names.index(Table_Name)].append(values)
                table_pos = self.re.search(r'(?x)(?<=\<table_name=\'%s\'\/\>).*?(?=\n\t\<\/table\>)' % Table_Name, self.__DB_FILE, self.re.DOTALL)
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
        try:
            table_index = self.__Tables_Names.index(Table_Name)
        except ValueError:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in") from None
        if self.__Logged and table_index != -1:
            table_fields = self.__Tables_Fields[table_index]
            table_rows = [x.split(',') for x in self.__Tables_Rows[table_index]]
            return (table_fields, table_rows)
        else:
            raise self.__Exceptions.InvalidTable('This Table Doesn\'t exists')
    def get_row(self, Table_Name, searching_by={}):
        try:
            table_index = self.__Tables_Names.index(Table_Name)
        except ValueError:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in") from None
        if self.__Logged and table_index != -1:
            table_fields = self.__Tables_Fields[table_index]
            indeces = [(table_fields.index(x), str(searching_by[x])) for x in searching_by]
            table_rows = self.__Tables_Rows[table_index]
            result = []
            for row in table_rows:
                mrow = row.split(',')
                result += [row] if all([True if mrow[index] == value else False for index, value in indeces]) else []
            return result
        else:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in")
    def delete_table(self, Table_Name):
        try:
            table_index = self.__Tables_Names.index(Table_Name)
        except ValueError:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in") from None
        if self.__Logged and table_index != -1:
            del self.__Tables_Names[table_index]
            del self.__Tables_Rows[table_index]
            del self.__Tables_Fields[table_index]
            with open(self.__DB_FILE_NAME, 'r+') as writer:
                table_pos = self.re.search(r'\<table>\n\t\t\<table_name\=\'%s\'\/\>.*?\n\t\<\/table\>' % Table_Name, self.__DB_FILE, self.re.DOTALL)
                self.Writer(self.__DB_FILE_NAME, row + self.__DB_FILE[table_pos.end():], table_pos.end())
                writer.seek(table_pos.start()-len('\n\t'))
                writer.truncate()
                writer.write(self.__DB_FILE[table_pos.end():])
                self.__DB_FILE = self.__DB_FILE[:table_pos.start()-len('\n\t')] + self.__DB_FILE[table_pos.end():]
        else:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in") from None
    def delete_row(self, Table_Name, searching_by={}):
        try:
            table_index = self.__Tables_Names.index(Table_Name)
        except ValueError:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in") from None
        if self.__Logged and table_index != -1 and searching_by:
            table_fields = self.__Tables_Fields[table_index]
            table_rows = self.__Tables_Rows[table_index]
            indeces = [(table_fields.index(x), str(searching_by[x])) for x in searching_by]
            for row in table_rows:
                mrow = row.split(',')
                if all([True if mrow[index] == value else False for index, value in indeces]):
                    self.__Tables_Rows[table_index].remove(row)
                    with open(self.__DB_FILE_NAME, 'r+') as writer:
                        row_pos = self.re.search(r'\<row %s>' % row, self.__DB_FILE)
                        writer.seek(row_pos.start()- len('\n\t\t'))
                        new_data = self.__DB_FILE[row_pos.end():]
                        writer.truncate()
                        writer.write(new_data)
                        self.__DB_FILE = self.__DB_FILE[:row_pos.start()-len('\n\t\t')]+self.__DB_FILE[row_pos.end():]
        else:
            raise self.__Exceptions.InvalidTable("This table is either doesn't exists or you are not logged in or you Null searcing pointer")
    def create_db(self, DB_FILE, username, password):
        if not self.path.isfile(DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE):
            DB_FILE = DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE
            with open(DB_FILE, 'w+') as DB_writer:
                DB_writer.write('<auth username="%s",password="%s"/>\n<DB></DB>' % (username, password))
                self.chmod(DB_FILE, 777)
        else:
            raise self.__Exceptions.OverwritingExistingDB('You Are Trying To Overwrite existing database')
    def __init__(self, DB_FILE=None, username=None, password=None):
        self.__username, self.__password = username, password
        if DB_FILE:
            self.load(DB_FILE, username, password)

class SQL_PREPROCESSOR:
    def __init__(self, QUERY, DB_MANAGER):
        self.__QUERY = QUERY
        self.__DB_MANAGER = DB_MANAGER
        self.__RESERVED_WORDS = ['SELECT', 'FROM', 'OR', 'AND', '*', 'DISTINCT', 'WHERE']

if __name__ == '__main__':
    from random import choice
    from string import ascii_letters, punctuation
    string_total = (ascii_letters + punctuation)*10
    users = list(set([''.join([choice(string_total) for i in range(7)]) for x in range(20000)]))
    users_DB = DB_MANAGER()
    #users_DB.create_db('FLASK_APP', 'root', 'toor')
    #users_DB.load('FLASK_APP', 'root', 'toor')
    #users_DB.create_row('Users', ['20003', 'Ezz', 'Th3@Professional', None])
    #users_DB.create_table('orders', ['ID', 'Name', 'Comments'])
    #users_DB.create_row('Users', ['20001', 'Nancy', 'Th3@Professional', None])
    #users_DB.create_table('Users', ['ID', 'USERNAME', 'PASSWORD', 'FLAG'])
    #for i in range(len(users)):
    #    users_DB.create_row('Users', [i, users[i], 'Th3@Professional', None])
    #users_DB.create_table('Admin', ['ID', 'USERNAME', 'PASSWORD', 'FLAG'])
    #for i in range(1000):
            #users_DB.create_row('Admin', [i, users[i], 'Th3@Professional', None])
    #orders = ['EZZ', 'SARAH', 'YARA', 'RADAWA', 'ABDELRHMAN', 'SHERIF']
    #for i in range(6):
    #    users_DB.create_row('orders', [i, users[i], 'TPCTROOT'])
