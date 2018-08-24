class DB_MANAGER:
    from threading import Thread, Lock
    from os import path, F_OK, access, chmod, R_OK
    from hashlib import md5
    import textwrap
    import re
    class Auto_Append:
        Auto_append = False
        def __get__(self, instance, owner):
            return self.__Auto_append
        def __set__(self, instance, value):
            self.__Auto_append = value
    class Security:
        @staticmethod
        def Encrypt(data, chunk_size, key):
            return ''.join([chr(ord(x) + key) if x != '\n' else x for x in data])
        @staticmethod
        def Decrypt(data, chunk_size, key):
            return ''.join([chr(abs(ord(x) - key)) if x != '\n' else x for x in data])
    class DB_CHUNK:
        __chunk_size = None
        def __get__(self, instance, owner):
            return self.__chunk_size
        def __set__(self, instance, value):
            self.__chunk_size = value
    class DB_FILE:
        __file = None
        def __get__(self, instance, owner):
            return self.__file
        def __set__(self, instance, value):
            self.__file = value
    class DB_FILENAME:
        __file_name = None
        def __get__(self, instance, owner):
            return self.__file_name
        def __set__(self, instance, value):
            self.__file_name = value
    class Tables:
        __Tables = None
        def __get__(self, instance, owner):
            return self.__Tables
        def __set__(self, instance, value):
            self.__Tables = value
    class Auto_erase:
        __value = False
        def __get__(self, instance, owner):
            return self.__value
        def __set__(self, instance, value):
            self.__value = value
    class ThreadsContainer:
        __threads_container = []
        def __get__(self, instance, owner):
            return self.__threads_container
        def __set__(self, instance, value):
            self.__threads_container = value
    class list(list):

        def index(self, i):
            return list.index(self, i) if i in self else -1
    class LockKey:
        __Lock_key = None
        def __get__(self, instance, owner):
            return self.__Lock_key
        def __set__(self, instance, value):
            self.__Lock_key = value
    class AUTH:
        __Auth = None
        def __get__(self, instance, owner):
            return self.__Auth
        def __set__(self, instance, value):
            self.__Auth = value
    class DBException(Exception):
        __module__ = Exception.__module__
    class Auth_len:
        __len = None
        def __get__(self, instance, owner):
            return self.__len
        def __set__(self, instance, value):
            self.__len = value
    class EncryptedFile:
        __file = None
        def __get__(self, instance, owner):
            return self.__file
        def __set__(self, instance, value):
            self.__file = value
    class ThreadWriter(Thread):
        def __init__(self, data, seek):
            self.__seek = seek
            self.__data = DB_MANAGER.re.sub(r'(?s)\<class(.*?)\>', r'\1', data)
            self.__file_name = DB_MANAGER.DB_FILENAME
            DB_MANAGER.Thread.__init__(self)
            DB_MANAGER.Thread.start(self)
            DB_MANAGER.Threads_Container.append(self)
        def run(self):
            DB_MANAGER.Thread_Locker.acquire()
            encrypted_data = DB_MANAGER.Security.Encrypt(self.__data, DB_MANAGER.chunk_size, DB_MANAGER.Locker)
            encrypted_data = encrypted_data.encode('utf-8').hex()
            if DB_MANAGER.Auto_append:
                with open(self.__file_name, 'r+') as writer:
                    writer.seek(DB_MANAGER.Auth_len + self.__seek[0])
                    forward_text = writer.read()
                    writer.truncate()
                    writer.seek(DB_MANAGER.Auth_len + self.__seek[0])
                    data = encrypted_data + forward_text
                    writer.write(data.replace('0a', '\n'))
                    writer.flush()
            DB_MANAGER.DB_FILE = DB_MANAGER.DB_FILE[:self.__seek[1]] + self.__data + DB_MANAGER.DB_FILE[self.__seek[1]:]
            DB_MANAGER.Encrypted_File = DB_MANAGER.Encrypted_File[:self.__seek[0]] + encrypted_data + DB_MANAGER.Encrypted_File[self.__seek[0]:]
            DB_MANAGER.Thread_Locker.release()
            [(thread.join(), DB_MANAGER.Threads_Container.remove(thread)) if thread != self else exit(0) for thread in DB_MANAGER.Threads_Container]
            exit(0)
    class row_dict(dict):
        def __getitem__(self, item):
            keys = list(self.keys())
            if isinstance(item, int) and item < len(self.keys()):
                return self[keys[item]]
            elif item in keys:
                return dict.get(self, item)
            else:
                raise ValueError('This key Doesn\'t Exists')
        def __getattr__(self, item):
            keys = list(self.keys())
            if item in self.keys():
                return self[keys[keys.index(item)]]
            else:
                raise ValueError('This key Doesn\'t Exists')
    class Delete_Thread(Thread):
        def __init__(self,seek):
            self.__seek = seek
            self.__file_name = DB_MANAGER.DB_FILENAME
            DB_MANAGER.Thread.__init__(self)
            DB_MANAGER.Thread.start(self)
            DB_MANAGER.Threads_Container.append(self)

        def run(self):
            DB_MANAGER.Thread_Locker.acquire()
            if DB_MANAGER.Auto_append:
                with open(self.__file_name, 'rb+') as writer:
                    writer.seek(DB_MANAGER.Auth_len + self.__seek[0][0])
                    data = writer.read()[self.__seek[0][1]:]
                    writer.truncate(DB_MANAGER.Auth_len + self.__seek[0][0])
                    writer.seek(DB_MANAGER.Auth_len + self.__seek[0][0])
                    writer.write(data)
            DB_MANAGER.DB_FILE = DB_MANAGER.DB_FILE[:self.__seek[1][0]] + DB_MANAGER.DB_FILE[self.__seek[1][1]:]
            DB_MANAGER.Encrypted_File = DB_MANAGER.Encrypted_File[:self.__seek[0][0]] + DB_MANAGER.Encrypted_File[self.__seek[0][1]:]
            DB_MANAGER.Thread_Locker.release()
            [(thread.join(), DB_MANAGER.Threads_Container.remove(thread)) if thread != self else exit(0) for thread in DB_MANAGER.Threads_Container]
            exit(0)
    class DB_Thread(Thread):
        def __init__(self, method, *args, **kwargs):
            self.__args = args
            self.__method = method
            self.__kw_args = kwargs
            DB_MANAGER.Thread.__init__(self)
            DB_MANAGER.Thread.start(self)
            DB_MANAGER.Threads_Container.append(self)

        def run(self):
            DB_MANAGER.Main_Thread_Locker.acquire(1)
            self.__method(*self.__args, **self.__kw_args)
            [(thread.join(), DB_MANAGER.Threads_Container.remove(thread)) if thread != self else None for thread in
             DB_MANAGER.Threads_Container]
            DB_MANAGER.Main_Thread_Locker.release()
            exit(0)
    class Dumping_Thread(Thread):
        def __init__(self):
            DB_MANAGER.Thread.__init__(self)
            DB_MANAGER.Thread.start(self)
        def run(self):
            DB_MANAGER.Thread_Locker.acquire()
            if DB_MANAGER.Auto_erase:
                Main_Data = ''
                for table_index, table_name in enumerate(DB_MANAGER.Tables_Names):
                    Table_Fields = DB_MANAGER.Tables_Fields[table_index]
                    table_query = DB_MANAGER.TABLE_QUERY(table_name, Table_Fields)
                    Main_Data += table_query.replace('</table>', '')
                    for row in DB_MANAGER.Tables_Rows[table_index]:
                        Main_Data += DB_MANAGER.ROW_QUERY(row)
                    Main_Data += '\n</table>'
                Main_Data = DB_MANAGER.Security.Encrypt(Main_Data, 9, DB_MANAGER.Locker)
                Main_Data = Main_Data.encode('utf-8').hex()
                DB_MANAGER.DB_FILE = Main_Data
                DB_MANAGER.Encrypted_File = Main_Data
            else:
                Main_Data = DB_MANAGER.Encrypted_File
            with open(DB_MANAGER.DB_FILENAME, 'r+') as writer:
                writer.seek(DB_MANAGER.Auth_len)
                new_data = writer.read()
                if new_data != Main_Data:
                    writer.truncate(DB_MANAGER.Auth_len)
                    writer.seek(DB_MANAGER.Auth_len)
                    writer.write(Main_Data.replace('0a', '\n'))
                    writer.flush()
            DB_MANAGER.Thread_Locker.release()
            [(thread.join(), DB_MANAGER.Threads_Container.remove(thread)) if thread != self else exit(0) for thread in DB_MANAGER.Threads_Container]
            exit(0)
    Tables_Names, Tables_Fields, Tables_Rows = list(), list(), list()
    chunk_size = DB_CHUNK()
    Auth = AUTH()
    Thread_Locker = Lock()
    Threads_Container = ThreadsContainer()
    DB_FILE = DB_FILE()
    DB_FILENAME = DB_FILENAME()
    Auto_erase = Auto_erase()
    Locker = LockKey()
    Encrypted_File = EncryptedFile()
    Auth_len = Auth_len()
    Main_Thread_Locker = Lock()
    Auto_append = Auto_Append()
    ROW_QUERY = classmethod(lambda self, values: "\n\t<row>%s</row>" % values)
    TABLE_QUERY = classmethod(lambda self, table_name, table_fields: '\n<table n="%s",f=[%s]></table>' % (table_name, ','.join(table_fields)))
    __Login_key = 12
    __AUTH_PATTERN = lambda self, username, password: r'(?<=\<auth u\=\")%s(?=\")|(?<=p\=\")%s(?=\")' % (username, password)
    __DB_TABLES_PATTERN = r'(?s)(?<=\<table n\=\").{1,255}?(?=\")|(?<=f\=\[).*?(?=\])|(?<=\]\>).*?(?=\<\/table\>)'
    __DB_TABLE_ROWS_PATTERN = r'(?s)(?<=\<row\>).*?(?=\<\/row\>)'
    __allowed_types = [str, int, float, dict, list, set]
    __Logged = False
    @classmethod
    def create_db(self, DB_FILE, username, password):
        if not self.path.isfile(DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE):
            DB_FILE = DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE
            username = self.md5(username.encode('utf-8')).hexdigest()
            password = self.md5(password.encode('utf-8')).hexdigest()
            key = sum([int(x, 16) for x in password])
            auth = self.Security.Encrypt('<auth u="%s",p="%s"/>' % (username, password), 9, DB_MANAGER.__Login_key).encode('utf-8')
            with open(DB_FILE, 'wb+') as DB_writer:
                DB_writer.write(auth.hex().encode('utf-8'))
            self.chmod(DB_FILE, 777)
        else:
            raise self.DBException('You Are Trying To Overwrite Existing DB')
    def __init__(self, DB_FILE=None, username=None, password=None, Auto_Append=False, Auto_erase=False):
        if DB_FILE:
            self.Load(DB_FILE, username, password, Auto_Append, Auto_erase)
    def Load(self, DB_FILE, username, password, Auto_Append=False, Auto_erase=False):
        self.chunk_size = 9
        self.Auto_append = Auto_Append
        DB_FILE = DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE
        self.Auto_erase = Auto_erase
        if DB_FILE == ':Memory:':
            self.DB_FILENAME = ':Memory:'
            return None
        if self.access(DB_FILE, self.F_OK | self.R_OK):
            with open(DB_FILE, 'r+') as db_reader:
                username, password = str(username), str(password)
                username = self.md5(username.encode('utf-8')).hexdigest()
                password = self.md5(password.encode('utf-8')).hexdigest()
                db_reader.seek(0)
                data = bytes.fromhex(db_reader.read(84*2)).decode()
                self.Auth_len = 84*2
                self.Auth = self.Security.Decrypt(data, self.chunk_size, DB_MANAGER.__Login_key)
                Auth_Pattern = self.__AUTH_PATTERN(username, password)
                Auth = self.re.findall(Auth_Pattern, self.Auth)
                if len(Auth) != 2:
                    raise self.DBException('Auth Error: wrong username or password')
                self.__Logged = True
                self.Locker = sum([int(x, 16) for x in password])
                self.DB_FILENAME = DB_FILE
                if not self.Auto_erase:
                    self.Encrypted_File = db_reader.read().replace('\n', '0a')
                    self.DB_FILE = self.Security.Decrypt(bytes.fromhex(self.Encrypted_File).decode(), self.chunk_size,
                                                         self.Locker)
                    tables = self.re.compile(self.__DB_TABLES_PATTERN).findall(self.DB_FILE)
                    for i in range(0, len(tables), 3):
                        x = tuple(tables[i:i + 3])
                        self.Tables_Names.append(x[0])
                        new_tables = [q.split(':')[0] for q in x[1].split(',')]
                        self.Tables_Fields.append(x[1].split(','))
                        self.Tables_Rows.append([])
                        data = x[2].replace('</table>', '')
                        for row in data.split('\n'):
                            row = self.re.sub(r'<row>(.*)</row>', r'\1', row).replace('\t', '').split(',')
                            if all(row):
                                row = self.row_dict(zip(new_tables, row))
                                self.Tables_Rows[-1].append(row)
        else:
            raise self.DBException('This DB Doesn\'t Exists')
    def create_table(self, Table_Name, Fields={}):
        if self.__Logged is not None and isinstance(Fields, dict):
            Fields = self.row_dict(Fields) if all(Fields.keys()) and all([True if x in self.__allowed_types else False for x in Fields.values()]) and len(Fields) > 0 else None
            if 0 < len(str(Table_Name).replace(' ', '')) < 256 and Table_Name not in self.Tables_Names and Fields is not None:
                Fields = str(Fields).replace('{', '').replace('}', '').replace('<class', '').replace(' ', '').replace('>', '').replace('\'', '').split(',')
                table_query = self.TABLE_QUERY(Table_Name, Fields)
                self.Tables_Names.append(Table_Name)
                self.Tables_Fields.append(Fields)
                self.Tables_Rows.append([])
                if not self.Auto_erase:
                    self.ThreadWriter(table_query, (len(self.Encrypted_File), len(self.DB_FILE)))
                [(thread.join(), DB_MANAGER.Threads_Container.remove(thread)) for thread in
                 DB_MANAGER.Threads_Container]
            else:
                raise self.DBException('Either Empty Table Name or This Table Exists or Table Fileds Is Empty or Table Name length > 255')
        else:
            raise self.DBException('Please Login or Load DB')
    def create_row(self, Table_Name, Values=[]):
        Table_Name = Table_Name[0] if isinstance(Table_Name, (list, tuple)) else Table_Name
        table_index = self.Tables_Names.index(Table_Name)
        table_fields = self.Tables_Fields[table_index] if table_index >= 0 else None
        checker = all([str(type(x)).replace('<class ', '').replace('\'', '').replace('>', '') == table_fields[i].split(':')[1] for i, x in enumerate(Values)]) if not table_fields is None else None
        if self.__Logged and table_index != -1 and table_fields is not None and len(Values) == len(table_fields) and checker:
            values = ','.join([str(x) for x in Values])
            temp = [x.split(':')[0] for x in table_fields]
            if self.row_dict(zip(temp, Values)) in self.Tables_Rows[table_index]:
                raise self.DBException('This Row Is Duplicated')
            if not self.Auto_erase:
                row = self.ROW_QUERY(values)
                while not self.DB_FILE:
                    pass
                pattern = r'(?s)\<table n\=\"%s\",f\=\[%s\]\>.*?(?=\<\/table\>)' % (
                    Table_Name, ','.join([x for x in table_fields]))
                table_pos = self.re.search(pattern, self.DB_FILE, self.re.DOTALL)
                dec_pos = table_pos.end()
                encrypted_pattern = self.Security.Encrypt(table_pos.group(), 9, self.Locker).encode().hex()
                while self.Thread_Locker.locked():
                    pass
                enc_pos = self.Encrypted_File.index(encrypted_pattern) + len(encrypted_pattern)
                '''try:
                except:
                    table_pos = self.re.search('(?s)%s' % encrypted_pattern, self.Encrypted_File, self.re.DOTALL|self.re.MULTILINE)
                    enc_pos = table_pos.end()'''
                self.ThreadWriter(row, (enc_pos, dec_pos))
            self.Tables_Rows[table_index].append(dict(zip([x.split(':')[0] for x in table_fields], Values)))
            [(thread.join(), DB_MANAGER.Threads_Container.remove(thread)) for thread in DB_MANAGER.Threads_Container]
        else:
            raise self.DBException('Either Invalid Auth or Wrong Table Name or Wrong Values')
    def get_table(self, Table_Name):
        table_index = self.Tables_Names.index(Table_Name)
        if self.__Logged and table_index != -1:
           table_fields = self.Tables_Fields[table_index]
           table_rows = [x for x in self.Tables_Rows[table_index]]
           return (table_fields, table_rows)
        else:
           raise self.DBException('This Table Doesn\'t exists')
    def get_row(self, Table_Name, search):
        if isinstance(search, dict):
            table_index = self.Tables_Names.index(Table_Name)
            if self.__Logged and table_index != -1:
                indeces = [(x, str(search[x])) for x in search]
                table_rows = self.Tables_Rows[table_index]
                result = []
                for row in table_rows:
                    result += [row] if all([True if row[index] == value else False for index, value in indeces]) else []
                if not result:
                    return None
                return result[0]
            else:
                raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in")
        else:
            self.DBException('Cann\'t Search Using These Search')
    def delete_table(self, Table_Name):
        table_index = self.Tables_Names.index(Table_Name)
        table_fields = self.Tables_Fields[table_index] if table_index >= 0 else None
        if self.__Logged and table_index != -1:
            del self.Tables_Rows[table_index]
            if not self.Auto_erase:
                pattern = '(?s)\n<table n="%s",f=%s>.*?\<\/table\>' % (Table_Name, str(
                    dict([x.split(':') for x in table_fields])).replace('{', r'\[').replace('}', r'\]').replace(' ',
                                                                                                                '').replace(
                    '"', '').replace('\'', ''))
                while not self.DB_FILE:
                    pass
                table_pos = self.re.search(pattern, self.DB_FILE, self.re.DOTALL)
                dec_pos = table_pos.start(), table_pos.end()
                encrypted_pattern = self.Security.Encrypt(table_pos.group(), 9, self.Locker).encode().hex()
                '''
                table_pos = self.re.search('(?s)%s' % encrypted_pattern, self.Encrypted_File, self.re.DOTALL | self.re.MULTILINE)
                '''
                enc_pos_start = self.Encrypted_File.index(encrypted_pattern)
                enc_pos = enc_pos_start, enc_pos_start + len(encrypted_pattern)
                self.Delete_Thread((enc_pos, dec_pos))
            del self.Tables_Fields[table_index]
            del self.Tables_Names[table_index]
        else:
            raise self.DBException("This table is either doesn't exists or You are not logged in")
    def delete_row(self, Table_Name, search):
        table_index = self.Tables_Names.index(Table_Name)
        if self.__Logged and table_index >= 0 and isinstance(search, dict):
            table_rows = self.Tables_Rows[table_index]
            indeces = [(x, str(search[x])) for x in search]
            main_row = None
            for main_index, row in enumerate(table_rows):
                if all([True if row[index] == value else False for index, value in indeces]):
                    main_row = row
                    del self.Tables_Rows[table_index][main_index]
                    break
            if main_row is not None:
                if not self.Auto_erase:
                    pattern = '(?s)\n\t\<row\>%s\<\/row\>' % ','.join(list(main_row.values()))
                    table_pos = self.re.search(pattern, self.DB_FILE, self.re.DOTALL)
                    while not self.DB_FILE:
                        pass
                    dec_pos = table_pos.start(), table_pos.end()
                    encrypted_pattern = self.Security.Encrypt(table_pos.group(), 9, self.Locker).encode().hex()
                    table_pos = self.re.search('(?s)%s' % encrypted_pattern, self.Encrypted_File,
                                               self.re.DOTALL | self.re.MULTILINE)
                    enc_pos = table_pos.start(), table_pos.end()
                    self.Delete_Thread((enc_pos, dec_pos))
            else:
                raise self.DBException('This Row Cann\'t Be Found')
        else:
            raise self.DBException('Either Not Logged or Wrong Searching Data or Table Name Doesn\'nt exists')
    def dump(self):
        if self.DB_FILENAME != ':Memory:':
            self.Dumping_Thread()
        else:
            raise self.DBException('Cannot Dump In Memory Mode')
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.Auto_append:
            self.dump()
    db_tables = property(lambda self: self.Tables_Names)
    db_table_rows = property(lambda self: self.Tables_Rows)
    db_table_fields = property(lambda self: self.Tables_Fields)
