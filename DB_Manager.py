class Small_DB:
    from threading import Thread, Lock
    from os import path, F_OK, access, chmod, R_OK
    from hashlib import md5
    import re
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
            self.__data = data
            self.__file_name = Small_DB.DB_FILENAME
            Small_DB.Thread.__init__(self)
            Small_DB.Thread.start(self)
            Small_DB.Threads_Container.append(self)
        def run(self):
            Small_DB.Thread_Locker.acquire()
            with open(self.__file_name, 'r+') as writer:
                writer.seek(Small_DB.Auth_len + self.__seek[0])
                forward_text = writer.read()
                writer.truncate()
                writer.seek(Small_DB.Auth_len + self.__seek[0])
                encrypted_data = Small_DB.Security.Encrypt(self.__data, Small_DB.chunk_size, Small_DB.Locker)
                encrypted_data = encrypted_data.encode('utf-8').hex()
                writer.write(encrypted_data + forward_text)
                writer.flush()
                Small_DB.DB_FILE = Small_DB.DB_FILE[:self.__seek[1]] + self.__data + Small_DB.DB_FILE[self.__seek[1]:]
                Small_DB.Encrypted_File = Small_DB.Encrypted_File[:self.__seek[0]] + encrypted_data + Small_DB.Encrypted_File[self.__seek[0]:]
            Small_DB.Thread_Locker.release()
    class row_dict(dict):
        def __getattr__(self, item):
            if item in self.keys():
                keys = list(self.keys())
                return self[keys[keys.index(item)]]
            if isinstance(item, int) and item < len(self.keys()):
                return self[self.keys()[item]]
            else:
                raise ValueError('This key Doesn\'t Exists')
    class Delete_Thread(Thread):
        def __init__(self,seek):
            self.__seek = seek
            self.__file_name = Small_DB.DB_FILENAME
            Small_DB.Thread.__init__(self)
            Small_DB.Thread.start(self)
            Small_DB.Threads_Container.append(self)
        def run(self):
            Small_DB.Thread_Locker.acquire()
            with open(self.__file_name, 'rb+') as writer:
                writer.seek(Small_DB.Auth_len + self.__seek[0][0])
                data = writer.read()[self.__seek[0][1]:]
                print(writer.tell())    
                from os import remove
                remove('UsersD.db')
            Small_DB.DB_FILE = Small_DB.DB_FILE[:self.__seek[1][0]] + Small_DB.DB_FILE[self.__seek[1][1]:]
            Small_DB.Encrypted_File = Small_DB.Encrypted_File[:self.__seek[0][0]] + Small_DB.Encrypted_File[self.__seek[0][1]:]
            Small_DB.Thread_Locker.release()
    chunk_size = DB_CHUNK()
    Auth = AUTH()
    Thread_Locker = Lock()
    Threads_Container = ThreadsContainer()
    DB_FILE = DB_FILE()
    DB_FILENAME = DB_FILENAME()
    Locker = LockKey()
    Encrypted_File = EncryptedFile()
    Auth_len = Auth_len()
    __Login_key = 12
    __AUTH_PATTERN = lambda self, username, password: r'(?<=\<auth u\=\")%s(?=\")|(?<=p\=\")%s(?=\")' % (username, password)
    __DB_TABLES_PATTERN = r'(?s)(?<=\<table n\=\").{1,255}?(?=\")|(?<=f\=\[).*?(?=\])|(?<=\]\>).*?(?=\<\/table\>)'
    __DB_TABLE_ROWS_PATTERN = r'(?s)(?<=\<row\>).*?(?=\<\/row\>)'
    __TABLE_QUERY = lambda self, table_name, table_fields: '\n<table n="%s",f=%s></table>' % (table_name, str(table_fields).replace('"', '').replace("'", '').replace(' ', ''))
    __ROW_QUERY = lambda self, values: "\r\n<row>%s</row>" % values
    __Tables_Names, __Tables_Fields, __Tables_Rows = list(), list(), list()
    __Logged = False
    @classmethod
    def create_db(self, DB_FILE, username, password):
        if not self.path.isfile(DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE):
            DB_FILE = DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE
            username = self.md5(username.encode('utf-8')).hexdigest()
            password = self.md5(password.encode('utf-8')).hexdigest()
            key = sum([int(x, 16) for x in password])
            auth = self.Security.Encrypt('<auth u="%s",p="%s"/>' % (username, password), 9, Small_DB.__Login_key).encode('utf-8')
            with open(DB_FILE, 'wb+') as DB_writer:
                DB_writer.write(auth.hex().encode('utf-8'))
            self.chmod(DB_FILE, 777)
        else:
            raise self.DBException('You Are Trying To Overwrite Existing DB')
    def Load(self, DB_FILE, username, password):
        self.chunk_size = 9
        DB_FILE = DB_FILE if DB_FILE.endswith('.db') else '%s.db' % DB_FILE
        if self.access(DB_FILE, self.F_OK | self.R_OK):
            with open(DB_FILE, 'r+') as db_reader:
                username, password = str(username), str(password)
                username = self.md5(username.encode('utf-8')).hexdigest()
                password = self.md5(password.encode('utf-8')).hexdigest()
                db_reader.seek(0)
                data = bytes.fromhex(db_reader.read(84*2)).decode()
                self.Auth_len = 84*2
                self.Auth = self.Security.Decrypt(data, self.chunk_size, Small_DB.__Login_key)
                Auth_Pattern = self.__AUTH_PATTERN(username, password)
                Auth = self.re.findall(Auth_Pattern, self.Auth)
                if len(Auth) != 2:
                    raise self.DBException('Auth Error: wrong username or password')
                self.Locker = sum([int(x, 16) for x in password])
                self.Encrypted_File = db_reader.read()
                self.DB_FILE = self.Security.Decrypt(bytes.fromhex(self.Encrypted_File).decode(), self.chunk_size, self.Locker)
                self.DB_FILENAME = DB_FILE
                self.__Logged = True
                tables = self.re.compile(self.__DB_TABLES_PATTERN).findall(self.DB_FILE)
                for i in range(0, len(tables), 3):
                    x = tuple(tables[i:i+3])
                    fields = x[1].split(',')
                    self.__Tables_Names.append(x[0])
                    self.__Tables_Fields.append(x[1].split(','))
                    self.__Tables_Rows.append([])
                    data = x[2].replace('</table>', '')
                    for row in data.split('\n'):
                        row = self.re.sub(r'<row>(.*)</row>', r'\1', row).replace('\r', '').split(',')
                        if all(row):
                            row = self.row_dict(zip(fields, row))
                            self.__Tables_Rows[-1].append(row)
        else:
            raise self.DBException('This DB Doesn\'t Exists')
    def create_table(self, Table_Name, Fields=[]):
        if self.__Logged and self.DB_FILE is not None:
            Fields = Fields if all(Fields) else None
            if 0 < len(str(Table_Name).replace(' ', '')) < 256 and Table_Name not in self.__Tables_Names and Fields is not None:
                table_query = self.__TABLE_QUERY(Table_Name, Fields)
                self.__Tables_Names.append(Table_Name)
                self.__Tables_Fields.append(Fields)
                self.__Tables_Rows.append([])
                thread = self.ThreadWriter(table_query, (len(self.Encrypted_File), len(self.DB_FILE)))
                [thread.join() for thread in self.Threads_Container]
            else:
                raise self.DBException('Either Empty Table Name or This Table Exists or Table Fileds Is Empty or Table Name length > 255')
        else:
            raise self.DBException('Please Login or Load DB')
    def create_row(self, Table_Name, Values=[]):
        table_index = self.__Tables_Names.index(Table_Name)
        table_fields = self.__Tables_Fields[table_index] if table_index >= 0 else None
        if self.__Logged and table_index != -1 and table_fields is not None and len(Values) == len(table_fields):
            values = ','.join([str(x) for x in Values])
            if self.row_dict(zip(table_fields, Values)) in self.__Tables_Rows[table_index]:
                raise self.DBException('This Row Is Duplicated')
            row = self.__ROW_QUERY(values)
            pattern = '(?s)<table n="%s",f=%s>.*?(?=\<\/table\>)' % (Table_Name, str(table_fields).replace(' ', '').replace(']', '\]').replace('[', '\[').replace("'", ''))
            table_pos = self.re.search(pattern, self.DB_FILE, self.re.DOTALL)
            dec_pos = table_pos.end()
            encrypted_pattern = self.Security.Encrypt(table_pos.group(), 9, self.Locker).encode().hex()
            table_pos = self.re.search('(?s)%s' % encrypted_pattern, self.Encrypted_File, self.re.DOTALL | self.re.MULTILINE)
            enc_pos = table_pos.end()
            thread = self.ThreadWriter(row, (enc_pos, dec_pos))
            [thread.join() for thread in self.Threads_Container]
            self.__Tables_Rows[table_index].append(Values)
        else:
            raise self.DBException('Either Invalid Auth or Wrong Table Name or Wrong Values')
    def get_table(self, Table_Name):
        table_index = self.__Tables_Names.index(Table_Name)
        if self.__Logged and table_index != -1:
           table_fields = self.__Tables_Fields[table_index]
           table_rows = [x.split(',') for x in self.__Tables_Rows[table_index]]
           return (table_fields, table_rows)
        else:
           raise self.DBException('This Table Doesn\'t exists')
    def get_row(self, Table_Name, search):
        if isinstance(search, dict):
            table_index = self.__Tables_Names.index(Table_Name)
            if self.__Logged and table_index != -1:
                table_fields = self.__Tables_Fields[table_index]
                indeces = [(x, str(search[x])) for x in search]
                table_rows = self.__Tables_Rows[table_index]
                result = []
                for row in table_rows:
                    result += [row] if all([True if row[index] == value else False for index, value in indeces]) else []
                return result
            else:
                raise self.__Exceptions.InvalidTable("This table is either doesn't exists or You are not logged in")
        else:
            self.DBException('Cann\'t Search Using These Search')
    def delete_table(self, Table_Name):
        table_index = self.__Tables_Names.index(Table_Name)
        if self.__Logged and table_index != -1:
            del self.__Tables_Rows[table_index]
            pattern = '(?s)<table n="%s",f=%s>.*?\<\/table\>' % (Table_Name, str(self.__Tables_Fields[table_index]).replace(' ', '').replace(']', '\]').replace('[', '\[').replace("'", ''))
            del self.__Tables_Fields[table_index]
            del self.__Tables_Names[table_index]
            table_pos = self.re.search(pattern, self.DB_FILE, self.re.DOTALL)
            dec_pos = table_pos.start(), table_pos.end()
            encrypted_pattern = self.Security.Encrypt(table_pos.group(), 9, self.Locker).encode().hex()
            table_pos = self.re.search('(?s)%s' % encrypted_pattern, self.Encrypted_File, self.re.DOTALL | self.re.MULTILINE)
            enc_pos = table_pos.start(), table_pos.end()
            self.Delete_Thread((enc_pos, dec_pos))
        else:
            raise self.DBException("This table is either doesn't exists or You are not logged in") from None

users = Small_DB()
from string import ascii_letters
from random import choice
from os import remove
#users_ = [''.join([choice(ascii_letters) for i in range(15)]) for i in range(100)]
#users_ = list(set(users_))
users.create_db('UsersD.db', 'root', 'toor')
users.Load('UsersD.db', 'root', 'toor')
users.create_table('Admin', ['ID', 'NAME'])
users.create_table('face', ['ID', 'NAME'])
users.delete_table('Admin')
'''
users.create_row('face', ['Iasd', "aad"])
users.create_table('Users', ['Username', 'Password'])
users.create_row('Users', ['Mohamed', 'Th3@Professional'])
users.create_table('Admin', ['Username', 'Password'])
users.create_row('Admin', ['I love', 'Nancy'])
'''
#print(users.DB_FILE)
#users.delete_table('Users')

#users.create_table('Users', ['ID', 'Name'])
#for i in range(len(users_)):
#    print(i)
#    users.create_row('Users', [i, users_[i]])
#users.create_row('face', ['ID', "NAmae"])
#users.create_table('admin', ['ID', 'NAME'])
#users.create_table('Aimo', ['ID', 'NAME'])

from threading import Thread, Lock
Thread_Locker = Lock()
class ThreadWriter(Thread):
    def __init__(self, File_Name, Text, Seeking):
        self.__File_Name = File_Name
        self.__Text = Text
        self.__Seeking = Seeking
        Thread.__init__(self)
        Thread.start(self)

    def run(self):
        with open(self.__File_Name, 'r+') as writer:
            writer.seek(self.__Seeking)
            forward_text = writer.read()
            writer.truncate()
            writer.seek(self.__Seeking)
            writer.write(self.__Text + forward_text)
            writer.flush()
