import json
import pickle
import os
import pandas as pd
# third party module not by me:
import bsddb3.db as db

# For more info on bsddb3 see here: http://pybsddb.sourceforge.net/bsddb3.html

DBS = []
env = db.DBEnv()


# this prevents lockers/locks from accumulating when python is closed
# normally, but does not prevent this when we C-c out of the server.
def close_dbs():
    """Closes the DB's when the system is closed with C-c"""
    for dbToClose in DBS:
        dbToClose.close()


def open_dbs():
    for dbToOpen in DBS:
        dbToOpen.setDB()


def getEnvTxn():
    if env is None:
        raise EnvNotCreatedException
    else:
        return env.txn_begin()


class EnvNotCreatedException(Exception):
    pass


class DBNeverOpenedException(Exception):
    pass


class DB(type):
    """Metaclass for Resource objects"""

    notFound = db.DB_NOTFOUND

    db = None

    def __init__(cls, name, bases, dct):
        """Called when Resource and each subclass is defined"""
        super().__init__(name, bases, dct)
        if "keys" in dir(cls):
            cls.filename = name
            DBS.append(cls)
            cls.setDB()

    def setDB(cls):
        if env is None:
            raise EnvNotCreatedException
        cls.db = db.DB(env)
        cls.db.open(cls.filename, None, cls.DBTYPE,
                    db.DB_AUTO_COMMIT |
                    db.DB_THREAD |
                    db.DB_CREATE)

    def close(cls):
        if cls.db is None:
            raise DBNeverOpenedException
        cls.db.close()

    def getCursor(cls, txn=None, readCommited=False, bulk=False):

        if cls.db is None:
            raise DBNeverOpenedException

        flags = 0

        if readCommited:
            if flags != 0:
                flags = flags | db.DB_READ_COMMITTED
            else:
                flags = db.DB_READ_COMMITTED

        if bulk:
            if flags != 0:
                flags = flags | db.DB_CURSOR_BULK
            else:
                flags = db.DB_CURSOR_BULK

        return Cursor(cls.db.cursor(txn=txn, flags=flags))

    def syncDb(cls):
        if cls.db is None:
            raise DBNeverOpenedException
        cls.db.sync()


class Cursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def get(self, flags=0):
        returnVal = self.cursor.get(flags=flags)
        if returnVal is None:
            return None
        else:
            key, value = returnVal
            return from_string(key), from_string(value)

    def getWithKey(self, key, flags=db.DB_SET):
        key = tupleToKey(tupleToStrings(key))
        out = self.cursor.get(key, flags=flags)
        if out is None:
            return None
        else:
            key, value = out
            return from_string(key), from_string(value)

    def put(self, key, value, flags=db.DB_CURRENT):
        key = tupleToKey(key)
        self.cursor.put(key, to_string(value), flags=flags)

    def next(self, flags=0):
        output = self.cursor.next(flags=flags)
        if output is None:
            return None
        key, value = output

        return from_string(key), from_string(value)

    def dup(self, flags=db.DB_POSITION):
        return Cursor(self.cursor.dup(flags))

    def close(self):
        return self.cursor.close()

    def current(self, flags=0):
        output = self.cursor.current(flags=flags)
        if output is None:
            return None
        key, value = output

        try:
            return from_string(key), from_string(value)
        except TypeError:
            return key, value



def tupleToKey(tupleToConvert):
    return to_string(" ".join([str(x) for x in tupleToConvert]))


def tupleToStrings(tupleToStr):
    return [str(a) for a in tupleToStr]


class Resource(metaclass=DB):
    """Base class for bsddb3 files"""
    DBTYPE = db.DB_BTREE

    @classmethod
    def length(cls):
        return len(cls.db.keys())

    @classmethod
    def all(cls, txn=None):
        return [cls(*tup).get(txn=txn) for tup in cls.db_key_tuples()]

    @classmethod
    def db_keys(cls):
        return [from_string(k) for k in cls.db.keys()]

    @classmethod
    def db_key_tuples(cls):
        return [k.split(" ") for k in cls.db_keys()]

    @classmethod
    def keysWhichMatch(cls, *args):
        """Get all keys matching the passed values"""
        lenArgs = len(args)
        if len(cls.keys) < lenArgs:
            raise ValueError('Number of keys provided is too long.\n'
                             'Len Class Keys: %s\n'
                             'Len Provided Keys: %s\n' % (len(cls.keys), len(args)))

        if lenArgs <= 0:
            raise ValueError('Number of keys provided is too short.\n'
                             'Len Class Keys: %s\n'
                             'Len Provided Keys: %s\n' % (len(cls.keys), len(args)))

        index = 0
        output = cls.db_key_tuples()

        for keyToCheck in args:
            temp = []
            for key in output:
                if key[index] == keyToCheck:
                    temp.append(key)

            index += 1
            output = temp

        return output

    def rename(self, **kwargs):
        """Read data for this key, delete that db entry, and save it under another key"""
        for k in kwargs:
            if k not in self.keys:
                raise ValueError(
                    "names of arguments must be db keys: " +
                    ", ".join([str(x) for x in self.keys]))
        data_dict = self.get()
        self.put(None)
        self.info.update(kwargs)
        self.values = tuple(self.info[k] for k in self.keys)
        self.set_db_key()
        self.put(data_dict)

    @classmethod
    def rename_all(cls, find, replace):
        """Call rename for all entries in this DB

        find is a dictionary used to search for entries in this DB;
        entry.rename(**replace) will be called for each of the entries
        found.

        """
        entry_list = []
        all_entries = cls.db_key_tuples()
        for tup in all_entries:
            entry = cls(*tup)
            match_list = [entry.info[k] == v for k, v in find.iteritems()]
            if all(match_list):
                entry_list.append(entry)
        print("%s %4d / %4d %s." % (
            cls.__name__,
            len(entry_list),
            len(all_entries),
            "entry matches" if len(entry_list) == 1 else "entries match"))
        for i, entry in enumerate(entry_list):
            old_db_key = entry.db_key
            entry.rename(**replace)
            print("%s %4d / %4d '%s' -> '%s'" % (
                cls.__name__,
                i + 1,
                len(entry_list),
                old_db_key,
                entry.db_key))

    @classmethod
    def has_key(cls, k):
        return to_string(k) in cls.db

    def __init__(self, *args):
        if len(args) != len(self.keys):
            raise ValueError(
                "should have exactly %d args: %s" % (
                    len(self.keys),
                    ", ".join([from_string(x) for x in self.keys]),
                ))
        self.values = tupleToStrings(args)
        for a in self.values:
            if " " in a:
                raise ValueError("values should have no spaces", self.values)
        self.info = dict(zip(self.keys, self.values))
        self.set_db_key()

    def set_db_key(self):
        self.db_key = tupleToKey(self.values)

    def alter(self, fun, txn=None):
        """Apply fun to current value and then save it."""
        before = self.get(txn=txn, write=True)
        after = fun(before)
        self.put(after, txn=txn)
        return after

    def get(self, txn=None, write=False):
        """Get method for resource, and its subclasses"""
        try:
            if self.db_key not in self.db:
                return self.make(txn)
        except AttributeError:
            txn.abort()
            raise DBNeverOpenedException
        flags = 0
        if write:
            flags = db.DB_RMW
        return from_string(self.db.get(self.db_key, txn=txn, flags=flags))

    def make(self, txn=None):
        """Make function for when object doesn't exist

        Override functionality by adding a make_details function to your subclass"""
        try:
            made = self.make_details()
        except AttributeError:
            return None
        self.put(made, txn)
        return made

    def put(self, value, txn=None):
        """Put method for resource, and its subclasses"""
        if value is None:
            if self.db_key in self.db:
                self.db.delete(self.db_key, txn=txn)
        else:
            self.db.put(self.db_key, to_string(value), txn=txn)

    def __repr__(self):
        return '%s("%s")' % (self.__class__.__name__, from_string(self.db_key))

    @classmethod
    def doBackup(cls, path, *args):
        dbPath = os.path.join(path, cls.filename)
        try:
            os.makedirs(dbPath)
        except OSError:
            return False

        for keys in cls.db_key_tuples():
            output = cls(*tuple(keys)).dbBackup(dbPath, args)
            if not output:
                print(keys)

    @classmethod
    def doRestore(cls, path, *args):
        dbPath = os.path.join(path, cls.filename)
        if os.path.exists(dbPath):
            keys = cls.getKeysFromFolders(dbPath, len(cls.keys))
            for key in keys:
                restore = cls(*tuple(key))

                restorePath = os.path.join(dbPath, *tuple(key[:-1]))
                file = key[-1] + '.backup'

                restoreFilePath = os.path.join(restorePath, file)

                if os.path.exists(restoreFilePath):
                    try:
                        storable = restore.fileToStorable(restoreFilePath)
                    except pd.errors.EmptyDataError:
                        continue

                    restore.put(storable)
                else:
                    raise Exception
        else:
            raise Exception

        return True

    @classmethod
    def getKeysFromFolders(cls, path, num_keys):
        output = []
        if num_keys == 1:
            for file in os.listdir(path):
                if '/' in file:
                    print(file, num_keys)
                    raise Exception
                output.append([file.split('.backup')[0]])
        else:
            for key in os.listdir(path):
                keyPath = os.path.join(path, key)
                prevKeys = cls.getKeysFromFolders(keyPath, num_keys - 1)
                for prevKey in prevKeys:
                    output.append([key] + prevKey)

        return output

    def fileToStorable(self, filePath):
        with open(filePath) as f:
            data = f.read()
            return self.dataToStorable(data)

    def dataToStorable(self, data):
        return json.loads(data)

    def dbBackup(self, path, *args):
        userPath = os.path.join(path, *(self.values[:-1]))

        if not os.path.exists(userPath):
            try:
                os.makedirs(userPath)
            except OSError:
                return False

        filePath = os.path.join(userPath, self.values[-1] + '.backup')

        return self.saveToFile(filePath, args)

    def saveToFile(self, filePath, *args):
        value = self.get()
        converted = self.convertToFile(value, args)
        with open(filePath, mode='w') as f:
            f.write(converted)
        return True

    def convertToFile(self, value, *args):
        return json.dumps(value)


class Container(Resource):
    """Methods to support updating lists or dicts.

    Subclasses will require an add_item and remove_item function"""

    def add(self, item, txn=None):
        self.item = item
        after = self.alter(self.add_item, txn=txn)
        return self.item, after

    def remove(self, item, txn=None):
        self.item = item
        after = self.alter(self.remove_item, txn=txn)
        return self.removed, after


class PandasDf(Container):
    """Adds support for using Pandas Data Frames, as well as different ways to add items"""

    def add_item(self, df):
        if isinstance(self.item, pd.Series):
            if len(df.index) >= 1:
                output = self.addSeries(df)
            else:
                output = df.append(self.item, ignore_index=True)
        elif isinstance(self.item, pd.DataFrame):
            if len(df.index) >= 1:
                output = self.addDf(df)
            else:
                output = self.item
        else:
            print('invalid add with item', self.item,
                  'of type', type(self.item),
                  'with db type', self.__class__.__name__)
            output = df

        try:
            return self.sortDf(output)
        except NameError:
            return output

    def addDf(self, df):
        self.item['exists'] = self.item.apply(self.checkExists, axis=1, args=(df,))

        exists = self.item[self.item['exists']]

        notExists = self.item[~self.item['exists']]

        updated = df.apply(self.updateExisting, axis=1, args=(exists,))

        return updated.append(notExists, ignore_index=True).drop(columns='exists')

    def addSeries(self, df):
        exists = self.conditional(self.item, df).any()

        if exists:
            return df.apply(self.updateExisting, axis=1, args=(self.item,))
        else:
            return df.append(self.item, ignore_index=True)

    def updateExisting(self, row, exists):
        exists['dupe'] = self.conditional(row, exists)

        if isinstance(exists, pd.Series):
            if exists['dupe']:
                return exists.drop('dupe')
            return row
        elif isinstance(exists, pd.DataFrame):
            if exists['dupe'].any():
                duped = exists[exists['dupe']]

                if not len(duped.index) == 1:
                    print('multiple dupes', row, exists)
                    raise Exception

                output = duped.iloc[0].drop('dupe')
            else:
                output = row
        else:
            print('invalid update with', exists, 'of type', type(exists))
            raise Exception

        return output

    def checkExists(self, row, df):
        duplicate = self.conditional(row, df)
        return duplicate.any()

    def remove_item(self, df):
        remove = self.conditional(self.item, df)
        self.removed = df[remove]
        return df[~remove]

    def make_details(self):
        return pd.DataFrame()

    def saveToFile(self, filePath, *args):
        value = self.get()
        if value.empty:
            return

        value.to_csv(filePath, sep='\t', index=False)

        return True

    def fileToStorable(self, filePath):
        df = pd.read_csv(filePath, sep='\t')
        return df


def to_string(a):
    return pickle.dumps(a, 2)


def from_string(a):
    return pickle.loads(a)


envOpened = False


def createEnvWithDir(envPath):
    """creates the DBEnv using envPath, Must be called before using the DB

    envPath: The directory where the db will be stored"""

    if not os.path.exists(envPath):
        os.makedirs(envPath)

    env.open(
        envPath,
        db.DB_INIT_MPOOL |
        db.DB_THREAD |
        db.DB_INIT_LOCK |
        db.DB_INIT_TXN |
        db.DB_INIT_LOG |
        db.DB_CREATE)
