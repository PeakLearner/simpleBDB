import json
import pickle
import os
import pandas as pd
# third party module not by me:
import bsddb3

# For more info on bsddb3 see here: http://pybsddb.sourceforge.net/bsddb3.html

# Setup Berkeley DB Env
# More info n the flags can be found here: https://docs.oracle.com/cd/E17276_01/html/api_reference/C/envopen.html
env = bsddb3.db.DBEnv()

CLOSE_ON_EXIT = []


# this prevents lockers/locks from accumulating when python is closed
# normally, but does not prevent this when we C-c out of the server.
def close_db():
    """Closes the DB's when the system is closed with C-c"""
    for db in CLOSE_ON_EXIT:
        db.sync()
        db.close()
    env.close()


def getEnvTxn():
    return env.txn_begin()


class DB(type):
    """Metaclass for Resource objects"""

    def __init__(cls, name, bases, dct):
        """Called when Resource and each subclass is defined"""
        if "keys" in dir(cls):
            cls.filename = name
            cls.db = bsddb3.db.DB(env)
            cls.db.open(cls.filename, None, cls.DBTYPE,
                        bsddb3.db.DB_AUTO_COMMIT |
                        bsddb3.db.DB_THREAD |
                        bsddb3.db.DB_CREATE)
            CLOSE_ON_EXIT.append(cls.db)

    def getTxn(cls):
        return cls.db.txn_begin()


class Resource(metaclass=DB):
    """Base class for bsddb3 files"""
    DBTYPE = bsddb3.db.DB_BTREE

    @classmethod
    def all(cls):
        return [cls(*tup).get() for tup in cls.db_key_tuples()]

    @classmethod
    def db_keys(cls):
        return [from_string(k) for k in cls.db.keys()]

    @classmethod
    def db_key_tuples(cls):
        return [k.split(" ") for k in cls.db_keys()]

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
        self.values = [str(a) for a in args]
        for a in self.values:
            if " " in a:
                raise ValueError("values should have no spaces")
        self.info = dict(zip(self.keys, self.values))
        self.set_db_key()

    def set_db_key(self):
        self.db_key = to_string(" ".join([str(x) for x in self.values]))

    def alter(self, fun, txn=None):
        """Apply fun to current value and then save it."""
        commit = False
        if txn is None:
            txn = env.txn_begin()
            commit = True
        before = self.get(txn, write=True)
        after = fun(before)
        self.put(after, txn)
        if commit:
            txn.commit()

        return after

    def get(self, txn=None, write=False):
        """Get method for resource, and its subclasses"""
        if self.db_key not in self.db:
            return self.make(txn)
        flags = 0
        if write:
            flags = bsddb3.db.DB_RMW
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
                prevKeys = cls.getKeysFromFolders(keyPath, num_keys-1)
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
    global envOpened

    if not envOpened:
        if not os.path.exists(envPath):
            os.makedirs(envPath)
        env.open(
            envPath,
            bsddb3.db.DB_INIT_MPOOL |
            bsddb3.db.DB_THREAD |
            bsddb3.db.DB_INIT_LOCK |
            bsddb3.db.DB_INIT_TXN |
            bsddb3.db.DB_INIT_LOG |
            bsddb3.db.DB_CREATE)
        envOpened = True
