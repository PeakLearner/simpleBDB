import threading
import time

import simpleBDB as db
import os
import shutil
import pandas as pd
import pytest
import berkeleydb
import random

testDir = 'testDb'

if os.path.exists('testDb'):
    shutil.rmtree('testDb')

db.createEnvWithDir(testDir)


def test_envCreate():
    assert os.path.exists(testDir)


class ResourceToTest(db.Resource):
    keys = ("First", "Second")
    pass


def test_resource_Initialize():
    test = ResourceToTest("a", "b")
    assert test is not None


def test_resource_getNoVal():
    test = ResourceToTest("a", "b")
    assert test.get() is None


def test_resource_put():
    test = ResourceToTest("a", "b")

    test.put('test')

    assert len(test.all()) == 1


def test_resource_get():
    test = ResourceToTest("a", "b")
    assert test.get() == 'test'


def test_resource_put_next():
    nextTest = ResourceToTest("a", "c")
    nextTest.put('nextTest')

    assert len(nextTest.all()) == 2


def test_resource_delete():
    txn = db.getEnvTxn()
    nextTest = ResourceToTest("a", "c")
    nextTest.put(None, txn=txn)
    txn.commit()

    assert len(ResourceToTest.all()) == 1


def test_resource_make_no_make_details():
    nextTest = ResourceToTest("a", "c")
    assert nextTest.make() is None


def alterTest(toAlter):
    return toAlter + 'altered'


def test_resource_alter():
    test = ResourceToTest("a", "b")
    txn = db.getEnvTxn()
    test.alter(alterTest, txn=txn)
    txn.commit()

    assert test.get() == 'testaltered'


class makeDetailsTest(db.Resource):
    keys = ("First", "Second")

    def make_details(self):
        return "Default Value"


def test_resource_get_default_value():
    testMake = makeDetailsTest('a', 'c')

    assert testMake.get() == 'Default Value'


class containerTest(db.Container):
    keys = ("First", "Second")

    def add_item(self, toAdd):
        toAdd.append(self.item)
        return toAdd

    def remove_item(self, toRemove):
        self.removed = toRemove.remove(self.item)
        return toRemove

    def make_details(self):
        return []


def test_container_add():
    testContainer = containerTest('a', 'b')
    txn = db.getEnvTxn()
    testContainer.add('test', txn=txn)
    txn.commit()

    assert len(testContainer.get()) == 1

    txn = db.getEnvTxn()
    testContainer.add('test2', txn=txn)
    txn.commit()

    assert len(testContainer.get()) == 2


def test_container_remove():
    testContainer = containerTest('a', 'b')
    txn = db.getEnvTxn()
    testContainer.remove('test', txn=txn)
    txn.commit()

    assert len(testContainer.get()) == 1

    assert testContainer.get()[0] == 'test2'


transactionTestResource = ResourceToTest('b', 'c')


class pandasTest(db.PandasDf):
    keys = ("First", "Second")

    def conditional(self, item, df):
        return item['test'] == df['test']

    def sortDf(self, df):
        return df.sort_values('test', ignore_index=True)
    pass


def testPandasDf():
    dfTest = pandasTest('a', 'b')
    otherDfTest = pandasTest('a', 'c')

    initial = dfTest.get()
    assert isinstance(initial, pd.DataFrame)
    assert len(initial.index) == 0

    seriesToAdd = pd.Series({'test': 1, 'otherVal': 'test'})
    txn = db.getEnvTxn()
    dfTest.add(seriesToAdd, txn=txn)
    txn.commit()
    afterSeriesAdd = dfTest.get()
    assert isinstance(afterSeriesAdd, pd.DataFrame)
    assert len(afterSeriesAdd.index) == 1

    otherSeriesToAdd = pd.Series({'test': 0, 'otherVal': 'test0'})
    txn = db.getEnvTxn()
    dfTest.add(otherSeriesToAdd, txn=txn)
    txn.commit()
    afterOtherSeriesAdd = dfTest.get()
    assert isinstance(afterOtherSeriesAdd, pd.DataFrame)
    assert len(afterOtherSeriesAdd.index) == 2

    updateOtherSeries = pd.Series({'test': 0, 'otherVal': 'updatedWithSeries'})
    txn = db.getEnvTxn()
    dfTest.add(updateOtherSeries, txn=txn)
    txn.commit()
    afterSeriesUpdate = dfTest.get()
    assert isinstance(afterSeriesUpdate, pd.DataFrame)
    assert len(afterSeriesUpdate.index) == 2

    val = afterSeriesUpdate[afterSeriesUpdate['test'] == 0]
    assert len(val.index) == 1
    assert val.iloc[0]['otherVal'] == 'updatedWithSeries'

    otherInit = otherDfTest.get()
    assert isinstance(otherInit, pd.DataFrame)
    assert len(otherInit.index) == 0

    dfToAdd = pd.DataFrame({'test': [1, 2, 3], 'otherVal': ['updatedWithDf', 'test2', 'test3']})
    txn = db.getEnvTxn()
    otherDfTest.add(dfToAdd, txn=txn)
    txn.commit()
    afterOtherAddDf = otherDfTest.get()
    assert isinstance(afterOtherAddDf, pd.DataFrame)
    assert len(afterOtherAddDf.index) == 3

    txn = db.getEnvTxn()
    dfTest.add(dfToAdd, txn=txn)
    txn.commit()
    afterDfAdd = dfTest.get()
    assert isinstance(afterDfAdd, pd.DataFrame)
    assert len(afterDfAdd.index) == 4

    test0 = afterDfAdd[afterDfAdd['test'] == 0]
    assert len(test0.index) == 1
    assert test0.iloc[0]['otherVal'] == 'updatedWithSeries'

    test1 = afterDfAdd[afterDfAdd['test'] == 1]
    assert len(test1.index) == 1
    assert test1.iloc[0]['otherVal'] == 'updatedWithDf'

    test1 = afterDfAdd[afterDfAdd['test'] == 2]
    assert len(test1.index) == 1
    assert test1.iloc[0]['otherVal'] == 'test2'

    toRemove = pd.Series({'test': 2})
    txn = db.getEnvTxn()
    removed, dfAfterRemove = dfTest.remove(toRemove, txn=txn)
    txn.commit()

    assert(len(dfAfterRemove.index)) == 3


def testWhichMatch():
    with pytest.raises(ValueError) as err:
        pandasTest.keysWhichMatch('a', 'b', 'c')

    withA = pandasTest.keysWhichMatch('a')

    assert len(withA) == 2

    withAB = pandasTest.keysWhichMatch('a', 'b')

    assert len(withAB) == 1

    with pytest.raises(ValueError) as err:
        pandasTest.keysWhichMatch()


class CursorTest(db.Resource):
    keys = ('first',)

    pass


def testCursors():

    for i in range(1, 3):
        txn = db.getEnvTxn()
        CursorTest(str(i)).put(i, txn=txn)
        txn.commit()

    txn = db.getEnvTxn()

    assert len(CursorTest.db_keys()) == 2

    cursor = CursorTest.getCursor(txn=txn, bulk=True)

    firstKey, firstValue = cursor.get(flags=berkeleydb.db.DB_NEXT)

    assert firstKey == ('1',)
    assert firstValue == 1

    secondKey, secondValue = cursor.get(flags=berkeleydb.db.DB_NEXT)

    assert secondKey == ('2',)
    assert secondValue == 2

    # Nothing left in db
    out = cursor.get(flags=berkeleydb.db.DB_NEXT)

    assert out is None
    withKeyKey, withKeyValue = cursor.getWithKey(firstKey, flags=berkeleydb.db.DB_SET)

    assert withKeyKey == ('1',)
    assert withKeyValue == 1

    cursor.close()

    txn.commit()


def testCursorzDuplicate():

    for i in range(3, 11):
        txn = db.getEnvTxn()
        CursorTest(str(i)).put(i, txn=txn)
        txn.commit()

    assert len(CursorTest.db_keys()) == 10

    txn = db.getEnvTxn()
    cursor = CursorTest.getCursor(txn=txn, bulk=True)
    prev = None

    # Testing next and dup functions
    for i in range(1, 11):
        key, current = cursor.next()

        assert current == i

        if prev is None:
            prev = cursor.dup()

        else:
            prevKey, prevCurrent = prev.current()

            assert prevCurrent + 1 == current

            prev.close()

            prev = cursor.dup()

    if prev is not None:
        prev.close()

    cursor.close()
    txn.commit()


def testCursorzzPut():

    txn = db.getEnvTxn()
    cursor = CursorTest.getCursor(txn=txn, bulk=True)

    current = cursor.next(flags=berkeleydb.db.DB_RMW)

    while current is not None:
        (key,), value = current

        print(key, value)

        toPut = value - 0.5

        cursor.put(key, toPut)

        current = cursor.next(flags=berkeleydb.db.DB_RMW)

    cursor.close()
    txn.commit()

    txn = db.getEnvTxn()
    cursor = CursorTest.getCursor(txn=txn, bulk=True)

    current = cursor.next(flags=berkeleydb.db.DB_RMW)

    # check that the changes went into effect
    while current is not None:
        # Key is a tuple of length one here, unpack it to a value
        (key,), value = current

        assert int(key) == value + 0.5

        current = cursor.next(flags=berkeleydb.db.DB_RMW)

    cursor.close()
    txn.commit()


def testCursorzzzPutLaterWithDup():
    toSelect = round((len(CursorTest.db_keys()) - 1) * random.random()) + 1
    txn = db.getEnvTxn()
    cursor = CursorTest.getCursor(txn=txn, bulk=True)
    randomSelected = None

    current = cursor.next(flags=berkeleydb.db.DB_RMW)

    while current is not None:
        (key,), value = current

        if int(key) == toSelect:
            randomSelected = cursor.dup()

        current = cursor.next(flags=berkeleydb.db.DB_RMW)

    cursor.close()

    toUpdate = randomSelected.current()

    assert toUpdate is not None

    (key,), value = toUpdate

    assert int(key) == toSelect

    value += 0.25

    randomSelected.put(key, value)

    randomSelected.close()
    txn.commit()

    toCheck = CursorTest(str(toSelect)).get()

    assert value == toCheck


runDeadlock = True


def deadlockDetector():
    while runDeadlock:
        print('lock')
        db.env.lock_detect(berkeleydb.db.DB_LOCK_DEFAULT)
        time.sleep(1)


def test_detect_deadlock():
    global runDeadlock
    firstTxn = db.getEnvTxn()
    secondTxn = db.getEnvTxn()

    thread = threading.Thread(target=deadlockDetector)
    thread.start()

    # Block using a write
    first = CursorTest('1').get(txn=firstTxn, write=True)

    # This should error due to deadlock
    with pytest.raises(berkeleydb.db.DBLockDeadlockError):
        second = CursorTest('1').get(txn=secondTxn, write=True)

    firstTxn.commit()
    secondTxn.commit()

    runDeadlock = False
    thread.join()




db.open_dbs()




