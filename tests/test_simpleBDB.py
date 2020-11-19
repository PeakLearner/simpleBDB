import simpleBDB as db
import os
import pandas as pd

testDir = 'testDb'

db.createEnvWithDir(testDir)


def test_envCreate():
    assert os.path.exists(testDir)


class ResourceToTest(db.Resource):
    keys = ("First", "Second")
    pass


test = ResourceToTest("a", "b")


def test_resource_Initialize():
    assert test is not None


def test_resource_getNoVal():
    assert test.get() is None


def test_resource_put():
    test.put('test')

    assert len(test.all()) == 1


def test_resource_get():
    assert test.get() == 'test'


nextTest = ResourceToTest("a", "c")


def test_resource_put_next():
    nextTest.put('nextTest')

    assert len(test.all()) == 2


def test_resource_delete():
    nextTest.put(None)

    assert len(nextTest.all()) == 1


def test_resource_make_no_make_details():
    assert nextTest.make() is None


def alterTest(toAlter):
    return toAlter + 'altered'


def test_resource_alter():
    test.alter(alterTest)

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


testContainer = containerTest('a', 'b')


def test_container_add():
    testContainer.add('test')

    assert len(testContainer.get()) == 1

    testContainer.add('test2')

    assert len(testContainer.get()) == 2


def test_container_remove():
    testContainer.remove('test')

    assert len(testContainer.get()) == 1

    assert testContainer.get()[0] == 'test2'


transactionTestResource = ResourceToTest('b', 'c')


def test_transaction():

    transactionTestResource.put('pretransaction')

    txn = db.env.txn_begin()

    assert transactionTestResource.get(txn=txn) == 'pretransaction'

    transactionTestResource.put('postransaction', txn=txn)

    txn.commit()

    assert transactionTestResource.get() == 'postransaction'


class pandasTest(db.PandasDf):
    keys = ("First", "Second")

    def addSeries(self, df):
        df['exists'] = self.item['test'] == df['test']

        if df['exists'].any():
            output = df.apply(updateInDf, axis=1, args=(self.item,))
        else:
            output = df.drop(columns='exists').append(self.item, ignore_index=True)
        return output

    def addDf(self, df):
        self.item['exists'] = self.item.apply(checkExists, axis=1, args=(df,))

        exists = self.item[self.item['exists']]

        notExists = self.item[~self.item['exists']]

        updated = df.apply(updateDfWithDf, axis=1, args=(exists,))

        return updated.append(notExists, ignore_index=True).drop(columns='exists')

    def sortDf(self, df):
        return df.sort_values('test', ignore_index=True)
    pass


def updateInDf(row, item):
    if row['exists']:
        return item
    return row.drop('exists')


def checkExists(row, df):
    duplicate = df['test'] == row['test']
    return duplicate.any()


def updateDfWithDf(row, df):
    df['dupe'] = row['test'] == df['test']

    if df['dupe'].any():
        duped = df[df['dupe']]

        if not len(duped.index) == 1:
            print('multiple dupes', row, df)
            raise Exception

        output = duped.iloc[0].drop('dupe')
    else:
        output = row

    return output


dfTest = pandasTest('a', 'b')
otherDfTest = pandasTest('a', 'c')


def testPandasDf():
    initial = dfTest.get()
    assert isinstance(initial, pd.DataFrame)
    assert len(initial.index) == 0

    seriesToAdd = pd.Series({'test': 1, 'otherVal': 'test'})
    dfTest.add(seriesToAdd)
    afterSeriesAdd = dfTest.get()
    assert isinstance(afterSeriesAdd, pd.DataFrame)
    assert len(afterSeriesAdd.index) == 1

    otherSeriesToAdd = pd.Series({'test': 0, 'otherVal': 'test0'})
    dfTest.add(otherSeriesToAdd)
    afterOtherSeriesAdd = dfTest.get()
    assert isinstance(afterOtherSeriesAdd, pd.DataFrame)
    assert len(afterOtherSeriesAdd.index) == 2

    updateOtherSeries = pd.Series({'test': 0, 'otherVal': 'updatedWithSeries'})
    dfTest.add(updateOtherSeries)
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
    otherDfTest.add(dfToAdd)
    afterOtherAddDf = otherDfTest.get()
    assert isinstance(afterOtherAddDf, pd.DataFrame)
    assert len(afterOtherAddDf.index) == 3

    dfTest.add(dfToAdd)
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










