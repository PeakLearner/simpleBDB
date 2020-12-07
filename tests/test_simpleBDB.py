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
    nextTest = ResourceToTest("a", "c")
    nextTest.put(None)

    assert len(nextTest.all()) == 1


def test_resource_make_no_make_details():
    nextTest = ResourceToTest("a", "c")
    assert nextTest.make() is None


def alterTest(toAlter):
    return toAlter + 'altered'


def test_resource_alter():
    test = ResourceToTest("a", "b")
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


def test_container_add():
    testContainer = containerTest('a', 'b')
    testContainer.add('test')

    assert len(testContainer.get()) == 1

    testContainer.add('test2')

    assert len(testContainer.get()) == 2


def test_container_remove():
    testContainer = containerTest('a', 'b')
    testContainer.remove('test')

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

    toRemove = pd.Series({'test': 2})
    removed, dfAfterRemove = dfTest.remove(toRemove)

    assert(len(dfAfterRemove.index)) == 3









