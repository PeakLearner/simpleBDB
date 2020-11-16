import simpleBDB as db
import os

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
