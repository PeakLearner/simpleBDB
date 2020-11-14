import simpleBDB as db
import os


testDir = 'testDb'

db.createEnvWithDir(testDir)


def test_envCreate():
    assert os.path.exists(testDir)


class ResourceToTest(db.Resource):
    keys = ("First", "Second")
    pass



def test_Initialize():
    test = ResourceToTest("a", "b")

    print(os.getcwd())

    test.put("TestVal1")

    print(test)

    print(test.all())

    print(test.db_keys())

    assert 0 == 1


