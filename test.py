import pickle

test = ('1', '2', '3')

print(test)

testlist = list(test)

print(testlist)

print(pickle.dumps(test, 1))

print(pickle.dumps(testlist, 1))