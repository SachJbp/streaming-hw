%%time
from nltk.tokenize import word_tokenize
import numpy as np
import nltk
import statistics
nltk.download('punkt')

def data_stream():
    """Stream the data in 'leipzig100k.txt' """
    with open('leipzig100k.txt', 'r') as f:
        for line in f:
            for w in word_tokenize(line):
                if w.isalnum():
                    yield w
                    
def bloom_filter_set():
    """Stream the data in 'Proper.txt' """
    with open('Proper.txt', 'r') as f:
        for line in f:
            yield line.strip()



############### DO NOT MODIFY ABOVE THIS LINE #################


# Implement a universal hash family of functions below: each function from the
# family should be able to hash a word from the data stream to a number in the
# appropriate range needed.

def uhf(rng):
    """Returns a hash function that can map a word to a number in the range
    0 - rng
    """
    p=567629137
    a = np.random.randint(1,p)
    b = np.random.randint(0,p)
    return lambda x: ((a*x+b)%p)%rng

############### 

################### Part 1 ######################

from bitarray import bitarray
size = 2**18   # size of the filter

hash_fns = [uhf(size), uhf(size), uhf(size), uhf(size), uhf(size)]  # place holder for hash functions
bloom_filter = None
num_words = 0         # number in data stream
num_words_in_set = 0  # number in Bloom filter's set

a=bitarray()
for i in range(size):
    a.append(False)

for word in bloom_filter_set(): # add the word to the filter by hashing etc.
        wordB=''.join(format(ord(x), 'b') for x in word)
        wordB=int(wordB,2)

        a[hash_fns[0](wordB)]=True
        a[hash_fns[1](wordB)]=True
        a[hash_fns[2](wordB)]=True
        a[hash_fns[3](wordB)]=True
        a[hash_fns[4](wordB)]=True
        num_words+=1
fp=0
k=0
for word in data_stream():  # check for membership in the Bloom filter
        wordB=''.join(format(ord(x), 'b') for x in word)
        wordB=int(wordB,2)      
        num_words_in_set+=1
        if   (a[hash_fns[0](wordB)]==True and a[hash_fns[1](wordB)]==True and a[hash_fns[2](wordB)]==True and a[hash_fns[3](wordB)]==True and a[hash_fns[4](wordB)]==True):    
             k+=1
        if   a[hash_fns[0](wordB)]==True and a[hash_fns[1](wordB)]==True and a[hash_fns[2](wordB)]==True and a[hash_fns[3](wordB)]==True and a[hash_fns[4](wordB)]==True and not (word in bloom_filter_set()):
            fp+=1
        
                         
print("Total number of False positives",fp)
print("False positive Percentage",fp/k * 100)
print('Total number of words in stream = %s'%(num_words,))
print('Total number of words in stream = %s'%(num_words_in_set,))
      
################### Part 2 ######################

hash_range = 24 # number of bits in the range of the hash functions
fm_hash_functions = [uhf(2**24) for _ in range(35)]  # Create the appropriate hashes here


def num_trailing_bits(n):
    """Returns the number of trailing zeros in bin(n)

    n: integer
    """
    
    b=bin(n)
    #print(b)
    bt=b[-1]
    #print(bt)
    tb=0
    i=1
    while(not (int(bt) == 1 or bt=='b') ) :
        tb+=1
        i+=1
        bt=b[-i]
    #print(tb)
    return tb
    

num_distinct = 0
j=0
maxTB=[0]*35
for word in data_stream(): # Implement the Flajolet-Martin algorithm
    
        wordB=''.join(format(ord(x), 'b') for x in word)
        wordB=int(wordB,2)
        for i in range(35):
            if maxTB[i] < num_trailing_bits(fm_hash_functions[i](wordB)):
                maxTB[i] = num_trailing_bits(fm_hash_functions[i](wordB))
        j+=1
AvgEst=[]*5
for i in range(0,35,7):
    AvgEst.append(np.average(maxTB[i:i+7]))
num_distinct=2**statistics.median(AvgEst)
print("Estimate of number of distinct elements = %s"%(num_distinct,))

################### Part 3 ######################

var_reservoir = [0]*512
second_moment = 0
third_moment = 0

# You can use numpy.random's API for maintaining the reservoir of variables

#for word in data_stream(): # Imoplement the AMS algorithm here
#    pass 
      
print("Estimate of second moment = %s"%(second_moment,))
print("Estimate of third moment = %s"%(third_moment,))
