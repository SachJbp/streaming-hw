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

bloom_filter=bitarray([0]*size)

import string
OrdDict=dict()

# Make a dictionary of ascii letters as keys and their binary form as values so as to use them to later to 
#convert words to binary values
for char in string.ascii_letters:   
    OrdDict[char]=format(ord(char), 'b')

for word in bloom_filter_set(): # add the word to the filter by hashing etc.
        try :
            wordB=''.join(OrdDict[char] for char in word)
        except:
            wordB=''.join(format(ord(x), 'b') for x in word)
        wordB=int(wordB,2)
        bloom_filter[hash_fns[0](wordB)]=True
        bloom_filter[hash_fns[1](wordB)]=True
        bloom_filter[hash_fns[2](wordB)]=True
        bloom_filter[hash_fns[3](wordB)]=True
        bloom_filter[hash_fns[4](wordB)]=True
        num_words+=1
fp=0
k=0
    
for word in data_stream():  # check for membership in the Bloom filter
        try :
            wordB=''.join(OrdDict[char] for char in word)
        except:
            wordB=''.join(format(ord(x), 'b') for x in word)
        wordB=int(wordB,2)     
        num_words_in_set+=1
        
        #Count the number of words in data_stream() for which all slots corresponding to 5 hash function values,in bloom fliter are True
        if   (bloom_filter[hash_fns[0](wordB)]==True and bloom_filter[hash_fns[1](wordB)]==True and bloom_filter[hash_fns[2](wordB)]==True and bloom_filter[hash_fns[3](wordB)]==True and bloom_filter[hash_fns[4](wordB)]==True):    
             k+=1
                
        #Count the number of false positives-i.e all 5 slots in Bloom filter are true but word is not in bloom_filter_set()
        if   bloom_filter[hash_fns[0](wordB)]==True and bloom_filter[hash_fns[1](wordB)]==True and bloom_filter[hash_fns[2](wordB)]==True and bloom_filter[hash_fns[3](wordB)]==True and bloom_filter[hash_fns[4](wordB)]==True and not (word in bloom_filter_set()):
            fp+=1
        
                         
print("Total number of False positives",fp)
print("False positive Percentage",fp/k * 100)
print('Total number of words in proper.txt = %s'%(num_words,))
print('Total number of words in stream = %s'%(num_words_in_set,))
      
################### Part 2 ######################

hash_range = 24 # number of bits in the range of the hash functions
fm_hash_functions = [uhf(2**24) for _ in range(35)]  # Create the appropriate hashes here


def num_trailing_bits(n):
    """Returns the number of trailing zeros in bin(n)

    n: integer
    """
    b=bin(n)
    b1=str(b).rstrip('0')  # Strip the bin(n) of trailing zeros 
    tb=len(b)-len(b1)  #Difference between the lengths of bin(n) and that after stripping trailing zeros gives us the length of trailing 0s
    return tb
    
num_distinct = 0
j=0
maxTB=[0]*35  # to track the maximum length of trailing zeros for each of the 35 hash functions

for word in data_stream(): # Implement the Flajolet-Martin algorithm
        try :
            wordB=''.join(OrdDict[char] for char in word)
        except:
            wordB=''.join(format(ord(x), 'b') for x in word)
        wordB=int(wordB,2)
        for i in range(35):
            if maxTB[i] < num_trailing_bits(fm_hash_functions[i](wordB)):
                maxTB[i] = num_trailing_bits(fm_hash_functions[i](wordB))
        j+=1
        
print(maxTB)
AvgEst=[]*5
for i in range(0,35,7):
    AvgEst.append(np.average(maxTB[i:i+7]))
num_distinct=2**statistics.median(AvgEst)
print("Estimate of number of distinct elements = %s"%(num_distinct,))

################### Part 3 ######################

var_reservoir=[]

genList=list(data_stream())
np.random.shuffle(genList) 
var_reservoir=genList[0:512]  #variable reservoir
 
m2=[]
m3=[]

# Select random positions from the reservoir sample for each position,find the frequency of the word in the reservoir threafter 

for i in range(100):
    j=np.random.randint(0,512)
    wordj=var_reservoir[j]
    fq=1
    for k in range(j+1,512):
        if var_reservoir[k]==wordj:
            fq+=1
    m2.append(512*(2*fq-1))
    m3.append(512*(fq**3 - (fq-1)**3))

second_moment=np.average(m2)
third_moment=np.average(m3)
      
print("Estimate of second moment = %s"%(second_moment,))
print("Estimate of third moment = %s"%(third_moment,))