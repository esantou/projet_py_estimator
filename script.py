#!/usr/bin/env python
# coding: utf-8

# In[26]:


#####projet spark####
# SANTOU EMMANUEL & CADNEL HOUNKPATIN
#
#
#
#
#
#-----------------------
# In[27]:


from time import time
import numpy as np
from random import random
from operator import add
import math
from pyspark import SparkContext,SparkConf


# In[28]:


from pyspark.sql import SparkSession


# In[37]:


conf = SparkConf().setAppName("").setMaster("local")
sc = SparkContext(conf=conf)


# In[30]:


n = 1000000

def is_point_inside_unit_circle(p):
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0

def spark_pi(n):
    t_0 = time()
    count = sc.parallelize(range(0, n))         .map(is_point_inside_unit_circle).reduce(add)
    print("Pi est proche %f" % (4.0 * count / n))
    print(np.round(time()-t_0, 3), "seconds elapsed for spark approach and n=", n)
    print("l'écart entre math.pi et pi est égale à ", math.pi-(4.0 * count / n))


# In[31]:


def pi_estimator_numpy(n):
    inside = 0
    t = time()
    for i in range (n):
        x, y = random(), random()
        if x**2 + y**2 < 1:
            inside += 1
    return dict(Approximation_pi = inside/n*4, secondes_execution = time()-t, ecart_py=math.pi-(4*inside/n))


# In[32]:


n_sample=100000

spark_pi(n_sample)


# In[33]:


pi_estimator_numpy(n_sample)


# In[36]:


sc.stop()





