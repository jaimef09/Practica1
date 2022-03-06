# -*- coding: utf-8 -*-
"""
Práctica 1: merge concurrente (almacén tamaño K)
"""

from multiprocessing import Process
from multiprocessing import Semaphore, BoundedSemaphore
from multiprocessing import current_process
from multiprocessing import Array
from random import randint

N = 10

K = 4

NPRODS = 3

    
def add_data(storage, pid, index):   #Crea el siguiente dato a partir del anterior
    if index[pid] == 0:
        storage[K*pid] += randint(0,5)
    else:
        storage[K*pid+index[pid]] = storage[K*pid + index[pid] - 1] + randint(0,5)
    index[pid] = index[pid] + 1
        
def get_data(storage, running, index):   #Obtiene el mínimo de los datos y el proceso que lo ha producido
    pid_min = 0
    while not running[pid_min]:
        pid_min += 1
    for i in range(pid_min, NPRODS):
        if storage[i*K]<storage[pid_min*K] and running[i]:
            pid_min = i
    data = storage[pid_min*K]
    for i in range(index[pid_min]-1):
        storage[pid_min*K + i] = storage[pid_min*K + i+1]
    return data, pid_min


def producer(storage, pid, empty_array, nonEmpty_array, index):   #Productor con almacén de tamaño K
    for v in range(N):
        empty_array[pid].acquire()
        print (f"producer {current_process().name} produciendo")
        add_data(storage, pid, index)
        print (f"producer {current_process().name} almacenado {v}")
        nonEmpty_array[pid].release()
    print (f"producer {current_process().name} acaba")
    for i in range(K):
        empty_array[pid].acquire()
        storage[pid*K + index[pid]] = -1 
        nonEmpty_array[pid].release()


def merge(storage, empty_array, nonEmpty_array, index):   #Consumidor
    for v in range(NPRODS):
        nonEmpty_array[v].acquire()   
    consumidos = []
    running = [True for _ in range(NPRODS)]
    while max(running):
        dato,pid_min =get_data(storage, running, index)
        if dato == -1:
            running[pid_min] = False
        else:
            print(f"merge desalmacenando de proceso {pid_min}")
            index[pid_min] = index[pid_min] - 1
            print (f"merge consumiendo un {dato}")
            consumidos.append(dato)
            empty_array[pid_min].release()
            nonEmpty_array[pid_min].acquire()    
    print(f"Consumidos = {consumidos}")



def main():
    empty_array = [BoundedSemaphore(K) for _ in range(NPRODS)]
    nonEmpty_array = [Semaphore(0) for _ in range(NPRODS)]
    storage = Array('i', NPRODS*K)
    index = Array('i', NPRODS)
    print ("Almacén inicial", storage[:])
    
    for i in range(NPRODS):
        index[i] = 0
        for j in range(K):
            storage[i*j+j] = 0
    
    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage, i, empty_array, nonEmpty_array, index))
                for i in range(NPRODS) ]
    
    merge_concurrente = Process(target=merge, args=(storage, empty_array, nonEmpty_array, index))

    for p in prodlst + [merge_concurrente]:
        p.start()

    for p in prodlst + [merge_concurrente]:
        p.join()


if __name__ == '__main__':
    main()
