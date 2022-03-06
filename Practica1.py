# -*- coding: utf-8 -*-
"""
Práctica 1: merge concurrente
"""

from multiprocessing import Process
from multiprocessing import Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Array
from random import randint

N = 10

NPRODS = 3


def add_data(storage, pid):   #Crea el siguiente dato a partir del anterior
    storage[pid] += randint(0,5)
        
def get_data(storage, running):   #Obtiene el mínimo de los datos y el proceso que lo ha producido
    index = 0
    while not running[index]:
        index += 1
    for i in range(index, NPRODS):
        if storage[i]<storage[index] and running[i]:
            index = i
    data = storage[index]
    return data, index


def producer(storage, pid, empty_array, nonEmpty_array):   #Productor
    for v in range(N):
        empty_array[pid].acquire()
        print (f"producer {current_process().name} produciendo")
        add_data(storage, pid)
        print (f"producer {current_process().name} almacenado {v}")
        nonEmpty_array[pid].release()
    print (f"producer {current_process().name} acaba")
    empty_array[pid].acquire()
    storage[pid] = -1
    nonEmpty_array[pid].release()

def merge(storage, empty_array, nonEmpty_array):   #Consumidor
    for v in range(NPRODS):
        nonEmpty_array[v].acquire()
    consumidos = []
    running = [True for _ in range(NPRODS)]
    while max(running):
        dato,index =get_data(storage,running)
        if dato == -1:
            running[index] = False
        else:
            print(f"merge desalmacenando de proceso {index}")
            print (f"merge consumiendo un {dato}")
            consumidos.append(dato)
            empty_array[index].release()
            nonEmpty_array[index].acquire()
    print(f"Consumidos = {consumidos}")



def main():
    empty_array = [Lock() for _ in range(NPRODS)]
    nonEmpty_array = [Semaphore(0) for _ in range(NPRODS)]
    storage = Array('i', NPRODS)
    print ("Almacén inicial", storage[:])
    
    for i in range(NPRODS):
        storage[i] = 0
    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage, i, empty_array, nonEmpty_array))
                for i in range(NPRODS) ]
    
    merge_concurrente = Process(target=merge, args=(storage, empty_array, nonEmpty_array))

    for p in prodlst + [merge_concurrente]:
        p.start()

    for p in prodlst + [merge_concurrente]:
        p.join()


if __name__ == '__main__':
    main()
