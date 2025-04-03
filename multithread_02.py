# Description: This script demonstrates the use of multithreading in Python to perform a countdown operation concurrently using two threads.
# Using threads for CPU-intensive tasks like calculations does not improve performance due to the GIL.
import threading

def countdown(n):
    while n > 0:
        n -= 1

thread1 = threading.Thread(target=countdown, args=(50000000,))
thread2 = threading.Thread(target=countdown, args=(50000000,))

thread1.start()
thread2.start()

thread1.join()
thread2.join()
print("Countdown finished.")

# Outcome: The execution time remains similar to single-threaded execution because only one thread runs at a time due to the GIL