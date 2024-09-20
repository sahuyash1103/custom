# fibonacci.py

def fibonacci(n):
    fib_sequence = [0, 1]
    while len(fib_sequence) < n:
        fib_sequence.append(fib_sequence[-1] + fib_sequence[-2])
    return fib_sequence

if __name__ == "__main__":
    n = 10  # Length of Fibonacci series
    result = fibonacci(n)
    print(f"The first {n} numbers in the Fibonacci sequence are: {result}")
