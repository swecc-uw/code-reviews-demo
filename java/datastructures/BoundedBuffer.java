package datastructures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

public class BoundedBuffer<T> {
  private final T[] buffer;
  private final int capacity;
  private final AtomicInteger size;
  private final AtomicInteger head;
  private final AtomicInteger tail;
  private final Lock lock;
  private final Condition notFull;
  private final Condition notEmpty;

  @SuppressWarnings("unchecked")
  public BoundedBuffer(int capacity) {
    this.capacity = capacity;
    this.buffer = (T[]) new Object[capacity];
    this.size = new AtomicInteger(0);
    this.head = new AtomicInteger(0);
    this.tail = new AtomicInteger(0);
    this.lock = new ReentrantLock();
    this.notFull = lock.newCondition();
    this.notEmpty = lock.newCondition();
  }

  public void put(T item) throws InterruptedException {
    lock.lock();
    try {
      while (size.get() == capacity) {
        notFull.await();
      }
      buffer[tail.getAndIncrement() % capacity] = item;
      if (tail.get() >= capacity)
        tail.set(0); // Reset to zero for circular behavior
      size.getAndIncrement();
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  public T take() throws InterruptedException {
    lock.lock();
    try {
      while (size.get() == 0) {
        notEmpty.await();
      }
      T item = buffer[head.getAndIncrement() % capacity];
      if (head.get() >= capacity)
        head.set(0); // Reset to zero for circular behavior
      size.getAndDecrement();
      notFull.signal();
      return item;
    } finally {
      lock.unlock();
    }
  }

  public static int simulate(int capacity, int numProducers, int numConsumers, int numOps) {
    BoundedBuffer<Integer> buffer = new BoundedBuffer<>(capacity);

    Thread[] producers = new Thread[numProducers];
    Thread[] consumers = new Thread[numConsumers];

    long startTime = System.nanoTime();

    for (int i = 0; i < numProducers; i++) {
      final int tid = i;
      producers[i] = new Thread(() -> {
        try {
          for (int j = 0; j < numOps; j++) {
            buffer.put(tid * numOps + j);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new Thread(() -> {
        try {
          for (int j = 0; j < numOps; j++) {
            buffer.take();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    for (int i = 0; i < numProducers; i++) {
      producers[i].start();
    }

    for (int i = 0; i < numConsumers; i++) {
      consumers[i].start();
    }

    for (int i = 0; i < numProducers; i++) {
      try {
        producers[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < numConsumers; i++) {
      try {
        consumers[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    long endTime = System.nanoTime();
    return (int) (endTime - startTime);
  }

  public static void main(String[] args) {

    int[] capacities = { 1, 10, 100, 1000 };
    int numProducers = 10;
    int numConsumers = 10;
    int[] numOps = { 100, 1000, 10000 };

    List<Integer> times = new ArrayList<>();
    List<Integer> capacitiesList = new ArrayList<>();
    List<Integer> numOpsList = new ArrayList<>();

    for (int capacity : capacities) {
      for (int numOp : numOps) {
        long totalTime = 0;
        for (int i = 0; i < 5; i++) {
          totalTime += simulate(capacity, numProducers, numConsumers, numOp);
        }
        times.add((int) (totalTime / 5));
        capacitiesList.add(capacity);
        numOpsList.add(numOp);
      }
    }

    List<Integer> sortedIdx = IntStream.range(0, times.size())
        .boxed()
        .sorted((i, j) -> times.get(i) / numOpsList.get(i) - times.get(j) / numOpsList.get(j))
        .toList();

    for (int idx : sortedIdx) {
      int capacity = capacitiesList.get(idx), numOp = numOpsList.get(idx), totalTime = times.get(idx);
      System.out.printf("Average time out of three runs for capacity %d, numOps %d: %.2f ms%n", capacity, numOp,
          totalTime / 5e6);
    }
  }
}
