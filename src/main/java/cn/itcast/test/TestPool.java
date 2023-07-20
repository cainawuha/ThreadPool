package cn.itcast.test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@FunctionalInterface
interface RejectPolicy<T> {
    void reject(BlockingQueue<T> queue, T task);
}

@Slf4j(topic = "c.TestPool")
public class TestPool {
    public static void main(String[] args) {
        ThreadPoll threadPoll = new ThreadPoll(2, 1000, TimeUnit.MILLISECONDS, 10, (queue, task) -> {
            queue.put(task);
        });
        for (int i = 0; i < 15; i++) {
            int j = i;
            threadPoll.execute(() -> {
                log.debug("{}", j);
            });
        }
    }
}

@Slf4j(topic = "c.ThreadPoll")
class ThreadPoll {
    //任务

    private BlockingQueue<Runnable> taskQueue;
    //线程集合在这
    private HashSet<Worker> workers = new HashSet<>();

    //核心线程数
    private int coreSize;

    //超时时间
    private long timeOut;
    private TimeUnit timeUnit;
    private RejectPolicy<Runnable> rejectPolicy;

    public ThreadPoll(int coreSize, long timeOut, TimeUnit timeUnit, int queueCapacity, RejectPolicy<Runnable> rejectPolicy) {
        this.coreSize = coreSize;
        this.timeOut = timeOut;
        this.timeUnit = timeUnit;
        this.taskQueue = new BlockingQueue<>(queueCapacity);
        this.rejectPolicy = rejectPolicy;
    }

    //线程池的线程执行任务
    public void execute(Runnable task) {
        //若任务数没有超过coreSize，就交给worker执行
        //若超过，就加入blockingQueue
        synchronized (workers) {
            if (workers.size() < coreSize) {
                Worker worker = new Worker(task);
                log.debug("在线程池中新增worker{}", worker);
                workers.add(worker);
                worker.start();
            } else {
                //taskQueue.put(task);

                taskQueue.tryPut(rejectPolicy, task);
            }
        }
    }


    class Worker extends Thread {
        private Runnable task;

        public Worker(Runnable task) {
            this.task = task;
        }

        //具体线程怎么执行任务的逻辑
        @Override
        public void run() {
            while (task != null || (task = taskQueue.poll(timeOut, timeUnit)) != null) {
                try {
                    log.debug("正在执行{}", task);
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    task = null;
                }
            }
            synchronized (workers) {
                log.debug("worker被移除{}", this);
                workers.remove(this);
            }
        }
    }
}

@Slf4j(topic = "c.BlockingQueue")
class BlockingQueue<T> {
    // 1 任务队列
    private final Deque<T> queue = new ArrayDeque<>();
    //锁
    private final ReentrantLock lock = new ReentrantLock();
    //3 生产者条件变量
    private final Condition fullWaitSet = lock.newCondition();
    //4
    private final Condition emptyWaitSet = lock.newCondition();

    private int capacity;
    //queue构造方法


    public BlockingQueue(int capacity) {
        this.capacity = capacity;
    }

    //带超时的获取
    public T poll(long timeout, TimeUnit unit) {
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (queue.isEmpty()) {
                try {
                    //返回剩余时间
                    if (nanos <= 0) {
                        return null;
                    }
                    nanos = emptyWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            fullWaitSet.signal();
            return queue.removeFirst();
        } finally {
            lock.unlock();
        }
    }

    //阻塞获取
    public T take() {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                try {
                    emptyWaitSet.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            fullWaitSet.signal();
            return queue.removeFirst();
        } finally {
            lock.unlock();
        }
    }

    //阻塞添加任务
    public void put(T task) {
        lock.lock();
        try {
            while (queue.size() == capacity) {
                try {
                    log.debug("任务队列满了，等待加入任务队列{}", task);
                    fullWaitSet.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.debug("把任务加入任务队列{}", task);
            queue.addLast(task);
            emptyWaitSet.signal();
        } finally {
            lock.unlock();
        }
    }

    //带超时时间的阻塞添加
    public boolean offer(T task, long timeOut, TimeUnit timeUnit) {
        lock.lock();
        try {
            long nanos = timeUnit.toNanos(timeOut);
            while (queue.size() == capacity) {
                try {
                    log.debug("任务队列满了，等待加入任务队列{}", task);
                    if (nanos <= 0) {
                        return false;
                    }
                    nanos = fullWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.debug("把任务加入任务队列{}", task);
            queue.addLast(task);
            emptyWaitSet.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    //获取队列中任务的数量
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public void tryPut(RejectPolicy<T> rejectPolicy, T task) {
        lock.lock();
        try {
            if (queue.size() == capacity) {
                rejectPolicy.reject(this, task);
            } else {
                log.debug("把任务加入任务队列{}", task);
                queue.addLast(task);
                emptyWaitSet.signal();
            }
        } finally {
            lock.unlock();
        }
    }
}
