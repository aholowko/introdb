package introdb.heap.pool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ObjectPool<T> {

	private static final int DEFAULT_MAX_SIZE = 25;

	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	private final AtomicLong poolSize = new AtomicLong(0);
	private final int maxPoolSize;

	private final ConcurrentLinkedQueue<T> objectsQueue = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<CompletableFuture<T>> waitingQueue = new ConcurrentLinkedQueue<>();

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this(fcty, validator, DEFAULT_MAX_SIZE);
	}

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
		this.fcty = fcty;
		this.validator = validator;
		this.maxPoolSize = maxPoolSize;
	}

	/**
	 * When there is object in pool returns completed future,
	 * if not, future will be completed when object is
	 * returned to the pool.
	 * 
	 * @return
	 */
	public CompletableFuture<T> borrowObject() {
		final T obj = objectsQueue.poll();
		if (obj == null) {
			if (poolSize.get() >= maxPoolSize) {
				final CompletableFuture<T> promise = new CompletableFuture<>();
				waitingQueue.offer(promise);
				return promise;
			} else {
				poolSize.incrementAndGet();
				return CompletableFuture.completedFuture(fcty.create());
			}
		} else {
			return CompletableFuture.completedFuture(obj);
		}
	}
	
	public void returnObject(T object) {
	    if (!validator.validate(object)) {
	        throw new IllegalArgumentException("Object is no valid, cannot be returned to the pool");
        }
		final CompletableFuture<T> waitingCF = waitingQueue.poll();
		if (waitingCF == null) {
			objectsQueue.offer(object);
		} else {
			waitingCF.complete(object);
		}
	}

	public void shutdown() throws InterruptedException {
		objectsQueue.clear();
		poolSize.set(0);
		waitingQueue.clear();
	}

	public int getPoolSize() {
		return poolSize.intValue();
	}

	public int getInUse() {
		return getPoolSize();
	}

}
