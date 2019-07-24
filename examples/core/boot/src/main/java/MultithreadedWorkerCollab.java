import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.worker.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Alex Syrotenko (@pantokrator)
 * Created on 17.07.19.
 */

//[START EXAMPLE]
public final class MultithreadedWorkerCollab extends AbstractModule {

	@Provides
	@Worker
	Eventloop eventloop(@WorkerId int wid, ConcurrentLinkedQueue<Integer> queue) {
		Eventloop eventloop = Eventloop.create();
		eventloop.submit(() -> eventloop.delay(100 * wid, () -> queue.add(wid)));
		return eventloop;
	}

	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(25);
	}

	@Provides
	ConcurrentLinkedQueue<Integer> queue() {
		return new ConcurrentLinkedQueue<>();
	}

	@Provides
	@Worker
	Integer string(@WorkerId int workerId) {
		System.out.println("Hello from worker #" + workerId);
		return workerId;
	}

	public static void main(String[] args) throws InterruptedException {
		Injector injector = Injector.of(new WorkerPoolModule(), new MultithreadedWorkerCollab());
		WorkerPool workerPool = injector.getInstance(WorkerPool.class);
		WorkerPool.Instances<Eventloop> eventloops = workerPool.getInstances(Eventloop.class);

		List<Thread> threads = new ArrayList<>();
		for (Eventloop eventloop : eventloops.getList()) {
			Thread thread = new Thread(eventloop);
			threads.add(thread);
		}

		Collections.shuffle(threads);
		threads.forEach(Thread::start);

		for (Thread thread : threads) {
			thread.join();
		}

		ConcurrentLinkedQueue<Integer> queue = injector.getInstance(new Key<ConcurrentLinkedQueue<Integer>>() {});

		while (!queue.isEmpty()) {
			System.out.println(queue.poll());
		}

	}
}
//[END EXAMPLE]
