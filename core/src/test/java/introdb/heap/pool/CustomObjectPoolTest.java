package introdb.heap.pool;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

class CustomObjectPoolTest {

    @Test
    void shouldReturnImmediatelyAvailableObject() throws ExecutionException, InterruptedException {
        //given
        final ObjectPool<String> stringObjectPool = new ObjectPool<>(() -> "a", (o) -> true, 1);
        final CompletableFuture<String> objFuture = stringObjectPool.borrowObject();

        //when
        final String obj = objFuture.get();
        assertThat(objFuture).isDone();
        assertThat(obj).isEqualTo("a");
    }

    @Test
    void shouldWaitForAvailableObject() throws ExecutionException, InterruptedException {
        //given
        final ObjectPool<Object> stringObjectPool = new ObjectPool<>(() -> new Object(), (o) -> true, 1);
        //when
        final CompletableFuture<Object> objFuture1 = stringObjectPool.borrowObject();
        final Object obj1 = objFuture1.get();
        //no more objects
        final CompletableFuture<Object> objFuture2 = stringObjectPool.borrowObject();
        assertThat(objFuture2).isNotDone();

        stringObjectPool.returnObject(obj1);
        final Object obj2 = objFuture2.get();
        assertThat(objFuture2).isDone();
        assertThat(obj2).isEqualTo(obj1);
    }
}