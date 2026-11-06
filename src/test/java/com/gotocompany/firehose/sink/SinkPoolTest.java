package com.gotocompany.firehose.sink;

import com.gotocompany.firehose.exception.SinkTaskFailedException;
import com.gotocompany.firehose.message.Message;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SinkPoolTest {

    @Mock
    private BlockingQueue<Sink> workerSinks;
    @Mock
    private ExecutorService executorService;

    private SinkPool sinkPool;

    @Mock
    private Sink sink1;
    @Mock
    private Sink sink2;
    @Mock
    private Future<List<Message>> future1;
    @Mock
    private Future<List<Message>> future2;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        sinkPool = new SinkPool(workerSinks, new ArrayList<>(), executorService, 5);
    }

    @Test
    public void shouldSubmitTask() throws InterruptedException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink1);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink1, messages, workerSinks))).thenReturn(future1);
        Assert.assertEquals(future1, sinkPool.submitTask(messages));

    }

    @Test
    public void shouldNotSubmitTask() throws InterruptedException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(null);
        Assert.assertNull(sinkPool.submitTask(messages));
    }

    @Test
    public void shouldFetchFinishedFutures() throws InterruptedException {
        List<Message> messageList1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink1);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink1, messageList1, workerSinks))).thenReturn(future1);

        sinkPool.submitTask(messageList1);

        List<Message> messageList2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }};

        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink2);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink2, messageList2, workerSinks))).thenReturn(future2);
        sinkPool.submitTask(messageList2);

        Mockito.when(future1.isDone()).thenReturn(false);
        Mockito.when(future2.isDone()).thenReturn(false);
        Set<Future<List<Message>>> finishedTasks = sinkPool.fetchFinishedSinkTasks();
        Assert.assertEquals(0, finishedTasks.size());

        Mockito.when(future1.isDone()).thenReturn(true);
        finishedTasks = sinkPool.fetchFinishedSinkTasks();
        Assert.assertEquals(1, finishedTasks.size());
        Mockito.verify(workerSinks, Mockito.never()).put(Mockito.any());
        Mockito.verify(workerSinks, Mockito.never()).add(Mockito.any());
    }

    @Test
    public void shouldReturnSinkToQueueAfterSuccessfulPush() throws Exception {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
        }};
        List<Message> failedMessages = new ArrayList<>();
        BlockingQueue<Sink> queue = new LinkedBlockingQueue<>();
        Mockito.when(sink1.pushMessage(messages)).thenReturn(failedMessages);

        List<Message> result = new SinkPool.SinkTask(sink1, messages, queue).call();

        Assert.assertSame(failedMessages, result);
        Assert.assertEquals(1, queue.size());
        Assert.assertSame(sink1, queue.peek());
    }

    @Test
    public void shouldReturnSinkToQueueWhenPushThrows() throws IOException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
        }};
        BlockingQueue<Sink> queue = new LinkedBlockingQueue<>();
        IOException exception = new IOException("push failed");
        Mockito.when(sink1.pushMessage(messages)).thenThrow(exception);

        try {
            new SinkPool.SinkTask(sink1, messages, queue).call();
            Assert.fail("Expected exception from SinkTask");
        } catch (Exception e) {
            Assert.assertSame(exception, e);
        }
        Assert.assertEquals(1, queue.size());
        Assert.assertSame(sink1, queue.peek());
    }

    @Test
    public void shouldThrowSinkTaskFailedExceptionWhenTaskFails() throws Exception {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
        }};
        RuntimeException cause = new RuntimeException("push failed");
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink1);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink1, messages, workerSinks))).thenReturn(future1);
        sinkPool.submitTask(messages);
        Mockito.when(future1.isDone()).thenReturn(true);
        Mockito.when(future1.get()).thenThrow(new ExecutionException(cause));

        try {
            sinkPool.fetchFinishedSinkTasks();
            Assert.fail("Expected SinkTaskFailedException");
        } catch (SinkTaskFailedException e) {
            Assert.assertSame(cause, e.getCause());
        }
    }

    @Test(timeout = 5000)
    public void shouldMakeSinkAvailableAsSoonAsWorkerFinishes() throws Exception {
        List<Message> messageList1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
        }};
        List<Message> messageList2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
        }};
        CountDownLatch releaseFirstPush = new CountDownLatch(1);
        Mockito.when(sink1.pushMessage(messageList1)).thenAnswer(invocation -> {
            releaseFirstPush.await();
            return new ArrayList<Message>();
        });
        Mockito.when(sink1.pushMessage(messageList2)).thenReturn(new ArrayList<>());
        List<Sink> sinks = Collections.singletonList(sink1);
        ExecutorService workerExecutor = Executors.newSingleThreadExecutor();
        ExecutorService submitterExecutor = Executors.newSingleThreadExecutor();
        SinkPool pool = new SinkPool(new LinkedBlockingQueue<>(sinks), sinks, workerExecutor, 10000);
        try {
            Assert.assertNotNull(pool.submitTask(messageList1));
            Future<Future<List<Message>>> secondSubmission = submitterExecutor.submit(() -> pool.submitTask(messageList2));

            releaseFirstPush.countDown();

            Assert.assertNotNull(secondSubmission.get(2000, TimeUnit.MILLISECONDS));
        } finally {
            releaseFirstPush.countDown();
            submitterExecutor.shutdownNow();
            workerExecutor.shutdownNow();
        }
    }
}
