package net.homan

//import org.junit.Test
import java.util.Random
import java.util.concurrent.{TimeUnit, LinkedBlockingDeque, CountDownLatch}
//import org.junit.Assert._

class Current(DIVISOR:Int) {
  type Work = (Int, Int)
  //val DIVISOR = 10
  val NUM_PIECES_OF_WORK_PER_WORKER = 10000 / DIVISOR
  val NUM_WORKERS = 2000 / DIVISOR
  val QUEUE_SIZE = 1000 / DIVISOR
  //@Test
  def current = {
    println("NUM_PIECES_OF_WORK_PER_WORKER = %d, NUM_WORKERS = %d, QUEUE_SIZE = %d" format (NUM_PIECES_OF_WORK_PER_WORKER, NUM_WORKERS, QUEUE_SIZE))
    // Some busy worker, but not so busy that the JVM optimizes it away
    class Worker() {
      val MS_TO_WAIT = 1
      val r = new Random(Thread.currentThread().hashCode())
      var workProcessed = 0
      def work(i:Int):Int = {
        val stop = System.currentTimeMillis + MS_TO_WAIT
        var j = i
        while(System.currentTimeMillis() < stop) {
          j = j + i + (r.nextInt() % 1000)
        }

        workProcessed = workProcessed + 1
        j
      }
    }

    // Build a bunch of workers, to be called upon when their id randomly arrives
    val range = 0 until NUM_WORKERS
    val workers = range.map(r => (r, new Worker)).toMap

    // Produce some work - confusing name, eh? - and distribute it to some worker
    // via the concurrent queue
    // Do this NUM_PIECES_OF_WORK_TIMES.  Wait for the signal before beginning
    abstract class Consumer(latch:CountDownLatch) extends Runnable {
      var workCompleted = 0
      val r = new Random(Thread.currentThread().hashCode())
      val work = (0 until NUM_PIECES_OF_WORK_PER_WORKER).map(n => (n % NUM_WORKERS, n)).toList

      def run() {
        latch.await()
        work.foreach(w => getQueue.put(w))
      }

      def getQueue:LinkedBlockingDeque[Work]
    }

    println("Creating blocking queue with %d capacity" format QUEUE_SIZE)
    val queue = new LinkedBlockingDeque[Work](QUEUE_SIZE)
    val latch = new CountDownLatch(1)
    val consumers = range.map(r => new Thread(new Consumer(latch) {
      def getQueue: LinkedBlockingDeque[Current.this.type#Work] = queue
    })).toList
    println("Created %d consumers to create some work." format consumers.size)

    // Main thread that polls the queue and hands out the work to the workers
    val runner = new Thread(new Runnable {
      val totalPiecesOfWorker = NUM_PIECES_OF_WORK_PER_WORKER * NUM_WORKERS
      var workReceived = 0
      var bigSum = 0
      var timesQueueWasFull = 0
      def run() {

        println("Starting...")
        while(workReceived < totalPiecesOfWorker) {
          if (queue.size() == QUEUE_SIZE) timesQueueWasFull = timesQueueWasFull + 1
          val (worker, work) = queue.poll(1, TimeUnit.SECONDS)

          bigSum = bigSum + workers(worker).work(work)
          workReceived = workReceived + 1
        }

        println("All done! Received %d pieces of work. bigSum = %d and queue was full %d times" format (workReceived, bigSum, timesQueueWasFull))
      }
    })
    runner.setName("Runner thread")
    runner.start

    consumers.foreach(_.start)

    // And go!
    val start = System.currentTimeMillis()
    latch.countDown
    runner.join()
    val stop = System.currentTimeMillis() - start
    consumers.foreach(c => if (!c.isAlive) c.join)
    println("Total pieces of work processed = " + workers.values.foldLeft(0)(_ + _.workProcessed))
    println("Done with runner, elapsed time = %d milliseconds (%1.2f seconds)." format (stop, stop / 1000.0))

    if (queue.size() != 0) {
      println("Queue wasn't 0!!!")
    }
    println("-30-")
  }
}

object Current extends Application {
  override def main(args:Array[String]) {
    new Current(args(0).toInt).current
  }
}
