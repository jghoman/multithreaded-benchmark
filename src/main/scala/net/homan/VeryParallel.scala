package net.homan

//import org.junit.Test
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, CountDownLatch}
import java.util.Random
import collection.{Map, mutable}

class VeryParallel {

  //@Test
  def parallel = {
    type Work = (Int, Int)
    val DIVISOR = 10
    val NUM_PIECES_OF_WORK_PER_WORKER = 10000 / DIVISOR
    val NUM_WORKERS = 2000 / DIVISOR
    val QUEUE_SIZE = 1000 / DIVISOR
    val NUM_CONSUMERS = 5
    // Some busy worker, but not so busy that the JVM optimizes it away
    class Worker(latch:CountDownLatch) extends Runnable {
      val MS_TO_WAIT = 1
      val WORKER_QUEUE_SIZE = 10
      val r = new Random(Thread.currentThread().hashCode())
      val workQueue = new LinkedBlockingQueue[Int](WORKER_QUEUE_SIZE)
      var bigSum = 0
      var workProcessed = 0
      @volatile var keepGoing = true

      def work(i:Int):Int = {
        val stop = System.currentTimeMillis + MS_TO_WAIT
        var j = i
        while(System.currentTimeMillis() < stop) {
          j = j + i + (r.nextInt() % 1000)
        }
        j
      }

      def run() {
        latch.await
        while(keepGoing || workQueue.size > 0) {
          val wNull = workQueue.poll(1, TimeUnit.SECONDS)

          if (wNull != null) {
            bigSum = bigSum + work(wNull)
            workProcessed = workProcessed + 1
          }

        }

      }
    }

    val latch = new CountDownLatch(1)
    val workers = (0 until NUM_WORKERS).map(w => (w, new Worker(latch))).toMap
    val workersThreads = workers.values.map(w => new Thread(w))

    val work = (for {i <- 0 until NUM_WORKERS
                     j <- 0 until NUM_PIECES_OF_WORK_PER_WORKER
    } yield (i % NUM_WORKERS, j)).toList

    println("Work size = " + work.size)

    //val workQueues = (0 until NUM_CONSUMERS).map(new mutable.ListBuffer[Work])
    val workQueues = Array.fill(NUM_CONSUMERS) { new mutable.ListBuffer[Work]}
    // Distribute the work to all of the work queues
    var counter:Int = 0
    for (w <- work) {
      workQueues(counter % NUM_CONSUMERS) += w
      counter = counter + 1
    }

    for(wq <- workQueues) { println("workqueue size = %d" format wq.size )}

    class Consumer(workers:Map[Int, Worker], latch:CountDownLatch, wq:List[Work]) extends Runnable {
      var count = 0
      def run() {
        latch.await

        for (w <- wq) {
          val worker:Int = w._1
          val work:Int = w._2
          //println("Sending %d to worker %d" format (work, worker))
          workers.get(worker).get.workQueue.put(work)
          count = count + 1
        }
      }
    }

    val consumers = (0 until NUM_CONSUMERS).map(c => new Thread(new Consumer(workers, latch, scala.util.Random.shuffle(workQueues(c)).toList), "Consumer " + c))
    workersThreads.foreach(_.start)
    consumers.foreach(_.start)

    // And go!
    val start = System.currentTimeMillis()
    latch.countDown()

    consumers.foreach(_.join)

    workers.values.foreach(_.keepGoing = false)
    workersThreads.foreach(_.join)
    val stop = System.currentTimeMillis() - start
    println("All workers joined")

    val totalWorkProcessed = workers.values.foldLeft(0)(_ + _.workProcessed)
    println("Total pieces of work processed = " + totalWorkProcessed)
    println("Done with parallel work, elapsed time = %d milliseconds (%1.2f seconds)." format (stop, stop / 1000.0))
    println("-30-")
  }
}

object VeryParallel extends Application {
  new VeryParallel().parallel
}
