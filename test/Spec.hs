{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}


--------------------------------------------------------------------------------
import           Control.Concurrent       ( forkIO, killThread, threadDelay )
import           Control.Concurrent.Async ( mapConcurrently_ )
import           Control.Concurrent.MVar  ( MVar, modifyMVarMasked_, newMVar
                                          , readMVar, takeMVar
                                          )
import           Control.Monad            ( replicateM_, void )
import           Control.Monad.Trans      ( lift, liftIO )
import           Data.Aeson               ( FromJSON(..), ToJSON(..) )
import qualified Data.Conduit            as Conduit
import           Data.Text                ( Text )
import qualified Data.Text               as T
import           Data.Time
import qualified Database.Redis          as Redis
import           GHC.Generics             ( Generic)
import           Saturn                   ( everyMinute )
import           Test.Hspec
import           Test.HUnit               ( assertEqual )
--------------------------------------------------------------------------------
import           System.Hworker
--------------------------------------------------------------------------------



main :: IO ()
main = hspec $ do
  describe "Simple" $ do
    it "should run and increment counter" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      queue hworker SimpleJob
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1 after job runs" 1 v

    it "queueing 2 jobs should increment twice" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-2" (SimpleState mvar))
      queue hworker SimpleJob
      queue hworker SimpleJob
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 2 after 2 jobs run" 2 v

    it "queueing 1000 jobs should increment 1000" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-3" (SimpleState mvar))
      replicateM_ 1000 (queue hworker SimpleJob)
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1000 after 1000 job runs" 1000 v

    it "should work with multiple workers" $ do
      -- NOTE(dbp 2015-07-12): This probably won't run faster, because
      -- they are all blocking on the MVar, but that's not the point.
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-4" (SimpleState mvar))
      replicateM_ 1000 (queue hworker SimpleJob)
      mapConcurrently_ runJobs [hworker, hworker, hworker, hworker]
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1000 after 1000 job runs" 1000 v

  describe "Priority Jobs" $ do
    it "should execute priority jobs before standard ones" $ do
      mvar <- newMVar []
      hworker <- createWith (conf "priority-worker" (PriorityState mvar))
      queue hworker $ PriorityJob "b"
      queue hworker $ PriorityJob "c"
      queuePriority hworker $ PriorityJob "a"
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be [c,b,a] after job runs" ["c","b","a"] v

  describe "Exceptions" $ do
    it "should be able to have exceptions thrown in jobs and retry the job" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "exworker-1" (ExState mvar))
            { hwconfigExceptionBehavior = RetryOnException }

      queue hworker ExJob
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      listJobs hworker 0 10
      assertEqual "State should be 2, since the first run failed" 2 v

    it "should not retry if mode is FailOnException" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "exworker-2" (ExState mvar))
      queue hworker ExJob
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1, since failing run wasn't retried" 1 v

  describe "Retry" $ do
    it "should be able to return Retry and get run again" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "retryworker-1" (RetryState mvar))
      queue hworker RetryJob
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 2, since it got retried" 2 v

  describe "Fail" $ do
    it "should not retry a job that Fails" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "failworker-1" (FailState mvar))
      queue hworker FailJob
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1, since failing run wasn't retried" 1 v

    it "should put a failed job into the failed queue" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "failworker-2" (FailState mvar))
      queue hworker FailJob
      runJobs hworker
      failedJobs <- listFailed hworker 0 100
      destroy hworker
      assertEqual "Should have failed job" [FailJob] failedJobs

    it "should only store failedQueueSize failed jobs" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "failworker-3" (AlwaysFailState mvar))
            { hwconfigFailedQueueSize = 2 }
      queue hworker AlwaysFailJob
      queue hworker AlwaysFailJob
      queue hworker AlwaysFailJob
      queue hworker AlwaysFailJob
      runJobs hworker
      failedJobs <- listFailed hworker 0 100
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 4, since all jobs were run" 4 v
      assertEqual "Should only have stored 2" [AlwaysFailJob,AlwaysFailJob] failedJobs

  describe "Batch" $ do
    it "should set up a batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      summary <- startBatch hworker Nothing >>= expectBatchSummary hworker
      batchSummaryQueued summary `shouldBe` 0
      batchSummaryCompleted summary `shouldBe` 0
      batchSummarySuccesses summary `shouldBe` 0
      batchSummaryFailures summary `shouldBe` 0
      batchSummaryRetries summary `shouldBe` 0
      batchSummaryStatus summary `shouldBe` BatchQueueing
      destroy hworker

    it "should expire batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker (Just 1)
      batchSummary hworker batch >>= shouldNotBe Nothing
      threadDelay 2000000
      batchSummary hworker batch >>= shouldBe Nothing
      destroy hworker

    it "should increment batch total after queueing a batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      destroy hworker

    it "should not enqueue job for completed batch" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      runJobs hworker
      stopBatchQueueing hworker batch
      summary <- expectBatchSummary hworker batch
      queueBatch hworker batch False [SimpleJob]
        >>= shouldBe (AlreadyQueued summary)
      runJobs hworker
      summary' <- expectBatchSummary hworker batch
      batchSummaryQueued summary' `shouldBe` 1
      destroy hworker

    it "should increment success and completed after completing a successful batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      runJobs hworker
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryFailures summary `shouldBe` 0
      batchSummarySuccesses summary `shouldBe` 1
      batchSummaryCompleted summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchQueueing
      destroy hworker

    it "should increment failure and completed after completing a failed batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "failworker-1" (FailState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [FailJob]
      runJobs hworker
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryFailures summary `shouldBe` 1
      batchSummarySuccesses summary `shouldBe` 0
      batchSummaryCompleted summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchQueueing
      destroy hworker

    it "should change job status to processing when batch is set to stop queueing" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      stopBatchQueueing hworker batch
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchProcessing
      destroy hworker

    it "should change job status to finished when batch is set to stop queueing and jobs are already run" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      runJobs hworker
      stopBatchQueueing hworker batch
      Just batch' <- batchSummary hworker batch
      batchSummaryQueued batch' `shouldBe` 1
      batchSummaryStatus batch' `shouldBe` BatchFinished
      destroy hworker

    it "should change job status to finished when last processed" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      stopBatchQueueing hworker batch
      runJobs hworker
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchFinished
      destroy hworker

    it "queueing 1000 jobs should increment 1000" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-3" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False (replicate 1000 SimpleJob)
      stopBatchQueueing hworker batch
      runJobs hworker
      v <- takeMVar mvar
      v `shouldBe` 1000
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1000
      batchSummaryFailures summary `shouldBe` 0
      batchSummarySuccesses summary `shouldBe` 1000
      batchSummaryCompleted summary `shouldBe` 1000
      batchSummaryStatus summary `shouldBe` BatchFinished
      destroy hworker

    it "should cancel batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob, SimpleJob, SimpleJob]

      job1 <- execWorker hworker
      job1 `shouldBe` CompletedJob SimpleJob Success
      cancelBatch hworker batch
      summary1 <- expectBatchSummary hworker batch
      batchSummaryStatus summary1 `shouldBe` BatchCanceled
      batchSummaryQueued summary1 `shouldBe` 3
      batchSummaryFailures summary1 `shouldBe` 0
      batchSummarySuccesses summary1 `shouldBe` 1
      batchSummaryCanceled summary1 `shouldBe` 0
      batchSummaryCompleted summary1 `shouldBe` 1

      job2 <- execWorker hworker
      job2 `shouldBe` CanceledJob False
      summary2 <- expectBatchSummary hworker batch
      batchSummaryStatus summary2 `shouldBe` BatchCanceled
      batchSummaryQueued summary2 `shouldBe` 3
      batchSummaryFailures summary2 `shouldBe` 0
      batchSummarySuccesses summary2 `shouldBe` 1
      batchSummaryCanceled summary2 `shouldBe` 1
      batchSummaryCompleted summary2 `shouldBe` 2

      job3 <- execWorker hworker
      job3 `shouldBe` CanceledJob True
      summary3 <- expectBatchSummary hworker batch
      batchSummaryStatus summary3 `shouldBe` BatchCanceled
      batchSummaryQueued summary3 `shouldBe` 3
      batchSummaryFailures summary3 `shouldBe` 0
      batchSummarySuccesses summary3 `shouldBe` 1
      batchSummaryCanceled summary3 `shouldBe` 2
      batchSummaryCompleted summary3 `shouldBe` 3

      destroy hworker
      v <- takeMVar mvar
      v `shouldBe` 1

    describe "Atomicity Tests" $ do
      it "should queue all jobs" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        streamBatch hworker batch True $ do
          replicateM_ 50 $ Conduit.yield SimpleJob
          return StreamingOk
        ls <- listJobs hworker 0 100
        length ls `shouldBe` 50
        summary <- expectBatchSummary hworker batch
        batchSummaryQueued summary `shouldBe` 50
        batchSummaryStatus summary `shouldBe` BatchProcessing
        destroy hworker

      it "should not queue jobs when producer throws error" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        streamBatch hworker batch True $ do
          replicateM_ 20 $ Conduit.yield SimpleJob
          return (StreamingAborted "abort")
        ls <- listJobs hworker 0 100
        expectBatchSummary hworker batch
        destroy hworker
        length ls `shouldBe` 0

      it "should not queue jobs on transaction error" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        streamBatchTx hworker batch True $ do
          replicateM_ 20 $ Conduit.yield SimpleJob
          _ <- lift $ Redis.lpush "" []
          replicateM_ 20 $ Conduit.yield SimpleJob
          return StreamingOk
        ls <- listJobs hworker 0 100
        destroy hworker
        length ls `shouldBe` 0

      it "should not queue jobs when transaction is aborted" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        _ <- Redis.runRedis (hworkerConnection hworker) $ Redis.watch [batchCounter hworker batch]
        streamBatch hworker batch True $ do
          replicateM_ 20 $ Conduit.yield SimpleJob
          return StreamingOk
        ls <- listJobs hworker 0 100
        destroy hworker
        length ls `shouldBe` 0

      it "should increment summary up until failure" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing

        void $ streamBatch hworker batch True $ do
          replicateM_ 5 $ Conduit.yield SimpleJob
          error "BLOW UP!"
          replicateM_ 5 $ Conduit.yield SimpleJob
          return StreamingOk

        summary <- expectBatchSummary hworker batch
        batchSummaryQueued summary `shouldBe` 5
        batchSummaryStatus summary `shouldBe` BatchFailed
        ls <- listJobs hworker 0 100
        length ls `shouldBe` 0
        destroy hworker

  describe "Monitor" $ do
    it "should add job back after timeout" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "timedworker-1" (TimedState mvar)) { hwconfigTimeout = 2 }
      wthread <- forkIO (worker hworker)
      queue hworker (TimedJob 1000000)
      threadDelay 500000
      -- Kill the worker thread before the job can even finish, leaving the job
      -- in the progress queue.
      killThread wthread


      -- Monitor should show that job is still in progress queue and has been
      -- running a half second.
      execMonitor hworker >>=
        \case
          [(_, Running _)] -> return ()
          _ -> fail "There should one running job."

      -- Delay further to pass the the timeout.
      threadDelay 2000000

      -- Run monitor again to put requeue job.
      execMonitor hworker >>=
        \case
          [(_, Requeued)] -> return ()
          _ -> fail "There should be one requeued job."

      -- Run worker again, this time allowing job to complete.
      execWorker hworker
      jobs <- execMonitor hworker
      destroy hworker
      assertEqual "Should no longer be job a job in progress queue" [] jobs
      v <- takeMVar mvar
      assertEqual "State should be 1, since first failed" 1 v

    it "should add back multiple jobs after timeout" $ do
      -- Similar to the above test, but we have multiple jobs started, multiple
      -- workers killed, then one worker will finish both interrupted jobs.
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "timedworker-2" (TimedState mvar)) { hwconfigTimeout = 2 }
      wthread1 <- forkIO (worker hworker)
      wthread2 <- forkIO (worker hworker)
      queue hworker (TimedJob 1000000)
      queue hworker (TimedJob 1000000)
      threadDelay 500000
      killThread wthread1
      killThread wthread2

      execMonitor hworker >>=
        \case
          [(_, Running _), (_, Running _)] -> return ()
          _ -> fail "Should be two running jobs"

      threadDelay 2000000
      execMonitor hworker >>=
        \case
          [(_, Requeued), (_, Requeued)] -> return ()
          _ -> fail "There should be two requeued jobs."

      runJobs hworker
      jobs <- execMonitor hworker
      destroy hworker
      assertEqual "Should no longer be job a job in progress queue" [] jobs
      v <- takeMVar mvar
      assertEqual "State should be 2, since first 2 failed" 2 v

    it "should work with multiple monitors" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "timedworker-3" (TimedState mvar)) { hwconfigTimeout = 2 }
      wthread1 <- forkIO (worker hworker)
      wthread2 <- forkIO (worker hworker)
      -- NOTE(dbp 2015-07-24): This might seem silly, but it
      -- was actually sufficient to expose a race condition.
      queue hworker (TimedJob 1000000)
      queue hworker (TimedJob 1000000)
      threadDelay 500000
      killThread wthread1
      killThread wthread2
      threadDelay 2000000
      mapConcurrently_ execMonitor [hworker, hworker, hworker, hworker, hworker, hworker]
      runJobs hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 2, since first 2 failed" 2 v
      -- NOTE(dbp 2015-07-24): It would be really great to have a
      -- test that went after a race between the retry logic and
      -- the monitors (ie, assume that the job completed with
      -- Retry, and it happened to complete right at the timeout
      -- period).  I'm not sure if I could get that sort of
      -- precision without adding other delay mechanisms, or
      -- something to make it more deterministic.

  describe "Scheduled and Recurring Jobs" $ do
    it "should execute job at scheduled time" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      sthread <- forkIO (scheduler hworker)
      time <- getCurrentTime
      queueScheduled hworker SimpleJob (addUTCTime 1 time)
      queueScheduled hworker SimpleJob (addUTCTime 2 time)
      queueScheduled hworker SimpleJob (addUTCTime 4 time)
      threadDelay 1500000 >> readMVar mvar >>= shouldBe 1
      threadDelay 1000000 >> readMVar mvar >>= shouldBe 2
      threadDelay 1000000 >> readMVar mvar >>= shouldBe 2
      threadDelay 1000000 >> readMVar mvar >>= shouldBe 3
      killThread wthread
      killThread sthread
      destroy hworker

    it "should execute a recurring job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "recurringworker-1" (RecurringState mvar))
      wthread <- forkIO (worker hworker)
      sthread <- forkIO (scheduler hworker)
      time <- getCurrentTime
      queueScheduled hworker RecurringJob (addUTCTime 2 time)
      threadDelay 3000000 >> readMVar mvar >>= shouldBe 1
      threadDelay 2000000 >> readMVar mvar >>= shouldBe 2
      threadDelay 2000000 >> readMVar mvar >>= shouldBe 3
      threadDelay 2000000 >> readMVar mvar >>= shouldBe 4
      destroy hworker
      killThread wthread
      killThread sthread

    it "should queue cron on start up" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "simpleworker-1" (SimpleState mvar))
            { hwconfigCronJobs = [CronJob "cron-test" SimpleJob everyMinute] }

      checkCron hworker "cron-test" >>= shouldBe True
      ls <- listScheduled hworker 0 100
      length ls `shouldBe` 1
      destroy hworker

    it "should not enqueue the same job multiple times" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "simpleworker-1" (SimpleState mvar))
            { hwconfigCronJobs = [CronJob "cron-test" SimpleJob everyMinute] }

      time <- getCurrentTime
      initCron hworker time [CronJob "cron-test" SimpleJob everyMinute]
      initCron hworker time [CronJob "cron-test" SimpleJob everyMinute]
      initCron hworker time [CronJob "cron-test" SimpleJob everyMinute]
      ls <- listScheduled hworker 0 100
      length ls `shouldBe` 1
      destroy hworker

    it "should add to processing hash once a cron job is pushed to the jobs queue" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      destroy hworker
      sthread <- forkIO (scheduler hworker)
      time <- getCurrentTime
      initCron hworker (addUTCTime (-60) time) [CronJob "cron-test" SimpleJob everyMinute]
      threadDelay 1000000
      s <- listScheduled hworker 0 100
      length s `shouldBe` 0
      j <- listJobs hworker 0 100
      length j `shouldBe` 1
      checkCron hworker "cron-test" >>= shouldBe True
      liftIO (getCronProcessing hworker "cron-test") >>= shouldNotBe Nothing
      destroy hworker
      killThread sthread

    it "should remove from processing hash and re-enqueue once a cron job is executed" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      destroy hworker
      time <- getCurrentTime
      initCron hworker (addUTCTime (-60) time) [CronJob "cron-test" SimpleJob everyMinute]
      wthread <- forkIO (worker hworker)
      sthread <- forkIO (scheduler hworker)
      threadDelay 1000000
      j <- listJobs hworker 0 100
      length j `shouldBe` 0
      s <- listScheduled hworker 0 100
      length s `shouldBe` 1
      liftIO (getCronProcessing hworker "cron-test") >>= shouldBe Nothing
      destroy hworker
      killThread wthread
      killThread sthread

  describe "Listing jobs" $ do
    it "should list pending jobs" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      replicateM_ 45 (queue hworker SimpleJob)
      listJobs hworker 0 10 >>= shouldBe 10 . length
      listJobs hworker 1 10 >>= shouldBe 10 . length
      listJobs hworker 2 10 >>= shouldBe 10 . length
      listJobs hworker 3 10 >>= shouldBe 10 . length
      listJobs hworker 4 10 >>= shouldBe 5 . length
      destroy hworker

    it "should list scheduled jobs" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      time <- getCurrentTime
      replicateM_ 45 (queueScheduled hworker SimpleJob (addUTCTime 1 time))
      listScheduled hworker 0 10 >>= shouldBe 10 . length
      listScheduled hworker 1 10 >>= shouldBe 10 . length
      listScheduled hworker 2 10 >>= shouldBe 10 . length
      listScheduled hworker 3 10 >>= shouldBe 10 . length
      listScheduled hworker 4 10 >>= shouldBe 5 . length
      destroy hworker

  describe "Broken jobs" $
    it "should store broken jobs" $ do
      -- NOTE(dbp 2015-08-09): The more common way this could
      -- happen is that you change your serialization format. But
      -- we can abuse this by creating two different workers
      -- pointing to the same queue, and submit jobs in one, try
      -- to run them in another, where the types are different.
      mvar <- newMVar 0
      hworker1 <- createWith (conf "broken-1" (TimedState mvar)) { hwconfigTimeout = 5 }
      hworker2 <- createWith (conf "broken-1" (SimpleState mvar)) { hwconfigTimeout = 5 }
      queue hworker2 SimpleJob
      runJobs hworker1
      brokenJobs <- broken hworker2
      destroy hworker1
      v <- takeMVar mvar
      assertEqual "State should be 0, as nothing should have happened" 0 v
      assertEqual "Should be one broken job, as serialization is wrong" 1 (length brokenJobs)

  describe "Dump jobs" $ do
    it "should return the job that was queued" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "dump-1" (SimpleState mvar)) { hwconfigTimeout = 5 }
      queue hworker SimpleJob
      res <- listJobs hworker 0 100
      destroy hworker
      assertEqual "Should be [SimpleJob]" [SimpleJob] res

    it "should return jobs in order (most recently added at front; worker pulls from back)" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "dump-2" (TimedState mvar)) { hwconfigTimeout = 5 }
      queue hworker (TimedJob 1)
      queue hworker (TimedJob 2)
      res <- listJobs hworker 0 100
      destroy hworker
      assertEqual "Should by [TimedJob 2, TimedJob 1]" [TimedJob 2, TimedJob 1] res

  describe "Large jobs" $ do
    it "should be able to deal with lots of large jobs" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "big-1" (BigState mvar))
      let content = T.intercalate "\n" (take 1000 (repeat "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"))
      replicateM_ 5000 (queue hworker (BigJob content))
      mapConcurrently_ runJobs [hworker, hworker, hworker, hworker]
      destroy hworker
      v <- takeMVar mvar
      assertEqual "Should have processed 5000" 5000 v


data SimpleJob =
  SimpleJob deriving (Generic, Show, Eq)

instance ToJSON SimpleJob
instance FromJSON SimpleJob

newtype SimpleState =
  SimpleState (MVar Int)

instance Job SimpleState SimpleJob where
  job Hworker { hworkerState = SimpleState mvar } SimpleJob =
    modifyMVarMasked_ mvar (return . (+1)) >> return Success


data ExJob =
  ExJob deriving (Generic, Show)

instance ToJSON ExJob
instance FromJSON ExJob

newtype ExState =
  ExState (MVar Int)

instance Job ExState ExJob where
  job Hworker { hworkerState = ExState mvar } ExJob = do
    modifyMVarMasked_ mvar (return . (+1))
    v <- readMVar mvar
    if v > 1
      then return Success
      else error "ExJob: failing badly!"


data RetryJob =
  RetryJob deriving (Generic, Show)

instance ToJSON RetryJob
instance FromJSON RetryJob

newtype RetryState =
  RetryState (MVar Int)

instance Job RetryState RetryJob where
  job Hworker { hworkerState = RetryState mvar } RetryJob = do
    modifyMVarMasked_ mvar (return . (+1))
    v <- readMVar mvar
    if v > 1
      then return Success
      else return (Retry "RetryJob retries")


data FailJob =
  FailJob deriving (Eq, Generic, Show)

instance ToJSON FailJob
instance FromJSON FailJob

newtype FailState =
  FailState (MVar Int)

instance Job FailState FailJob where
  job Hworker { hworkerState = FailState mvar } FailJob = do
    modifyMVarMasked_ mvar (return . (+1))
    v <- readMVar mvar
    if v > 1
      then return Success
      else return (Failure "FailJob fails")


data AlwaysFailJob =
  AlwaysFailJob deriving (Eq, Generic, Show)

instance ToJSON AlwaysFailJob
instance FromJSON AlwaysFailJob

newtype AlwaysFailState =
  AlwaysFailState (MVar Int)

instance Job AlwaysFailState AlwaysFailJob where
  job Hworker { hworkerState = AlwaysFailState mvar} AlwaysFailJob = do
    modifyMVarMasked_ mvar (return . (+1))
    return (Failure "AlwaysFailJob fails")


data TimedJob =
  TimedJob Int deriving (Generic, Show, Eq)

instance ToJSON TimedJob
instance FromJSON TimedJob

newtype TimedState =
  TimedState (MVar Int)

instance Job TimedState TimedJob where
  job Hworker { hworkerState = TimedState mvar } (TimedJob delay) = do
    threadDelay delay
    modifyMVarMasked_ mvar (return . (+1))
    return Success


data PriorityJob =
  PriorityJob Text deriving (Generic, Show, Eq)

instance ToJSON PriorityJob
instance FromJSON PriorityJob

newtype PriorityState =
  PriorityState (MVar [Text])

instance Job PriorityState PriorityJob where
  job Hworker { hworkerState = PriorityState mvar } (PriorityJob val) = do
    modifyMVarMasked_ mvar (return . (val:))
    return Success


data BigJob =
  BigJob Text deriving (Generic, Show, Eq)

instance ToJSON BigJob
instance FromJSON BigJob

newtype BigState =
  BigState (MVar Int)

instance Job BigState BigJob where
  job Hworker { hworkerState = BigState mvar } (BigJob _) =
    modifyMVarMasked_ mvar (return . (+1)) >> return Success


data RecurringJob =
  RecurringJob deriving (Generic, Show, Eq)

instance ToJSON RecurringJob
instance FromJSON RecurringJob

newtype RecurringState =
  RecurringState (MVar Int)

instance Job RecurringState RecurringJob where
  job hw@Hworker{ hworkerState = RecurringState mvar} RecurringJob = do
    modifyMVarMasked_ mvar (return . (+1))
    time <- getCurrentTime
    queueScheduled hw RecurringJob (addUTCTime 1.99 time)
    return Success


conf :: Text -> s -> HworkerConfig s t
conf n s =
  (defaultHworkerConfig n s)
    { hwconfigLogger = const (return ())
    , hwconfigExceptionBehavior = FailOnException
    , hwconfigTimeout = 4
    }


startBatch :: Hworker s t -> Maybe Integer -> IO BatchId
startBatch hw expiration =
  initBatch hw expiration >>=
    \case
      Just batch -> return batch
      Nothing    -> fail "Failed to create batch"


expectBatchSummary :: Hworker s t -> BatchId -> IO BatchSummary
expectBatchSummary hw batch =
  batchSummary hw batch >>=
    \case
      Just summary -> return summary
      Nothing      -> fail "Failed to getch batch summary"


runJobs :: Job s t => Hworker s t -> IO Int
runJobs hw =
  let
    loop n =
      execWorker hw >>=
        \case
          NoJobs -> return n
          _      -> loop (n + 1)
  in
  loop 0
