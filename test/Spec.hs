{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
import           Control.Concurrent       (forkIO, killThread, threadDelay)
import           Control.Concurrent.MVar  (MVar, modifyMVarMasked_, newMVar,
                                           readMVar, takeMVar)
import           Control.Monad            (replicateM_)
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Text                as T
import           GHC.Generics             (Generic)
import           System.Hworker
import           System.IO

import           Test.Hspec
import           Test.Hspec.Contrib.HUnit
import           Test.HUnit

data SimpleJob = SimpleJob deriving (Generic, Show, Eq)
data SimpleState = SimpleState { unSimpleState :: MVar Int }
instance ToJSON SimpleJob
instance FromJSON SimpleJob
instance Job SimpleState SimpleJob where
  job (SimpleState mvar) SimpleJob =
    do modifyMVarMasked_ mvar (return . (+1))
       return Success

data ExJob = ExJob deriving (Generic, Show)
data ExState = ExState { unExState :: MVar Int }
instance ToJSON ExJob
instance FromJSON ExJob
instance Job ExState ExJob where
  job (ExState mvar) ExJob =
    do modifyMVarMasked_ mvar (return . (+1))
       v <- readMVar mvar
       if v > 1
          then return Success
          else error "ExJob: failing badly!"

data RetryJob = RetryJob deriving (Generic, Show)
data RetryState = RetryState { unRetryState :: MVar Int }
instance ToJSON RetryJob
instance FromJSON RetryJob
instance Job RetryState RetryJob where
  job (RetryState mvar) RetryJob =
    do modifyMVarMasked_ mvar (return . (+1))
       v <- readMVar mvar
       if v > 1
          then return Success
          else return (Retry "RetryJob retries")

data FailJob = FailJob deriving (Eq, Generic, Show)
data FailState = FailState { unFailState :: MVar Int }
instance ToJSON FailJob
instance FromJSON FailJob
instance Job FailState FailJob where
  job (FailState mvar) FailJob =
    do modifyMVarMasked_ mvar (return . (+1))
       v <- readMVar mvar
       if v > 1
          then return Success
          else return (Failure "FailJob fails")

data AlwaysFailJob = AlwaysFailJob deriving (Eq, Generic, Show)
data AlwaysFailState = AlwaysFailState { unAlwaysFailState :: MVar Int }
instance ToJSON AlwaysFailJob
instance FromJSON AlwaysFailJob
instance Job AlwaysFailState AlwaysFailJob where
  job (AlwaysFailState mvar) AlwaysFailJob =
    do modifyMVarMasked_ mvar (return . (+1))
       return (Failure "AlwaysFailJob fails")

data TimedJob = TimedJob Int deriving (Generic, Show, Eq)
data TimedState = TimedState { unTimedState :: MVar Int }
instance ToJSON TimedJob
instance FromJSON TimedJob
instance Job TimedState TimedJob where
  job (TimedState mvar) (TimedJob delay) =
    do threadDelay delay
       modifyMVarMasked_ mvar (return . (+1))
       return Success

data BigJob = BigJob T.Text deriving (Generic, Show, Eq)
data BigState = BigState { unBigState :: MVar Int }
instance ToJSON BigJob
instance FromJSON BigJob
instance Job BigState BigJob where
  job (BigState mvar) (BigJob _) =
    do modifyMVarMasked_ mvar (return . (+1))
       return Success

nullLogger :: Show a => a -> IO ()
nullLogger = const (return ())

print' :: Show a => a -> IO ()
print' a = do print a
              hFlush stdout

conf n s = (defaultHworkerConfig n s) {
               hwconfigLogger = nullLogger
             , hwconfigExceptionBehavior = FailOnException
             , hwconfigTimeout = 4
             }

main :: IO ()
main = hspec $
  do describe "Simple" $
       do it "should run and increment counter" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "simpleworker-1"
                                           (SimpleState mvar))
               wthread <- forkIO (worker hworker)
               queue hworker SimpleJob
               threadDelay 30000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 1 after job runs" 1 v
          it "queueing 2 jobs should increment twice" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "simpleworker-2"
                                           (SimpleState mvar))
               wthread <- forkIO (worker hworker)
               queue hworker SimpleJob
               queue hworker SimpleJob
               threadDelay 40000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2 after 2 jobs run" 2 v
          it "queueing 1000 jobs should increment 1000" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "simpleworker-3"
                                           (SimpleState mvar))
               wthread <- forkIO (worker hworker)
               replicateM_ 1000 (queue hworker SimpleJob)
               threadDelay 2000000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 1000 after 1000 job runs" 1000 v
          it "should work with multiple workers" $
          -- NOTE(dbp 2015-07-12): This probably won't run faster, because
          -- they are all blocking on the MVar, but that's not the point.
            do mvar <- newMVar 0
               hworker <- createWith (conf "simpleworker-4"
                                           (SimpleState mvar))
               wthread1 <- forkIO (worker hworker)
               wthread2 <- forkIO (worker hworker)
               wthread3 <- forkIO (worker hworker)
               wthread4 <- forkIO (worker hworker)
               replicateM_ 1000 (queue hworker SimpleJob)
               threadDelay 1000000
               killThread wthread1
               killThread wthread2
               killThread wthread3
               killThread wthread4
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 1000 after 1000 job runs" 1000 v

     describe "Exceptions" $
       do it "should be able to have exceptions thrown in jobs and retry the job" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "exworker-1"
                                           (ExState mvar)) {
                                       hwconfigExceptionBehavior =
                                         RetryOnException
                                     }
               wthread <- forkIO (worker hworker)
               queue hworker ExJob
               threadDelay 40000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2, since the first run failed" 2 v
          it "should not retry if mode is FailOnException" $
             do mvar <- newMVar 0
                hworker <- createWith (conf "exworker-2"
                                            (ExState mvar))
                wthread <- forkIO (worker hworker)
                queue hworker ExJob
                threadDelay 30000
                killThread wthread
                destroy hworker
                v <- takeMVar mvar
                assertEqual "State should be 1, since failing run wasn't retried" 1 v

     describe "Retry" $
       do it "should be able to return Retry and get run again" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "retryworker-1"
                                           (RetryState mvar))
               wthread <- forkIO (worker hworker)
               queue hworker RetryJob
               threadDelay 50000
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2, since it got retried" 2 v

     describe "Fail" $
       do it "should not retry a job that Fails" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "failworker-1"
                                           (FailState mvar))
               wthread <- forkIO (worker hworker)
               queue hworker FailJob
               threadDelay 30000
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 1, since failing run wasn't retried" 1 v
          it "should put a failed job into the failed queue" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "failworker-2"
                                           (FailState mvar))
               wthread <- forkIO (worker hworker)
               queue hworker FailJob
               threadDelay 30000
               jobs <- failed hworker
               destroy hworker
               assertEqual "Should have failed job" [FailJob] jobs
          it "should only store failedQueueSize failed jobs" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "failworker-3"
                                           (AlwaysFailState mvar)) {
                                     hwconfigFailedQueueSize = 2
                           }
               wthread <- forkIO (worker hworker)
               queue hworker AlwaysFailJob
               queue hworker AlwaysFailJob
               queue hworker AlwaysFailJob
               queue hworker AlwaysFailJob
               threadDelay 100000
               jobs <- failed hworker
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 4, since all jobs were run" 4 v
               assertEqual "Should only have stored 2"
                           [AlwaysFailJob,AlwaysFailJob] jobs
     describe "Monitor" $
       do it "should add job back after timeout" $
          -- NOTE(dbp 2015-07-12): The timing on this test is somewhat
          -- tricky.  We want to get the job started with one worker,
          -- then kill the worker, then start a new worker, and have
          -- the monitor put the job back in the queue and have the
          -- second worker finish it. It's important that the job
          -- takes less time to complete than the timeout for the
          -- monitor, or else it'll queue it forever.
          --
          -- The timeout is 5 seconds. The job takes 1 seconds to run.
          -- The worker is killed after 0.5 seconds, which should be
          -- plenty of time for it to have started the job. Then after
          -- the second worker is started, we wait 10 seconds, which
          -- should be plenty; we expect the total run to take around 11.
            do mvar <- newMVar 0
               hworker <- createWith (conf "timedworker-1"
                                           (TimedState mvar)) {
                                       hwconfigTimeout = 5
                                     }
               wthread1 <- forkIO (worker hworker)
               mthread <- forkIO (monitor hworker)
               queue hworker (TimedJob 1000000)
               threadDelay 500000
               killThread wthread1
               wthread2 <- forkIO (worker hworker)
               threadDelay 10000000
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2, since monitor thinks it failed" 2 v
          it "should add back multiple jobs after timeout" $
             -- NOTE(dbp 2015-07-23): Similar to the above test, but we
             -- have multiple jobs started, multiple workers killed.
             -- then one worker will finish both interrupted jobs.
            do mvar <- newMVar 0
               hworker <- createWith (conf "timedworker-2"
                                          (TimedState mvar)) {
                                       hwconfigTimeout = 5
                                     }
               wthread1 <- forkIO (worker hworker)
               wthread2 <- forkIO (worker hworker)
               mthread <- forkIO (monitor hworker)
               queue hworker (TimedJob 1000000)
               queue hworker (TimedJob 1000000)
               threadDelay 500000
               killThread wthread1
               killThread wthread2
               wthread3 <- forkIO (worker hworker)
               threadDelay 10000000
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 4, since monitor thinks first 2 failed" 4 v
          it "should work with multiple monitors" $
            do mvar <- newMVar 0
               hworker <- createWith (conf "timedworker-3"
                                          (TimedState mvar)) {
                                       hwconfigTimeout = 5
                                     }
               wthread1 <- forkIO (worker hworker)
               wthread2 <- forkIO (worker hworker)
               -- NOTE(dbp 2015-07-24): This might seem silly, but it
               -- was actually sufficient to expose a race condition.
               mthread1 <- forkIO (monitor hworker)
               mthread2 <- forkIO (monitor hworker)
               mthread3 <- forkIO (monitor hworker)
               mthread4 <- forkIO (monitor hworker)
               mthread5 <- forkIO (monitor hworker)
               mthread6 <- forkIO (monitor hworker)
               queue hworker (TimedJob 1000000)
               queue hworker (TimedJob 1000000)
               threadDelay 500000
               killThread wthread1
               killThread wthread2
               wthread3 <- forkIO (worker hworker)
               threadDelay 30000000
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 4, since monitor thinks first 2 failed" 4 v
               -- NOTE(dbp 2015-07-24): It would be really great to have a
               -- test that went after a race between the retry logic and
               -- the monitors (ie, assume that the job completed with
               -- Retry, and it happened to complete right at the timeout
               -- period).  I'm not sure if I could get that sort of
               -- precision without adding other delay mechanisms, or
               -- something to make it more deterministic.
     describe "Broken jobs" $
       it "should store broken jobs" $
         do -- NOTE(dbp 2015-08-09): The more common way this could
            -- happen is that you change your serialization format. But
            -- we can abuse this by creating two different workers
            -- pointing to the same queue, and submit jobs in one, try
            -- to run them in another, where the types are different.
            mvar <- newMVar 0
            hworker1 <- createWith (conf "broken-1"
                                           (TimedState mvar)) {
                                        hwconfigTimeout = 5
                                      }
            hworker2 <- createWith (conf "broken-1"
                                           (SimpleState mvar)) {
                                        hwconfigTimeout = 5
                                      }
            wthread <- forkIO (worker hworker1)
            queue hworker2 SimpleJob
            threadDelay 100000
            jobs <- broken hworker2
            killThread wthread
            destroy hworker1
            v <- takeMVar mvar
            assertEqual "State should be 0, as nothing should have happened" 0 v
            assertEqual "Should be one broken job, as serialization is wrong" 1 (length jobs)
     describe "Dump jobs" $ do
       it "should return the job that was queued" $
         do mvar <- newMVar 0
            hworker <- createWith (conf "dump-1"
                                              (SimpleState mvar)) {
                                       hwconfigTimeout = 5
                                     }
            queue hworker SimpleJob
            res <- jobs hworker
            destroy hworker
            assertEqual "Should be [SimpleJob]" [SimpleJob] res
       it "should return jobs in order (most recently added at front; worker pulls from back)" $
         do mvar <- newMVar 0
            hworker <- createWith (conf "dump-2"
                                       (TimedState mvar)) {
                                    hwconfigTimeout = 5
                                  }
            queue hworker (TimedJob 1)
            queue hworker (TimedJob 2)
            res <- jobs hworker
            destroy hworker
            assertEqual "Should by [TimedJob 2, TimedJob 1]" [TimedJob 2, TimedJob 1] res
     describe "Large jobs" $ do
       it "should be able to deal with lots of large jobs" $
         do mvar <- newMVar 0
            hworker <- createWith (conf "big-1"
                                       (BigState mvar))
            wthread1 <- forkIO (worker hworker)
            wthread2 <- forkIO (worker hworker)
            wthread3 <- forkIO (worker hworker)
            wthread4 <- forkIO (worker hworker)
            let content = T.intercalate "\n" (take 1000 (repeat "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"))
            replicateM_ 5000 (queue hworker (BigJob content))
            threadDelay 10000000
            killThread wthread1
            killThread wthread2
            killThread wthread3
            killThread wthread4
            destroy hworker
            v <- takeMVar mvar
            assertEqual "Should have processed 5000" 5000 v
