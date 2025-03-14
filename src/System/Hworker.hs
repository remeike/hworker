{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StrictData                 #-}

{-|

This module contains an at-least-once persistent job processing queue
backed by Redis. It depends upon Redis not losing data once it has
acknowledged it, and guaranteeing the atomicity that is specified for
commands like EVAL (ie, that if you do several things within an EVAL,
they will all happen or none will happen). Nothing has been tested
with Redis clusters (and it likely will not work).

An example use is the following (see the repository for a
slightly expanded version; also, the test cases in the repository are
also good examples):


> data PrintJob = Print deriving (Generic, Show)
> data State = State (MVar Int)
> instance ToJSON PrintJob
> instance FromJSON PrintJob
>
> instance Job State PrintJob where
>   job Hworker { hworkerState = State mvar } Print =
>     do v <- takeMVar mvar
>        putMVar mvar (v + 1)
>        putStrLn $ "A(" ++ show v ++ ")"
>        return Success
>
> main = do mvar <- newMVar 0
>           hworker <- create "printer" (State mvar)
>           forkIO (worker hworker)
>           forkIO (monitor hworker)
>           forkIO (forever $ queue hworker Print >> threadDelay 1000000)
>           forever (threadDelay 1000000)


-}

module System.Hworker
  ( -- * Types
    Result(..)
  , Job(..)
  , Hworker(..)
  , HworkerConfig(..)
  , defaultHworkerConfig
  , ExceptionBehavior(..)
  , RedisConnection(..)
  , BatchId(..)
  , BatchStatus(..)
  , BatchSummary(..)
  , CronJob(..)
  , QueueingResult(..)
  , StreamingResult(..)
    -- * Managing Workers
  , create
  , createWith
  , destroy
  , worker
  , WorkerResult(..)
  , execWorker
  , scheduler
  , execScheduler
  , monitor
  , JobProgress(..)
  , execMonitor
    -- * Queuing Jobs
  , queue
  , queuePriority
  , queueScheduled
  , queueBatch
  , streamBatch
  , streamBatchTx
  , initBatch
  , cancelBatch
  , pauseBatch
  , resumeBatch
  , stopBatchQueueing
    -- * Cron Jobs
  , initCron
  , queueCron
  , requeueCron
  , checkCron
    -- * Inspecting Workers
  , listJobs
  , listFailed
  , listScheduled
  , countPaused
  , getCronProcessing
  , broken
  , batchSummary
    -- * Debugging Utilities
  , debugger
  , batchCounter
  ) where

--------------------------------------------------------------------------------
import           Control.Arrow           ( second)
import           Control.Concurrent      ( threadDelay)
import           Control.Exception       ( SomeException
                                         , Exception
                                         , throw
                                         , catch
                                         , catchJust
                                         , asyncExceptionFromException
                                         , AsyncException
                                         )
import           Control.Monad           ( forM_, forever, void, when )
import           Control.Monad.Trans     ( liftIO, lift )
import           Data.Aeson              ( FromJSON, ToJSON, (.=), (.:), (.:?) )
import qualified Data.Aeson             as A
import           Data.ByteString         ( ByteString )
import qualified Data.ByteString.Char8  as B8
import qualified Data.ByteString.Lazy   as LB
import           Data.Conduit            ( ConduitT )
import qualified Data.Conduit           as Conduit
import           Data.Either             ( isRight )
import           Data.Maybe              ( fromMaybe
                                         , isJust
                                         , listToMaybe
                                         , mapMaybe
                                         )
import           Data.Text               ( Text )
import qualified Data.Text              as T
import qualified Data.Text.Encoding     as T
import           Data.Time.Clock         ( NominalDiffTime
                                         , UTCTime(..)
                                         , diffUTCTime
                                         , getCurrentTime
                                         )
import qualified Data.Time.Clock.POSIX  as Posix
import           Data.UUID               ( UUID )
import qualified Data.UUID              as UUID
import qualified Data.UUID.V4           as UUID
import           Database.Redis          ( Redis
                                         , RedisTx
                                         , TxResult(..)
                                         , Connection
                                         , ConnectInfo
                                         , runRedis
                                         )
import qualified Database.Redis         as R
import           GHC.Generics            ( Generic )
import           Saturn                  ( Schedule )
import qualified Saturn                 as Schedule
--------------------------------------------------------------------------------



-- | Jobs can return 'Success', 'Retry' (with a message), or 'Failure'
-- (with a message). Jobs that return 'Failure' are stored in the
-- 'failed' queue and are not re-run. Jobs that return 'Retry' are re-run.

data Result
  = Success
  | Retry Text
  | Failure Text
  deriving (Generic, Eq, Show)


instance ToJSON Result
instance FromJSON Result


-- | Each Worker that you create will be responsible for one type of
-- job, defined by a 'Job' instance.
--
-- The job can do many different things (as the value can be a
-- variant), but be careful not to break deserialization if you add
-- new things it can do.
--
-- The job will take some state (passed as the `s` parameter), which
-- does not vary based on the job, and the actual job data
-- structure. The data structure (the `t` parameter) will be stored
-- and copied a few times in Redis while in the lifecycle, so
-- generally it is a good idea for it to be relatively small (and have
-- it be able to look up data that it needs while the job is running).
--
-- Finally, while deriving FromJSON and ToJSON instances automatically
-- might seem like a good idea, you will most likely be better off
-- defining them manually, so you can make sure they are backwards
-- compatible if you change them, as any jobs that can't be
-- deserialized will not be run (and will end up in the 'broken'
-- queue). This will only happen if the queue is non-empty when you
-- replace the running application version, but this is obviously
-- possible and could be likely depending on your use.

class (FromJSON t, ToJSON t, Show t) => Job s t | s -> t where
  job :: Hworker s t -> t -> IO Result


-- | What should happen when an unexpected exception is thrown in a
-- job - it can be treated as either a 'Failure' (the default) or a
-- 'Retry' (if you know the only exceptions are triggered by
-- intermittent problems).

data ExceptionBehavior
  = RetryOnException
  | FailOnException


type JobId = Text


-- | A unique identifier for grouping jobs together.

newtype BatchId =
  BatchId UUID
  deriving (ToJSON, FromJSON, Eq, Show)


-- | The result of a batch of jobs queued atomically.

data QueueingResult
  = BatchNotFound BatchId
  | TransactionAborted BatchId Int
  | QueueingSuccess BatchSummary
  | QueueingFailed BatchId Int Text
  | AlreadyQueued BatchSummary
  deriving (Eq, Show)


-- | The return value of a batch of jobs that are streamed in.

data StreamingResult
  = StreamingOk            -- End the stream successfully
  | StreamingAborted Text  -- Close the stream with the given error message,
                           -- reverting all previously added jobs

-- | Represents a recurring job that executes on a particular schedule.

data CronJob t =
  CronJob Text t Schedule


-- | Represents the current status of a batch. A batch is considered to be
-- "queueing" if jobs can still be added to the batch. While jobs are
-- queueing it is possible for them to be "processing" during that time.
-- The status only changes to "processing" once jobs can no longer be queued
-- but are still being processed. The batch is then finished once all jobs
-- are processed (they have either failed or succeeded).

data BatchStatus
  = BatchQueueing
  | BatchFailed
  | BatchProcessing
  | BatchFinished
  | BatchCanceled
  | BatchPaused
  deriving (Eq, Show)


-- |  A summary of a particular batch, including figures on the total number
-- of jobs queued, the number of jobs that have completed (i.e.
-- failed or succeeded), the number of jobs succeeded, the number of jobs
-- failed, the number of jobs retried, and the current status of the
-- batch overall.

data BatchSummary =
  BatchSummary
    { batchSummaryID        :: BatchId
    , batchSummaryQueued    :: Int
    , batchSummaryCompleted :: Int
    , batchSummarySuccesses :: Int
    , batchSummaryFailures  :: Int
    , batchSummaryRetries   :: Int
    , batchSummaryCanceled  :: Int
    , batchSummaryStatus    :: BatchStatus
    } deriving (Eq, Show)


data JobRef =
  JobRef JobId (Maybe BatchId) (Maybe Text)
  deriving (Eq, Show)


instance ToJSON JobRef where
  toJSON (JobRef j b s) =
    A.object ["j" .= j, "b" .= b, "s" .= s]


instance FromJSON JobRef where
  -- NOTE(rjbf 2022-11-19): This is just here for the sake of migration and
  -- can be removed eventually. Before `JobRef`, which is encoded as
  -- a JSON object, there was a just a `String` representing the job ID.

  parseJSON (A.String j) = pure (JobRef j Nothing Nothing)
  parseJSON val = A.withObject "JobRef" (\o -> JobRef <$> o .: "j" <*> o .: "b" <*> o .:? "s") val


hwlog :: Show a => Hworker s t -> a -> IO ()
hwlog hw a =
  hworkerLogger hw (hworkerName hw, a)


-- | The worker data type - it is parametrized be the worker
-- state (the `s`) and the job type (the `t`).

data Hworker s t =
  Hworker
    { hworkerName              :: ByteString
    , hworkerState             :: s
    , hworkerConnection        :: Connection
    , hworkerExceptionBehavior :: ExceptionBehavior
    , hworkerLogger            :: forall a. Show a => a -> IO ()
    , hworkerJobTimeout        :: NominalDiffTime
    , hworkerFailedQueueSize   :: Int
    , hworkerDebug             :: Bool
    , hworkerBatchCompleted    :: BatchSummary -> IO ()
    }


-- | When configuring a worker, you can tell it to use an existing
-- redis connection pool (which you may have for the rest of your
-- application). Otherwise, you can specify connection info. By
-- default, hworker tries to connect to localhost, which may not be
-- true for your production application.

data RedisConnection
  = RedisConnectInfo ConnectInfo
  | RedisConnection Connection


-- | The main configuration for workers.
--
-- Each pool of workers should have a unique `hwconfigName`, as the
-- queues are set up by that name, and if you have different types of
-- data written in, they will likely be unable to be deserialized (and
-- thus could end up in the 'broken' queue).
--
-- The 'hwconfigLogger' defaults to writing to stdout, so you will
-- likely want to replace that with something appropriate (like from a
-- logging package).
--
-- The `hwconfigTimeout` is really important. It determines the length
-- of time after a job is started before the 'monitor' will decide
-- that the job must have died and will restart it. If it is shorter
-- than the length of time that a normal job takes to complete, the
-- jobs _will_ be run multiple times. This is _semantically_ okay, as
-- this is an at-least-once processor, but obviously won't be
-- desirable. It defaults to 120 seconds.
--
-- The 'hwconfigExceptionBehavior' controls what happens when an
-- exception is thrown within a job.
--
-- 'hwconfigFailedQueueSize' controls how many 'failed' jobs will be
-- kept. It defaults to 1000.

data HworkerConfig s t =
  HworkerConfig
    { hwconfigName              :: Text
    , hwconfigState             :: s
    , hwconfigRedisConnectInfo  :: RedisConnection
    , hwconfigExceptionBehavior :: ExceptionBehavior
    , hwconfigLogger            :: forall a. Show a => a -> IO ()
    , hwconfigTimeout           :: NominalDiffTime
    , hwconfigFailedQueueSize   :: Int
    , hwconfigDebug             :: Bool
    , hwconfigBatchCompleted    :: BatchSummary -> IO ()
    , hwconfigCronJobs          :: [CronJob t]
    }


-- | The default worker config - it needs a name and a state (as those
-- will always be unique).

defaultHworkerConfig :: Text -> s -> HworkerConfig s t
defaultHworkerConfig name state =
  HworkerConfig
    { hwconfigName              = name
    , hwconfigState             = state
    , hwconfigRedisConnectInfo  = RedisConnectInfo R.defaultConnectInfo
    , hwconfigExceptionBehavior = FailOnException
    , hwconfigLogger            = print
    , hwconfigTimeout           = 120
    , hwconfigFailedQueueSize   = 1000
    , hwconfigDebug             = False
    , hwconfigBatchCompleted    = const (return ())
    , hwconfigCronJobs          = []
    }


-- | Create a new worker with the default 'HworkerConfig'.
--
-- Note that you must create at least one 'worker' and 'monitor' for
-- the queue to actually process jobs (and for it to retry ones that
-- time-out).

create :: Job s t => Text -> s -> IO (Hworker s t)
create name state =
  createWith (defaultHworkerConfig name state)


-- | Create a new worker with a specified 'HworkerConfig'.
--
-- Note that you must create at least one 'worker' and 'monitor' for
-- the queue to actually process jobs (and for it to retry ones that
-- time-out).

createWith :: Job s t => HworkerConfig s t -> IO (Hworker s t)
createWith HworkerConfig{..} = do
  conn <-
    case hwconfigRedisConnectInfo of
      RedisConnectInfo c -> R.connect c
      RedisConnection  c -> return c

  let
    hworker =
      Hworker
        { hworkerName              = T.encodeUtf8 hwconfigName
        , hworkerState             = hwconfigState
        , hworkerConnection        = conn
        , hworkerExceptionBehavior = hwconfigExceptionBehavior
        , hworkerLogger            = hwconfigLogger
        , hworkerJobTimeout        = hwconfigTimeout
        , hworkerFailedQueueSize   = hwconfigFailedQueueSize
        , hworkerDebug             = hwconfigDebug
        , hworkerBatchCompleted    = hwconfigBatchCompleted
        }

  time <- getCurrentTime
  initCron hworker time hwconfigCronJobs
  return hworker


-- | Destroy a worker. This will delete all the queues, clearing out
-- all existing 'jobs', the 'broken' and 'failed' queues, and the hashes for
-- batched jobs. There is no need to do this in normal applications
-- (and most likely, you won't want to).

destroy :: Job s t => Hworker s t -> IO ()
destroy hw =
  void $ runRedis (hworkerConnection hw) $ do
    R.keys (batchCounterPrefix hw <> "*") >>=
      \case
        Left  err  -> liftIO $ hwlog hw err
        Right keys -> void $ R.del keys

    R.keys (batchPausedPrefix hw <> "*") >>=
      \case
        Left  err  -> liftIO $ hwlog hw err
        Right keys -> void $ R.del keys

    R.del
      [ jobQueue hw
      , priorityQueue hw
      , progressQueue hw
      , brokenQueue hw
      , failedQueue hw
      , scheduleQueue hw
      , cronSchedule hw
      , cronProcessing hw
      ]


jobQueue :: Hworker s t -> ByteString
jobQueue hw =
  "hworker-jobs-" <> hworkerName hw


priorityQueue :: Hworker s t -> ByteString
priorityQueue hw =
  "hworker-priority-jobs-" <> hworkerName hw


progressQueue :: Hworker s t -> ByteString
progressQueue hw =
  "hworker-progress-" <> hworkerName hw


brokenQueue :: Hworker s t -> ByteString
brokenQueue hw =
  "hworker-broken-" <> hworkerName hw


failedQueue :: Hworker s t -> ByteString
failedQueue hw =
  "hworker-failed-" <> hworkerName hw


scheduleQueue :: Hworker s t -> ByteString
scheduleQueue hw =
  "hworker-scheduled-" <> hworkerName hw


batchCounter :: Hworker s t -> BatchId -> ByteString
batchCounter hw (BatchId batch) =
  batchCounterPrefix hw <> UUID.toASCIIBytes batch


batchCounterPrefix :: Hworker s t -> ByteString
batchCounterPrefix hw =
  "hworker-batch-" <> hworkerName hw <> ":"


batchPaused :: Hworker s t -> BatchId -> ByteString
batchPaused hw (BatchId batch) =
  batchPausedPrefix hw <> UUID.toASCIIBytes batch


batchPausedPrefix :: Hworker s t -> ByteString
batchPausedPrefix hw =
  "hworker-batch-paused-jobs-" <> hworkerName hw <> ":"


cronSchedule :: Hworker s t -> ByteString
cronSchedule hw  =
  "hworker-cron-schedule-" <> hworkerName hw


cronProcessing :: Hworker s t -> ByteString
cronProcessing hw  =
  "hworker-cron-processing-" <> hworkerName hw


cronId :: Text -> Text
cronId cron =
  "cron:" <> cron


-- | Adds a job to the queue. Returns whether the operation succeeded.

queue :: Job s t => Hworker s t -> t -> IO Bool
queue hw j = do
  jobId <- UUID.toText <$> UUID.nextRandom
  result <-
    runRedis (hworkerConnection hw)
      $ R.lpush (jobQueue hw)
      $ [LB.toStrict $ A.encode (JobRef jobId Nothing Nothing, j)]
  return $ isRight result


-- | Adds a job to the priority queue. Returns whether the operation succeeded.

queuePriority :: Job s t => Hworker s t -> t -> IO Bool
queuePriority hw j = do
  jobId <- UUID.toText <$> UUID.nextRandom
  result <-
    runRedis (hworkerConnection hw)
      $ R.lpush (priorityQueue hw)
      $ [LB.toStrict $ A.encode (JobRef jobId Nothing Nothing, j)]
  return $ isRight result


-- | Initializes all cron jobs. This is will add all of the cron schedules
-- if not already present or update the schedules if they are.

initCron :: Job s t => Hworker s t -> UTCTime -> [CronJob t] -> IO ()
initCron hw time cronJobs = do
  void
    $ runRedis (hworkerConnection hw)
    $ R.hmset (cronSchedule hw)
    $ fmap
        ( \(CronJob cron _ schedule) ->
            (T.encodeUtf8 cron, T.encodeUtf8 $ Schedule.toText schedule)
        )
        cronJobs

  mapM_ (queueCron hw time) cronJobs


-- | Queues a cron job for the first time, adding it to the schedule queue
-- at its next scheduled time.

queueCron :: Job s t => Hworker s t -> UTCTime -> CronJob t -> IO Bool
queueCron hw time (CronJob cron j schedule) =
  case Schedule.nextMatch time schedule of
    Nothing ->
      return False

    Just utc -> do
      result <-
        runRedis (hworkerConnection hw) $ R.zadd (scheduleQueue hw) $
          [ ( utcToDouble utc
            , LB.toStrict $ A.encode (JobRef (cronId cron) Nothing (Just cron), j)
            )
          ]
      return $ isRight result


-- | Re-enqueues cron job, removing the record from the cron processing hash
-- and adding it back to the schedule queue at its next scheduled time.

requeueCron :: Job s t => Hworker s t -> Text -> t -> IO ()
requeueCron hw cron j =
  runRedis (hworkerConnection hw) $ do
    void $ withInt hw $ R.hdel (cronProcessing hw) [T.encodeUtf8 cron]
    R.hget (cronSchedule hw) (T.encodeUtf8 cron) >>=
      \case
        Left err ->
          liftIO $ hwlog hw err

        Right Nothing ->
          -- This can happen if the scheduled changed between the job
          -- being queued for execution and then being requeued
          liftIO $ hwlog hw $ "CRON NOT FOUND: " <> cron

        Right (Just field) ->
          case Schedule.fromText (T.decodeUtf8 field) of
            Left err ->
              liftIO $ hwlog hw err

            Right schedule -> do
              time <- liftIO getCurrentTime

              case Schedule.nextMatch time schedule of
                Nothing ->
                  liftIO $ hwlog hw $ "CRON SCHEDULE NOT FOUND: " <> field

                Just utc ->
                  void $ withInt hw $ R.zadd (scheduleQueue hw) $
                    [ ( utcToDouble utc
                      , LB.toStrict $ A.encode (JobRef (cronId cron) Nothing (Just cron), j)
                      )
                    ]


-- | Checks if the there is a already a job for a particular cron process
-- which is either scheduled or currently being processed.

checkCron :: forall s t. Job s t => Hworker s t -> Text -> IO Bool
checkCron hw cron =
  runRedis (hworkerConnection hw) $ do
    R.hget (cronProcessing hw) (T.encodeUtf8 cron) >>=
      \case
        Right (Just _) ->
          return True

        _ ->
          R.zrange (scheduleQueue hw) 0 (-1) >>=
            \case
              Left err -> do
                liftIO $ hwlog hw err
                return False

              Right ls ->
                case traverse A.decodeStrict ls :: Maybe [(JobRef, t)] of
                  Just scheduledJobs ->
                    return
                      $ any (\(JobRef _ _ c, _) -> c == Just cron)
                      $ scheduledJobs

                  Nothing ->
                    return False


-- | Adds a job to be added to the queue at the specified time.
-- Returns whether the operation succeeded.

queueScheduled :: Job s t => Hworker s t -> t -> UTCTime -> IO Bool
queueScheduled hw j utc = do
  jobId <- UUID.toText <$> UUID.nextRandom
  result <-
    runRedis (hworkerConnection hw)
      $ R.zadd (scheduleQueue hw)
      $ [(utcToDouble utc, LB.toStrict $ A.encode (JobRef jobId Nothing Nothing, j))]
  return $ isRight result


-- | Adds jobs to the queue, but as part of a particular batch of jobs.
-- It takes the `BatchId` of the specified job, a `Bool` that when `True`
-- closes the batch to further queueing, and a list of jobs to be queued, and
-- returns whether the operation succeeded. The process is atomic
-- so that if a single job fails to queue then then none of the jobs
-- in the list will queue.

queueBatch :: Job s t => Hworker s t -> BatchId -> Bool -> [t] -> IO QueueingResult
queueBatch hw batch close js =
  withBatchQueue hw batch $ runRedis (hworkerConnection hw) $
    R.multiExec $ do
      mapM_
        ( \j -> do
            jobId <- UUID.toText <$> liftIO UUID.nextRandom
            let ref = JobRef jobId (Just batch) Nothing
            _ <- R.lpush (jobQueue hw) [LB.toStrict $ A.encode (ref, j)]

            -- Do the counting outside of the transaction, hence runRedis here.
            liftIO
              $ runRedis (hworkerConnection hw)
              $ R.hincrby (batchCounter hw batch) "queued" 1
        )
        js

      when close
        $ void
        $ R.hset (batchCounter hw batch) "status" "processing"

      return (pure ())


data AbortException =
  AbortException Text
  deriving Show


instance Exception AbortException


-- | Like 'queueBatch', but instead of a list of jobs, it takes a conduit
-- that streams jobs within IO.

streamBatch ::
  Job s t =>
  Hworker s t -> BatchId -> Bool -> ConduitT () t IO StreamingResult ->
  IO QueueingResult
streamBatch hw batch close producer =
  streamBatchTx hw batch close $ Conduit.transPipe liftIO producer


-- | Like 'streamBatch', but instead of IO, jobs are streamed directly within
-- a Redis transaction.

streamBatchTx ::
  Job s t =>
  Hworker s t -> BatchId -> Bool -> ConduitT () t RedisTx StreamingResult ->
  IO QueueingResult
streamBatchTx hw batch close producer =
  let
    sink =
      Conduit.await >>=
        \case
          Nothing ->
            liftIO (batchSummary hw batch) >>=
              \case
                Just summary | batchSummaryQueued summary == 0 ->
                  void . lift $ R.hset (batchCounter hw batch) "status" "finished"

                _ ->
                  when close
                    $ void . lift
                    $ R.hset (batchCounter hw batch) "status" "processing"

          Just j -> do
            jobId <- UUID.toText <$> liftIO UUID.nextRandom
            let ref = JobRef jobId (Just batch) Nothing
            _ <- lift $ R.lpush (jobQueue hw) [LB.toStrict $ A.encode (ref, j)]

            -- Do the counting outside of the transaction, hence runRedis here.
            _ <-
              liftIO
                $ runRedis (hworkerConnection hw)
                $ R.hincrby (batchCounter hw batch) "queued" 1

            sink

    run =
      Conduit.runConduit (Conduit.fuseUpstream producer sink) >>=
        \case
          StreamingOk          -> return (pure ())
          StreamingAborted err -> throw (AbortException err)
  in
  withBatchQueue hw batch
    $ runRedis (hworkerConnection hw)
    $ R.multiExec run


withBatchQueue ::
  Job s t => Hworker s t -> BatchId -> IO (TxResult ()) -> IO QueueingResult
withBatchQueue hw batch process =
  runRedis (hworkerConnection hw) (batchSummary' hw batch) >>=
    \case
      Nothing ->
        return $ BatchNotFound batch

      Just summary | batchSummaryStatus summary == BatchQueueing ->
        catch
          ( catch
              ( process >>=
                  \case
                    TxSuccess () ->
                      return $ QueueingSuccess summary

                    TxAborted -> do
                      n <- runRedis (hworkerConnection hw) $ failBatchSummary hw batch
                      return $ TransactionAborted batch n

                    TxError err -> do
                      n <- runRedis (hworkerConnection hw) $ failBatchSummary hw batch
                      return $ QueueingFailed batch n (T.pack err)
              )
              ( \(AbortException msg :: AbortException) -> do
                  n <- runRedis (hworkerConnection hw) (failBatchSummary hw batch)
                  return $ QueueingFailed batch n msg
              )
          )
          ( \(e :: SomeException) -> do
              n <- runRedis (hworkerConnection hw) (failBatchSummary hw batch)
              return $ QueueingFailed batch n (T.pack (show e))
          )

      Just summary ->
        return $ AlreadyQueued summary


-- | Prevents queueing new jobs to a batch. If the number of jobs completed equals
-- the number of jobs queued, then the status of the batch is immediately set
-- to `BatchFinished`, otherwise it's set to `BatchProcessing`.

stopBatchQueueing :: Hworker s t -> BatchId -> IO ()
stopBatchQueueing hw batch =
  runRedis (hworkerConnection hw) $ do
    batchSummary' hw batch >>=
      \case
        Nothing ->
          liftIO $ hwlog hw $ "Batch not found: " <> show batch

        Just summary | batchSummaryCompleted summary >= batchSummaryQueued summary ->
          void
            $ R.hset (batchCounter hw batch) "status"
            $ encodeBatchStatus BatchFinished

        Just _->
          void
            $ R.hset (batchCounter hw batch) "status"
            $ encodeBatchStatus BatchProcessing


-- | Creates a new worker thread. This is blocking, so you will want to
-- 'forkIO' this into a thread. You can have any number of these (and
-- on any number of servers); the more there are, the faster jobs will
-- be processed.

worker :: Job s t => Hworker s t -> IO ()
worker hw =
  forever $
    execWorker hw >>=
      \case
        NoJobs           -> threadDelay 10000
        DequeueError _   -> threadDelay 10000
        BrokenJob _      -> threadDelay 10000
        CanceledJob _    -> return ()
        PausedJob        -> return ()
        CompletedJob _ _ -> return ()


-- | The result of a single worker operation.

data WorkerResult t
  = NoJobs
  | DequeueError Text
  | BrokenJob ByteString
  | CanceledJob Bool
  | PausedJob
  | CompletedJob t Result
  deriving (Eq, Show)


-- | Removes one job from the queue, if there are any, and executes it.
-- This can be useful for testing, as you don't have to fork an entire worker
-- thread.

execWorker :: Job s t => Hworker s t -> IO (WorkerResult t)
execWorker hw =
  let
    runJob action = do
      eitherResult <-
        catchJust
          ( \(e :: SomeException) ->
              if isJust (asyncExceptionFromException e :: Maybe AsyncException)
                then Nothing
                else Just e
          )
          ( Right <$> action )
          ( return . Left )

      case eitherResult of
        Left exception ->
          let
            resultMessage =
              case hworkerExceptionBehavior hw of
                RetryOnException -> Retry
                FailOnException  -> Failure
          in
          return
            $ resultMessage
            $ "Exception raised: " <> (T.pack . show) exception

        Right result ->
          return result
  in do
  now <- getCurrentTime

  eitherReply <-
    -- NOTE(rjbf 2025-01-23): We're using `brpop` here just to get a job from
    -- either of the job queues. Blocking commands do not actually block
    -- inside Redis transactions so the timeout here has no effect.
    runRedis (hworkerConnection hw) $
      R.eval
        "local job = redis.call('brpop', KEYS[1], KEYS[2], '0.0001')\n\
        \if job then\n\
        \  local status = nil\n\
        \  local batch = cjson.decode(job[2])[1]['b']\n\
        \  local summary = ''\n\
        \  if batch ~= nil then \n\
        \    summary = KEYS[4] .. tostring(batch)\n\
        \    status = redis.call('hget', summary, 'status')\n\
        \  end\n\
        \  if tostring(status) == 'canceled' then\n\
        \    redis.call('hincrby', summary, 'canceled', '1')\n\
        \    local completed = redis.call('hincrby', summary, 'completed', '1')\n\
        \    local queued = redis.call('hincrby', summary, 'queued', '0')\n\
        \    if tonumber(completed) >= tonumber(queued) then\n\
        \      status = 'canceled'\n\
        \    else\n\
        \      status = 'canceling'\n\
        \    end \n\
        \  elseif tostring(status) == 'paused' then\n\
        \    redis.call('lpush', KEYS[5] .. tostring(batch), tostring(job[2]))\n\
        \  else\n\
        \    redis.call('hset', KEYS[3], tostring(job[2]), ARGV[1])\n\
        \  end\n\
        \  return { job[2], tostring(status), summary }\n\
        \else\n\
        \  return nil\n\
        \end"
        [ priorityQueue hw
        , jobQueue hw
        , progressQueue hw
        , batchCounterPrefix hw
        , batchPausedPrefix hw
        ]
        [ LB.toStrict $ A.encode now ]

  case eitherReply of
    Left err -> do
      hwlog hw err
      return $ DequeueError $ T.pack $ show err

    Right Nothing ->
      return NoJobs

    Right (Just [t, b, _]) | b == "paused" -> do
      when (hworkerDebug hw) $ hwlog hw ("PAUSED JOB" :: Text, t)
      return PausedJob

    Right (Just [t, b, _]) | b == "canceling" -> do
      when (hworkerDebug hw) $ hwlog hw ("CANCELED JOB" :: Text, t)
      return $ CanceledJob False

    Right (Just [t, b, k]) | b == "canceled" -> do
      when (hworkerDebug hw) $ hwlog hw ("CANCELED JOB" :: Text, t)

      case UUID.fromASCIIBytes k of
        Nothing ->
          return ()

        Just uuid ->
          batchSummary hw (BatchId uuid) >>=
            \case
              Nothing      -> return ()
              Just summary -> hworkerBatchCompleted hw summary

      return $ CanceledJob True

    Right (Just [t, _, _]) -> do
      when (hworkerDebug hw) $ hwlog hw ("WORKER RUNNING" :: Text, t)

      case A.decodeStrict t of
        Nothing -> do
          hwlog hw ("BROKEN JOB" :: Text, t)
          now' <- getCurrentTime

          runWithNil hw $
            R.eval
              "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
              \if del == 1 then\n\
              \  redis.call('hset', KEYS[2], ARGV[1], ARGV[2])\n\
              \end\n\
              \return nil"
              [progressQueue hw, brokenQueue hw]
              [t, LB.toStrict $ A.encode now']

          return $ BrokenJob t

        Just (JobRef _ maybeBatch maybeCron, j) ->
          let
            nextCron =
              case maybeCron of
                Just cron -> requeueCron hw cron j
                Nothing   -> return ()
          in do
          runJob (job hw j) >>=
            \case
              Success -> do
                when (hworkerDebug hw) $ hwlog hw ("JOB COMPLETE" :: Text, t)
                case maybeBatch of
                  Nothing -> do
                    deletionResult <-
                      runRedis (hworkerConnection hw)
                        $ R.hdel (progressQueue hw) [t]
                    nextCron

                    case deletionResult of
                      Left err -> hwlog hw err >> return ()
                      Right 1  -> return ()
                      Right n  -> hwlog hw ("Job done: did not delete 1, deleted " <> show n)

                    return $ CompletedJob j Success

                  Just batch -> do
                    runWithMaybe hw
                      ( R.eval
                          "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                          \if del == 1 then\n\
                          \  local batch = KEYS[2]\n\
                          \  redis.call('hincrby', batch, 'successes', '1')\n\
                          \  local completed = redis.call('hincrby', batch, 'completed', '1')\n\
                          \  local queued = redis.call('hincrby', batch, 'queued', '0')\n\
                          \  local status = redis.call('hget', batch, 'status')\n\
                          \  if tonumber(completed) >= tonumber(queued) and status == 'processing' then\n\
                          \    redis.call('hset', batch, 'status', 'finished')\n\
                          \  end\n\
                          \  return redis.call('hgetall', batch)\
                          \end\n\
                          \return nil"
                          [progressQueue hw, batchCounter hw batch]
                          [t]
                      )
                      ( \hm -> do
                          nextCron
                          case decodeBatchSummary batch hm of
                            Nothing -> do
                              hwlog hw ("Job done: did not delete 1" :: Text)

                            Just summary -> do
                              when (batchSummaryStatus summary == BatchFinished)
                                $ hworkerBatchCompleted hw summary
                      )

                    return $ CompletedJob j Success

              Retry msg -> do
                hwlog hw ("RETRY: " <> msg)

                case maybeBatch of
                  Nothing ->
                    runWithNil hw $
                      R.eval
                        "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                        \if del == 1 then\n\
                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                        \end\n\
                        \return nil"
                        [progressQueue hw, jobQueue hw]
                        [t]

                  Just batch ->
                    runWithNil hw $
                      R.eval
                        "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                        \if del == 1 then\n\
                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                        \  redis.call('hincrby', KEYS[3], 'retries', '1')\n\
                        \end\n\
                        \return nil"
                        [progressQueue hw, jobQueue hw, batchCounter hw batch]
                        [t]

                return $ CompletedJob j $ Retry msg

              Failure msg -> do
                hwlog hw ("Failure: " <> msg)

                case maybeBatch of
                  Nothing ->
                    runWithNil hw $
                      R.eval
                        "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                        \if del == 1 then\n\
                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                        \  redis.call('ltrim', KEYS[2], 0, ARGV[2])\n\
                        \end\n\
                        \return nil"
                        [progressQueue hw, failedQueue hw]
                        [t, B8.pack (show (hworkerFailedQueueSize hw - 1))]

                  Just batch ->
                    runWithMaybe hw
                      ( R.eval
                          "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                          \if del == 1 then\n\
                          \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                          \  redis.call('ltrim', KEYS[2], 0, ARGV[2])\n\
                          \  local batch = KEYS[3]\n\
                          \  redis.call('hincrby', batch, 'failures', '1')\n\
                          \  local completed = redis.call('hincrby', batch, 'completed', '1')\n\
                          \  local queued = redis.call('hincrby', batch, 'queued', '0')\n\
                          \  local status = redis.call('hget', batch, 'status')\n\
                          \  if tonumber(completed) >= tonumber(queued) and status == 'processing' then\n\
                          \    redis.call('hset', batch, 'status', 'finished')\n\
                          \    return redis.call('hgetall', batch)\
                          \  end\n\
                          \end\n\
                          \return nil"
                          [progressQueue hw, failedQueue hw, batchCounter hw batch]
                          [t, B8.pack (show (hworkerFailedQueueSize hw - 1))]
                      )
                      ( \hm ->
                          forM_ (decodeBatchSummary batch hm)
                            $ hworkerBatchCompleted hw
                      )

                nextCron
                return $ CompletedJob j $ Failure msg

    Right (Just ls) ->
      return
        $ DequeueError
        $ "Unexpected script result: " <> T.pack (show ls)


-- | Start a scheduler. Like 'worker', this is blocking, so should be
-- started in a thread. This is responsible for pushing scheduled jobs
-- to the queue at the expected time.

scheduler :: Job s t => Hworker s t -> IO ()
scheduler hw =
  forever $ do
    now <- getCurrentTime
    execScheduler hw now
    threadDelay 500000


-- | Execute a single check of the scheduler queue. The next job available at
-- the given time is pushed to the job queue and returned by this function.
-- Like `execWorker` this can also be useful for testing.

execScheduler ::
  Job s t => Hworker s t -> UTCTime -> IO (Maybe (ByteString, UTCTime))
execScheduler hw now =
  runRedis (hworkerConnection hw) $
    R.eval
      "local job = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'limit', 0, 1)[1]\n\
      \if job ~= nil then\n\
      \  redis.call('lpush', KEYS[2], tostring(job))\n\
      \  redis.call('zrem', KEYS[1], tostring(job))\n\
      \  local cron = cjson.decode(job)[1]['s']\n\
      \  if cron ~= cjson.null then\n\
      \    redis.call('hset', KEYS[3], tostring(cron), ARGV[1])\n\
      \    return tostring(cron)\n\
      \  end\n\
      \  return job\n\
      \end\n\
      \return nil"
      [ scheduleQueue hw
      , priorityQueue hw
      , cronProcessing hw
      ]
      [ B8.pack $ show (utcToDouble now) ]
    >>=
      \case
        Left err       -> liftIO (hwlog hw err) >> return Nothing
        Right Nothing  -> return Nothing
        Right (Just j) -> return $ Just (j, now)


-- | Start a monitor. Like 'worker', this is blocking, so should be
-- started in a thread. This is responsible for retrying jobs that
-- time out (which can happen if the processing thread is killed, for
-- example). You need to have at least one of these running to have
-- the retry happen, but it is safe to have any number running.

monitor :: Job s t => Hworker s t -> IO ()
monitor hw =
  forever $ do
    execMonitor hw
    -- NOTE(dbp 2015-07-25): We check every 1/10th of timeout.
    threadDelay (floor $ 100000 * hworkerJobTimeout hw)


-- | The status of a job found in the progress queue.

data JobProgress
  = Running NominalDiffTime
  | Requeued
  deriving (Eq, Show)


-- | Checks how long each job in the progress queue has been running
-- and returns their progress. Any job that that has timed out is added back to
-- the progress queue to be retried. Like `execWorker` and `execScheduler`,
-- this can also be useful for testing.

execMonitor :: Job s t => Hworker s t -> IO [(ByteString, JobProgress)]
execMonitor hw = do
  now <- getCurrentTime

  runRedis (hworkerConnection hw) (R.hkeys (progressQueue hw)) >>=
    \case
      Left err ->
        hwlog hw err >> return []

      Right [] ->
        return []

      Right js -> do
        progress <-
          traverse
            ( \j ->
                runRedis (hworkerConnection hw) (R.hget (progressQueue hw) j) >>=
                  \case
                    Left err ->
                      hwlog hw err >> return []

                    Right Nothing ->
                      return []

                    Right (Just start) ->
                      let
                        duration =
                          diffUTCTime now (parseTime start)

                      in
                      if (duration > hworkerJobTimeout hw) then do
                        n <-
                          runWithInt hw $
                            R.eval
                              "local del = redis.call('hdel', KEYS[2], ARGV[1])\n\
                              \if del == 1 then\
                              \  redis.call('rpush', KEYS[1], ARGV[1])\n\
                              \end\n\
                              \return del"
                              [jobQueue hw, progressQueue hw]
                              [j]
                        when (hworkerDebug hw)
                          $ hwlog hw ("MONITOR RV" :: Text, n)
                        when (hworkerDebug hw && n == 1)
                          $ hwlog hw ("MONITOR REQUEUED" :: Text, j)
                        return [(j, Requeued)]
                      else
                        return [(j, Running duration)]
            ) js

        return $ mconcat progress


-- | Returns the jobs that could not be deserialized, most likely
-- because you changed the 'ToJSON'/'FromJSON' instances for you job
-- in a way that resulted in old jobs not being able to be converted
-- back from json. Another reason for jobs to end up here (and much
-- worse) is if you point two instances of 'Hworker', with different
-- job types, at the same queue (i.e., you re-use the name). Then
-- anytime a worker from one queue gets a job from the other it would
-- think it is broken.

broken :: Hworker s t -> IO [(ByteString, UTCTime)]
broken hw =
  runRedis (hworkerConnection hw) (R.hgetall (brokenQueue hw)) >>=
    \case
      Left err -> hwlog hw err >> return []
      Right xs -> return (map (second parseTime) xs)


-- | Returns pending jobs.

listJobs :: Job s t => Hworker s t -> Integer -> Integer -> IO [t]
listJobs hw offset limit =
  (<>)
    <$> listJobsFromQueue hw (priorityQueue hw) offset limit
    <*> listJobsFromQueue hw (jobQueue hw) offset limit


listJobsFromQueue ::
  Job s t => Hworker s t -> ByteString -> Integer -> Integer -> IO [t]
listJobsFromQueue hw q offset limit =
  let
    a = offset * limit
    b = (offset + 1) * limit - 1
  in
  runRedis (hworkerConnection hw) (R.lrange q a b) >>=
    \case
      Left err ->
        hwlog hw err >> return []

      Right [] ->
        return []

      Right xs ->
        return $ mapMaybe (fmap (\(JobRef _ _ _, x) -> x) . A.decodeStrict) xs


-- | Returns all scheduled jobs

listScheduled ::
  Job s t => Hworker s t -> Integer -> Integer -> IO [(t, UTCTime)]
listScheduled hw offset limit =
  let
    a = offset * limit
    b = (offset + 1) * limit - 1
  in
  runRedis (hworkerConnection hw) (R.zrangeWithscores (scheduleQueue hw) a b) >>=
    \case
      Left err ->
        hwlog hw err >> return []

      Right [] ->
        return []

      Right xs ->
        return $
          mapMaybe
            ( \(bytes, s) ->
                case A.decodeStrict bytes of
                  Just (JobRef _ _ _, j) -> Just (j, doubleToUtc s)
                  Nothing                -> Nothing
            )
            xs


countPaused :: Hworker s t -> BatchId -> IO Integer
countPaused hw batch =
  runRedis (hworkerConnection hw) (R.llen (batchPaused hw batch)) >>=
    \case
      Left  _ -> return 0
      Right n -> return n


-- | Returns timestamp of active cron job.

getCronProcessing :: Job s t => Hworker s t -> Text -> IO (Maybe Double)
getCronProcessing hw cron =
  runRedis (hworkerConnection hw) (R.hget (cronProcessing hw) (T.encodeUtf8 cron)) >>=
    \case
      Right mbytes -> return $ mbytes >>= A.decodeStrict
      Left _       -> return Nothing


-- | Returns failed jobs. This is capped at the most recent
-- 'hworkerconfigFailedQueueSize' jobs that returned 'Failure' (or
-- threw an exception when 'hworkerconfigExceptionBehavior' is
-- 'FailOnException').

listFailed :: Job s t => Hworker s t -> Integer -> Integer -> IO [t]
listFailed hw offset limit =
  listJobsFromQueue hw (failedQueue hw) offset limit


-- | Logs the contents of the jobqueue and the inprogress queue at
-- `microseconds` intervals.

debugger :: Job s t => Int -> Hworker s t -> IO ()
debugger microseconds hw =
  forever $ do
    runWithList hw (R.hkeys (progressQueue hw)) $
      \running ->
        runWithList hw (R.lrange (priorityQueue hw) 0 (-1)) $
          \priority ->
            runWithList hw (R.lrange (jobQueue hw) 0 (-1)) $
              \queued -> hwlog hw ("DEBUG" :: Text, priority, queued, running)

    threadDelay microseconds


-- | Initializes a batch of jobs. By default the information for tracking a
-- batch of jobs, created by this function, will expires a week from
-- its creation. The optional `seconds` argument can be used to override this.

initBatch :: Hworker s t -> Maybe Integer -> IO (Maybe BatchId)
initBatch hw mseconds = do
  batch <- BatchId <$> UUID.nextRandom
  runRedis (hworkerConnection hw) $ do
    r <-
      R.hmset (batchCounter hw batch)
        [ ("queued", "0")
        , ("completed", "0")
        , ("successes", "0")
        , ("failures", "0")
        , ("retries", "0")
        , ("canceled", "0")
        , ("status", "queueing")
        ]
    case r of
      Left err ->
        liftIO (hwlog hw err) >> return Nothing

      Right _ -> do
        case mseconds of
          Nothing -> return ()
          Just s  -> void $ R.expire (batchCounter hw batch) s

        return (Just batch)


-- | Cancels a batch of jobs. All jobs for the batch that are still in the
-- worker queue are not run once they're taken off of the queue.

cancelBatch :: Hworker s t -> BatchId -> IO Bool
cancelBatch hw batch =
  runRedis (hworkerConnection hw) $
    R.hget (batchCounter hw batch) "status" >>=
      \case
        Left err -> do
          when (hworkerDebug hw) $ liftIO $ hwlog hw err
          return False

        Right Nothing ->
          return False

        Right (Just status) ->
          if status == "processing" || status == "queueing" || status == "paused"
            then do
              void $ R.hset (batchCounter hw batch) "status" "canceled"
              when (status == "paused") $ void $ R.del [batchPaused hw batch]
              return True
            else
              return False

-- | Pauses a batch of jobs. All jobs for the batch that are still in the
-- worker queue are moved to a separate list once they're take of the the queue
-- and are place back into the job queue once the batch is resumed.

pauseBatch :: Hworker s t -> BatchId -> IO Bool
pauseBatch hw batch =
  runRedis (hworkerConnection hw) $
    R.hget (batchCounter hw batch) "status" >>=
      \case
        Left err -> do
          when (hworkerDebug hw) $ liftIO $ hwlog hw err
          return False

        Right Nothing ->
          return False

        Right (Just status) ->
          if status == "processing" || status == "queueing" then do
            void $ R.persist (batchCounter hw batch)
            void $ R.hset (batchCounter hw batch) "status" "paused"
            return True
          else
            return False


-- | Resumes a batch of jobs.

resumeBatch :: Hworker s t -> BatchId -> IO Bool
resumeBatch hw batch =
  runRedis (hworkerConnection hw) $
    batchSummary' hw batch >>=
      \case
        Nothing ->
          return False

        Just summary | batchSummaryStatus summary == BatchPaused -> do
          withNil hw $
            R.eval
              "redis.call('hset', KEYS[1], 'status', 'processing')\n\
              \local s = KEYS[2]\n\
              \local d = KEYS[3]\n\
              \local l = redis.call('llen', s)\n\
              \local i = tonumber(l)\n\
              \\n\
              \while i > 0 do\n\
              \  local v = redis.call('rpoplpush', s, d)\n\
              \  i = i - 1\n\
              \end\n\
              \return nil"
              [ batchCounter hw batch
              , batchPaused hw batch
              , jobQueue hw
              ]
              []

          return True

        Just _ ->
          return False


-- | Return a summary of the batch.

batchSummary :: Hworker s t -> BatchId -> IO (Maybe BatchSummary)
batchSummary hw batch =
  runRedis (hworkerConnection hw) (batchSummary' hw batch)


batchSummary' :: Hworker s t -> BatchId -> Redis (Maybe BatchSummary)
batchSummary' hw batch = do
  R.hgetall (batchCounter hw batch) >>=
    \case
      Left err -> liftIO (hwlog hw err) >> return Nothing
      Right hm -> return $ decodeBatchSummary batch hm


-- Redis helpers follow

runWithList ::
  Show a => Hworker s t -> Redis (Either a [b]) -> ([b] -> IO ()) -> IO ()
runWithList hw a f =
  runRedis (hworkerConnection hw) a >>=
    \case
      Left err -> hwlog hw err
      Right [] -> return ()
      Right xs -> f xs


runWithMaybe ::
  Show a => Hworker s t -> Redis (Either a (Maybe b)) -> (b -> IO ()) -> IO ()
runWithMaybe hw a f = do
  runRedis (hworkerConnection hw) a >>=
    \case
      Left err       -> hwlog hw err
      Right Nothing  -> return ()
      Right (Just v) -> f v


runWithNil :: Show a => Hworker s t -> Redis (Either a (Maybe ByteString)) -> IO ()
runWithNil hw a =
  runRedis (hworkerConnection hw) $ withNil hw a


withNil ::
  Show a => Hworker s t -> Redis (Either a (Maybe ByteString)) -> Redis ()
withNil hw a =
  a >>=
    \case
      Left err -> liftIO (hwlog hw err)
      Right _  -> return ()


runWithInt :: Hworker s t -> Redis (Either R.Reply Integer) -> IO Integer
runWithInt hw a =
  runRedis (hworkerConnection hw) $ withInt hw a


withInt :: Hworker s t -> Redis (Either R.Reply Integer) -> Redis Integer
withInt hw a =
  a >>=
    \case
      Left err -> liftIO (hwlog hw err) >> return (-1)
      Right n  -> return n


-- Parsing Helpers

encodeBatchStatus :: BatchStatus -> ByteString
encodeBatchStatus =
  \case
    BatchQueueing   -> "queueing"
    BatchFailed     -> "failed"
    BatchProcessing -> "processing"
    BatchFinished   -> "finished"
    BatchCanceled   -> "canceled"
    BatchPaused     -> "paused"


decodeBatchStatus :: ByteString -> Maybe BatchStatus
decodeBatchStatus =
  \case
    "queueing"   -> Just BatchQueueing
    "failed"     -> Just BatchFailed
    "processing" -> Just BatchProcessing
    "finished"   -> Just BatchFinished
    "canceled"   -> Just BatchCanceled
    "paused"     -> Just BatchPaused
    _            -> Nothing


decodeBatchSummary :: BatchId -> [(ByteString, ByteString)] -> Maybe BatchSummary
decodeBatchSummary batch hm =
  BatchSummary batch
    <$> (lookup "queued" hm >>= readMaybe)
    <*> (lookup "completed" hm >>= readMaybe)
    <*> (lookup "successes" hm >>= readMaybe)
    <*> (lookup "failures" hm >>= readMaybe)
    <*> (lookup "retries" hm >>= readMaybe)
    <*> (pure $ fromMaybe 0 $ lookup "canceled" hm >>= readMaybe)
    <*> (lookup "status" hm >>= decodeBatchStatus)


parseTime :: ByteString -> UTCTime
parseTime t =
  case A.decodeStrict t of
    Nothing   -> error ("FAILED TO PARSE TIMESTAMP: " <> B8.unpack t)
    Just time -> time


readMaybe :: Read a => ByteString -> Maybe a
readMaybe =
  fmap fst . listToMaybe . reads . B8.unpack


failBatchSummary :: Hworker s t -> BatchId -> Redis Int
failBatchSummary hw batch = do
  void
    $ R.hset (batchCounter hw batch) "status"
    $ encodeBatchStatus BatchFailed

  batchSummary' hw batch >>=
    \case
      Just summary -> return $ batchSummaryQueued summary
      _            -> return 0


utcToDouble :: UTCTime -> Double
utcToDouble = realToFrac . Posix.utcTimeToPOSIXSeconds


doubleToUtc :: Double -> UTCTime
doubleToUtc = Posix.posixSecondsToUTCTime . realToFrac
