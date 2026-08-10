# GraphQL API Documentation

---

## Table of Contents

- [Root Types](#root-types)
- [Object Types](#object-types)
- [Input Types](#input-types)
- [Enums](#enums)
- [Interfaces](#interfaces)
- [Unions](#unions)
- [Directives](#directives)


## Root Types

### Query

Copyright (c) 2022-2023 Altair Engineering Inc.
All Rights Reserved.
Copyright notice does not imply publication.
Contains trade secret, proprietary, and confidential Information.

#### Fields

**reservation**(resvId: String!): `ResvPayload!`
  -  get reservation information of given reservation id
  - **Arguments:**
    - `resvId`: `String!` -  id of reservation

**reservations**(filter: ResvsFilter, count: Int, from: Cursor, orderBy: ResvOrderBy): `ResvConnection!`
  - **Arguments:**
    - `filter`: `ResvsFilter` -  reservations filters
    - `count`: `Int` - max count to return in response
If -ve then return previous <count> reservations from given <from> cursor (aka backward pagination)
If +ve then return next <count> reservations from given <from> cursor (if any) (aka forward pagination) (default: `1000`)
    - `from`: `Cursor` -  return reservations after/before this cursor
    - `orderBy`: `ResvOrderBy` -  order by key and direction  (default: `RESV_NO_ORDERBY`)

**job**(jobId: String!): `JobPayload!`
  -  get job information of given job id
  - **Arguments:**
    - `jobId`: `String!` -  id of job

**jobs**(filter: JobsFilter, count: Int, from: Cursor, orderBy: JobsOrderBy): `JobConnection!`
  -  get jobs information by given job filters
  - **Arguments:**
    - `filter`: `JobsFilter` -  job filters
    - `count`: `Int` - max count to return in response
If -ve then return previous <count> jobs from given <from> cursor (aka backward pagination)
If +ve then return next <count> jobs from given <from> cursor (if any) (aka forward pagination) (default: `1000`)
    - `from`: `Cursor` -  return jobs after/before this cursor
    - `orderBy`: `JobsOrderBy` -  order by key and direction  (default: `J_NO_ORDERBY`)

**jobDiag**(jobId: String!): `String!`
  -  get job's diagnostic information
  - **Arguments:**
    - `jobId`: `String!` -  id of job

**machine**(name: String!): `MachinePayload!`
  -  get machine information of given machine name
  - **Arguments:**
    - `name`: `String!` -  name of machine

**machines**(filter: MachinesFilter, count: Int, from: Cursor, orderBy: MachinesOrderBy): `MachineConnection!`
  -  get machines information by given machine filters
  - **Arguments:**
    - `filter`: `MachinesFilter` -  machine filters
    - `count`: `Int` - max count to return in response
If -ve then return previous <count> machines from given <from> cursor (aka backward pagination)
If +ve then return next <count> machines from given <from> cursor (if any) (aka forward pagination) (default: `1000`)
    - `from`: `Cursor` -  return machines after/before this cursor
    - `orderBy`: `MachinesOrderBy` -  order by key and direction  (default: `M_NO_ORDERBY`)

**queue**(name: String!): `QueuePayload!`
  -  get queue information of given queue name
  - **Arguments:**
    - `name`: `String!` -  name of queue

**queues**(filter: QueuesFilter, count: Int, from: Cursor, orderBy: QueuesOrderBy): `QueueConnection!`
  -  get queues information by given queue filters
  - **Arguments:**
    - `filter`: `QueuesFilter` -  queue filters
    - `count`: `Int` - max count to return in response
If -ve then return previous <count> queues from given <from> cursor (aka backward pagination)
If +ve then return next <count> queues from given <from> cursor (if any) (aka forward pagination) (default: `1000`)
    - `from`: `Cursor` -  return queues after/before this cursor
    - `orderBy`: `QueuesOrderBy` -  order by key and direction  (default: `Q_NO_ORDERBY`)

**resource**(name: String!): `ResourcePayload!`
  -  get resource information of given resource name
  - **Arguments:**
    - `name`: `String!` -  name of resource

**resources**(filter: ResourcesFilter, count: Int, from: Cursor): `ResourceConnection!`
  -  get all resources information
  - **Arguments:**
    - `filter`: `ResourcesFilter` -  resource filters
    - `count`: `Int` - max count to return in response
If -ve then return previous <count> resources from given <from> cursor (aka backward pagination)
If +ve then return next <count> resources from given <from> cursor (if any) (aka forward pagination) (default: `1000`)
    - `from`: `Cursor` -  return resources after/before this cursor

**parallelEnv**(name: String!): `ParallelEnvPayload!`
  -  get parallel evironment information of given parallel evironment name
  - **Arguments:**
    - `name`: `String!` -  name of parallel evironment

**parallelEnvs**(filter: ParallelEnvsFilter, count: Int, from: Cursor): `ParallelEnvConnection!`
  -  get all parallel evironment information
  - **Arguments:**
    - `filter`: `ParallelEnvsFilter` -  parallel envs filters
    - `count`: `Int` - max count to return in response
If -ve then return previous <count> parallel envs from given <from> cursor (aka backward pagination)
If +ve then return next <count> parallel envs from given <from> cursor (if any) (aka forward pagination) (default: `1000`)
    - `from`: `Cursor` -  return parallel envs after/before this cursor

### Mutation

#### Fields

**createJob**(input: JobInput!): `JobPayload!`
  -  create new job
  - **Arguments:**
    - `input`: `JobInput!`

**orderJob**(input: OrderJobInput!): `[Unknown!]!`
  -  change order of the job
  - **Arguments:**
    - `input`: `OrderJobInput!`

**deleteJob**(jobId: String!, input: DeleteJobInput!): `JobPayload!`
  -  delete job by given id
  - **Arguments:**
    - `jobId`: `String!`
    - `input`: `DeleteJobInput!`

**updateJob**(jobId: String!, input: JobInput!): `JobPayload!`
  -  update job by given id
  - **Arguments:**
    - `jobId`: `String!`
    - `input`: `JobInput!`

**controlJob**(jobId: String!, input: ControlJobInput!): `JobPayload!`
  -  apply control action to job by given id
  - **Arguments:**
    - `jobId`: `String!`
    - `input`: `ControlJobInput!`

**createReservation**(input: ResvInput!): `ResvPayload!`
  -  create new reservation
  - **Arguments:**
    - `input`: `ResvInput!`

**deleteReservation**(resvId: String!): `ResvPayload!`
  -  delete a reservation
  - **Arguments:**
    - `resvId`: `String!`

**updateReservation**(resvId: String!, input: ResvInput!): `ResvPayload!`
  -  update a reservation by given id
  - **Arguments:**
    - `resvId`: `String!`
    - `input`: `ResvInput!`

**deleteJobs**(input: DeleteJobsInput!): `[Unknown!]!`
  -  delete jobs by given filters
  - **Arguments:**
    - `input`: `DeleteJobsInput!`

**updateJobs**(input: UpdateJobsInput!): `[Unknown!]!`
  -  update jobs by given filters
  - **Arguments:**
    - `input`: `UpdateJobsInput!`

**controlJobs**(input: ControlJobsInput!): `[Unknown!]!`
  -  apply control action on jobs by given filters
  - **Arguments:**
    - `input`: `ControlJobsInput!`

**deleteReservations**(input: DeleteResvsInput!): `[Unknown!]!`
  -  delete reservations
  - **Arguments:**
    - `input`: `DeleteResvsInput!`

**updateReservations**(input: UpdateResvsInput!): `[Unknown!]!`
  -  update reservations
  - **Arguments:**
    - `input`: `UpdateResvsInput!`


## Object Types

### AccessList

 Generic access list for users, groups and hosts

#### Fields

**allowed**: `[String!]`
  -  list of allowed users/hosts/groups

**denied**: `[String!]`
  -  list of denied user names


### Job

#### Fields

**metadata**: `[StrNameValue!]`
  -  the list of key-value pair of metadata for job

**jobId**: `String`
  -  the job's identifier

**status**: `JobStatus`
  -  status info of the job

**owner**: `String`
  -  owner of the job

**submitHost**: `String`
  -  submission host of the job

**remoteCommand**: `String`
  -  the command to be executed

**commandArgs**: `[String!]`
  -  the list of arguments to remoteCommand

**shellPath**: `String`
  -  path to shell that runs command/script

**rerunnable**: `Boolean`
  -  indicates whether job is re-runnable or not

**workDir**: `String`
  -  path of the directory in which the job will run

**category**: `String`
  -  the job category to be used

**interactivePort**: `UInt`
  -  the port number to connect by execution for interactive job

**email**: `[String!]`
  -  the list of emails to send notifications about job status

**noEmail**: `Boolean`
  -  indicates no notification for job

**emailOnStarted**: `Boolean`
  -  indicates to get a notification when the job starts

**emailOnTerminated**: `Boolean`
  -  indicates to get a notification when the job terminated/ends

**emailOnAborted**: `Boolean`
  -  indicates to get a notification when the job is aborted

**resourcesRequested**: `JobResources`
  -  resources requested for the job

**resourcesUsed**: `JobResources`
  -  resources used by the job

**allocatedMachines**: `[Machine!]`
  -  list of machines information on which the job is running

**env**: `[StrNameValue!]`
  -  the list of environment variables set for the job

**queue**: `Queue`
  -  queue in which job belongs

**comment**: `String`
  -  descriptive string on why job is in current state

**priority**: `Int`
  -  priority of the job

**earliestStartTime**: `EpochTime`
  -  datetime in microseconds epoch when the job may be eligible to be run

**eligibleTime**: `EpochTime`
  -  datetime in microseconds epoch is the job is waiting to run

**startTime**: `EpochTime`
  -  datetime in microseconds epoch when job was started

**endTime**: `EpochTime`
  -  datetime in microseconds epoch when job was ended

**submitTime**: `EpochTime`
  -  datetime in microseconds epoch when job was submitted

**modifiedTime**: `EpochTime`
  -  datetime in microseconds epoch when job was modified

**name**: `String`
  -  name of the job

**stageIn**: `[JobStage!]`
  -  the list of files to be staged-in when the job runs

**stageOut**: `[JobStage!]`
  -  the list of files to be staged-out when the job finishes

**errorPath**: `String`
  -  path of stderr of the job

**outputPath**: `String`
  -  path of stdout of the job

**inputPath**: `String`
  -  path of stdin of the job

**joinFiles**: `Boolean`
  -  indicates join error and output into outputPath of the job

**keepFiles**: `Boolean`
  -  indicates to keep files of the job on machine where job ran

**privateJobDir**: `Boolean`
  -  indicates to use given workDir as private directory to the job

**array**: `JobArrayInfo`
  -  information about job array indices if current job is job array

**holdType**: `String`
  -  type of hold placed on the job

**dependOnJobs**: `[JobDependInfo!]`
  -  list of the information about jobs on which this job is dependent

**accountingId**: `String`
  -  specifies the name of the account used for job, for accounting purposes

**group**: `String`
  -  specifies the primary group of the job owner

**resvId**: `String`
  -  reservation Id for job, for reservation purposes

**sessionId**: `UInt`
  -  session id of the job

**extension**: `JSON`
  -  extension in json format


### JobArrayHeldIndices

#### Fields

**userHeldIndices**: `String`
  -  number of subjobs in user held status in a job array in format START[-END[:STEP]][,...]

**privilegedHeldIndices**: `String`
  -  number of subjobs in privileged held status in a job array in format START[-END[:STEP]][,...]


### JobArrayInfo

#### Fields

**beginIndex**: `UInt`
  -  start index of job array

**endIndex**: `UInt`
  -  last index of job array

**step**: `UInt`
  -  step size of job array

**maxParallel**: `UInt`
  -  maximum number of subjobs that can run parallely in job array

**remainingIndices**: `String`
  -  range of subjobs remaining in a job array in format START[-END[:STEP]][,...]

**heldIndices**: `JobArrayHeldIndices`
  -  range of subjobs in user and privileged held status in a job array

**suspendedIndices**: `String`
  -  range of subjobs suspended in a job array in format START[-END[:STEP]][,...]

**runningIndices**: `String`
  -  range of subjobs running in a job array in format START[-END[:STEP]][,...]

**deletedIndices**: `String`
  -  range of subjobs deleted in a job array in format START[-END[:STEP]][,...]

**failedIndices**: `String`
  -  range of subjobs failed in a job array in format START[-END[:STEP]][,...]

**doneIndices**: `String`
  -  range of subjobs done in a job array in format START[-END[:STEP]][,...]

**exitingIndices**: `String`
  -  range of subjobs exiting in a job array in format START[-END[:STEP]][,...]

**arrayIndex**: `UInt`
  -  job array index of current subjob


### JobConnection

 a connection to list of Job values

#### Fields

**edges**: `[Unknown!]!`
  -  a list of edges which contains the Job and cursor to aid in connection

**pageInfo**: `PageInfo!`
  -  information to aid in pagination

**totalCount**: `UInt!`
  -  the count of all Jobs you could get from this connection


### JobDependInfo

 information about job dependency

#### Fields

**dependType**: `JobDependencyType!`
  -  dependency type

**jobs**: `[Unknown!]!`
  -  jobs on which the current job depends on


### JobEdge

 information about job edges in connection

**Implements:** JobInfo

#### Fields

**node**: `Job`
  -  information about job

**error**: `JobError`
  -  information about error occured during some job operation

**cursor**: `Cursor`
  -  cursor value for this edge


### JobError

 information about error occured during some job operation

**Implements:** Error

#### Fields

**jobId**: `String!`
  -  job id on which this error occurs

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### JobPayload

 information about job payload

**Implements:** JobInfo

#### Fields

**node**: `Job`
  -  information about job

**error**: `JobError`
  -  information about error occured during some job operation


### JobResources

#### Fields

**peName**: `String`
  -  name of parallel environment (if any)

**taskCount**: `JobTaskCount`
  -  total number of tasks

**jobPlacement**: `Int`
  - job placement policy

Possible values:

0 = None, let WLM decide which placement policy to be used for job

1 = Free, place job on any available machine(s)

2 = Pack, try to place job on one machine

3 = RoundRobin, tries to place each task on single machine, If more tasks are requested than the available machines, then the placement begins from the first host

4 = Scatter, Only one job task with any parallel processes is placed on a machine A job task with no parallel processes may be placed on the same machine as another task

5 = VScatter, Only one job task is placed on a machine. Each task must fit on a machine

6 = NProc, NProc specifies the maximum number of tasks to be launched on individual machines. It needs JobPlacementNProcCount

**jobPlacementSharing**: `Int`
  - job placement sharing policy

Possible values:

0 = None, let WLM decide which placement sharing policy to be used job

1 = Excl, Only this job uses the vnodes chosen

2 = ExclHost, The entire host is allocated to this job

3 = Shared, This job can share the vnodes chosen

**jobPlacementNProcCount**: `UInt`
  -  number of proc count on indiviual host if placement policy = NProc

**jobPlacementRescGroupName**: `String`
  -  name of resource group for placement policy

**tasksResources**: `[JobTasksResources!]`
  -  resources requested for each task

**jobResources**: `JobTasksResources`
  -  job-wide resources requested


### JobStage

 job stage in/our file info

#### Fields

**execPath**: `String`
  -  path of file on execution host

**hostname**: `String`
  -  hostname/ip addr of storage host

**storagePath**: `String`
  -  path of file on storage host


### JobStatus

#### Fields

**state**: `Int`
  - current state of job

Possible values:

0 = Queued, Job is queued and ready for selection

1 = Waiting, Job is waiting until user-specified execution time

2 = DependHeld, Job is waiting for its dependency to finish

3 = Held, User/admin put job on held

4 = StagingFail, Job has failed to stage in its files before running

5 = StagingIn, Job is staging in files before running

6 = StagingOut, Job is staging out it's output files

7 = Running, Job is running, atleast job's (or one of subjob's) one process is started

8 = Suspended, Job is suspended either by user or scheduler in case of preemption

9 = Exiting, All job processes is finished and now job is exiting

10 = Done, Job (or subjob of an array job) is successfully finished

11 = Failed, Job (or subjob of an array job) is finished with failure

12 = Deleted, Job (or subjob of an array job) is deleted by user/admin

13 = Moved, Job is moved to another cluster

14 = Unlicensed, Job is waiting for licenses

**exitStatus**: `Int`
  -  exit status of finished job


### JobTaskCount

 min/max count of job tasks

#### Fields

**min**: `UInt`
  -  minimum tasks count

**max**: `UInt`
  -  maximum tasks count


### JobTasksResources

 job task level resources

#### Fields

**index**: `String`
  - index of tasks requesting this resources
Format: [""]|[startIndex[-endIndex[:step]][,<specificIndex>]]...
Empty string value indicates that this is job-wide resources requested/used

**wallClockTime**: `UInt`
  -  value of wallclock in seconds

**cpuTime**: `UInt`
  -  value of cputime in seconds

**cpuPercentage**: `Float`
  -  value of cpu usage in percentage

**architecture**: `String`
  -  value of architecture

**candidateMachineName**: `String`
  -  requested machine name of this task

**slots**: `UInt`
  -  value of slots

**physicalMemory**: `ULL`
  -  value of physical memory in KB

**virtualMemory**: `ULL`
  -  value of virtual memory in KB

**gpus**: `UInt`
  -  number of gpus used by the job

**customResources**: `[StrNameValue!]`
  -  list of custom resources of task


### Machine

 information about machine

#### Fields

**metadata**: `[StrNameValue!]`
  -  the list of key-value pair of metadata for machine

**name**: `String`
  -  name of machine

**hostname**: `String`
  -  hostname of machine

**port**: `UInt`
  -  port on which machine is listening

**state**: `Int`
  - state of machine

Possible values:

0 = Idle, no jobs running and ready to accept jobs

1 = Busy, jobs running, can accept more jobs

2 = Full, jobs running and all resources assigned, can not accept any jobs

3 = Maintenance, connected but marked as under maintenance by admin, can not accept any jobs

4 = Unlicensed, machine is unlicensed and can not accept any jobs

5 = Down, disconnected

**runningJobs**: `[Job!]`
  -  list of information about running jobs on current machine

**load**: `Float`
  -  1-minute load of machine

**resourcesAvail**: `MachineResources`
  -  information about available resources on machine

**resourcesAssigned**: `MachineResources`
  -  information about assigned resources on machine

**machineOS**: `String`
  -  OS name of machine

**machineOSVersion**: `String`
  -  OS version of machine

**machineArchitecture**: `String`
  -  architecture of machine

**extension**: `JSON`
  -  extension in json format


### MachineConnection

 a connection to list of Machine values

#### Fields

**edges**: `[Unknown!]!`
  -  a list of edges which contains the Machine and cursor to aid in connection

**pageInfo**: `PageInfo!`
  -  information to aid in pagination

**totalCount**: `UInt!`
  -  the count of all Machines you could get from this connection


### MachineEdge

 information about machine edges in connetion

**Implements:** MachineInfo

#### Fields

**node**: `Machine`
  -  information about machine

**error**: `MachineError`
  -  information about error occured during some machine operation

**cursor**: `Cursor`
  -  cursor value for this edge


### MachineError

 information about error occured during some machine operation

**Implements:** Error

#### Fields

**name**: `String!`
  -  name of machine on which this error occurs

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### MachinePayload

 information about machine payload

**Implements:** MachineInfo

#### Fields

**node**: `Machine`
  -  information about machine

**error**: `MachineError`
  -  information about error occured during some machine operation


### MachineResources

 information of machine total/free resources

#### Fields

**slots**: `UInt`
  -  value of slots on machine

**physicalMemory**: `ULL`
  -  value of physical memory in KB on machine

**virtualMemory**: `ULL`
  -  value of virtual memory in KB on machine

**gpus**: `UInt`
  -  number of gpus on available on the machine or assigned to jobs by the machine

**customResources**: `[StrNameValue!]`
  -  list of custom resources of machine


### PageInfo

 information about pagination

#### Fields

**hasPreviousPage**: `Boolean!`
  -  indicates whether there is previous page or not

**hasNextPage**: `Boolean!`
  -  indicates whether there is next page or not

**startCursor**: `Cursor!`
  -  cursor of first item in current page

**endCursor**: `Cursor!`
  -  cursor of last item in current page


### ParallelEnv

 information abour parallel environment

#### Fields

**name**: `String`
  -  name of parallel environment

**extension**: `JSON`
  -  extension in json format


### ParallelEnvConnection

 a connection to list of parallel evironment values

#### Fields

**edges**: `[Unknown!]!`
  -  a list of edges which contains the ParallelEnv and cursor to aid in connection

**pageInfo**: `PageInfo!`
  -  information to aid in pagination

**totalCount**: `UInt!`
  -  the count of all parallel evironment you could get from this connection


### ParallelEnvEdge

 information about parallel evironment edges in connection

**Implements:** ParallelEnvInfo

#### Fields

**node**: `ParallelEnv`
  -  information about parallel evironment

**error**: `ParallelEnvError`
  -  information about error occured during some parallel evironment operation

**cursor**: `Cursor`
  -  cursor value for this edge


### ParallelEnvError

 information about error occured during some parallel evironment operation

**Implements:** Error

#### Fields

**name**: `String!`
  -  name of parallel evironment on which this error occurs

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### ParallelEnvPayload

 information about parallel evironment payload

**Implements:** ParallelEnvInfo

#### Fields

**node**: `ParallelEnv`
  -  information about parallel evironment

**error**: `ParallelEnvError`
  -  information about error occured during some parallel evironment operation


### Queue

 information about queue

#### Fields

**metadata**: `[StrNameValue!]`
  -  the list of key-value pair of metadata for queue

**name**: `String`
  -  name of queue

**type**: `Int`
  - type of queue

Possible values:

0 = Batch, this type of queue can run only batch jobs

1 = Interactive, this type of queue can run only interactive jobs

2 = BatchInteractive, this type of queue can run batch and/or interactive jobs

3 = Route, this type of queue can only route job to another given destination

4 = None, this indicates undefined type of queue and no jobs will be scheduled from this type of queue

**routeDestinations**: `[String!]`
  -  routing destinations if queue type is route

**resourcesAvail**: `QueueResources`
  -  information about available resources on queue

**resourcesAssigned**: `QueueResources`
  -  information about assigned resources on queue

**stateCount**: `QueueStateCount`
  -  counts of jobs in this queue based on job state

**userAccessList**: `UserAccessList`
  -  user access list of queue

**extension**: `JSON`
  -  extension in json format


### QueueConnection

 a connection to list of Queue values

#### Fields

**edges**: `[Unknown!]!`
  -  a list of edges which contains the Queue and cursor to aid in connection

**pageInfo**: `PageInfo!`
  -  information to aid in pagination

**totalCount**: `UInt!`
  -  the count of all Queue you could get from this connection


### QueueEdge

 information about queue edges in connetion

**Implements:** QueueInfo

#### Fields

**node**: `Queue`
  -  information about queue

**error**: `QueueError`
  -  information about error occured during some queue operation

**cursor**: `Cursor`
  -  cursor value for this edge


### QueueError

 information about error occured during some queue operation

**Implements:** Error

#### Fields

**name**: `String!`
  -  name of queue on which this error occurs

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### QueuePayload

 information about queue payload

**Implements:** QueueInfo

#### Fields

**node**: `Queue`
  -  information about queue

**error**: `QueueError`
  -  information about error occured during some queue operation


### QueueResources

 information of queue total/free resources

#### Fields

**wallClockTime**: `UInt`
  -  value of wallclock in seconds

**cpuTime**: `UInt`
  -  value of cputime in seconds

**slots**: `UInt`
  -  value of slots on queue

**physicalMemory**: `ULL`
  -  value of physical memory in KB on queue

**virtualMemory**: `ULL`
  -  value of virtual memory in KB on queue

**customResources**: `[StrNameValue!]`
  -  list of custom resources of queue


### QueueStateCount

 job counts based on their state in queue

#### Fields

**total**: `UInt`
  -  total count of jobs in this queue

**queued**: `UInt`
  -  count of jobs in this queue that are queued

**queuedHeld**: `UInt`
  -  count of jobs in this queue that are in queued and put on held due to various reason

**held**: `UInt`
  -  count of jobs in this queue that are in held by user/admin

**stagingIn**: `UInt`
  -  count of jobs in this queue that are staging in files before running

**stagingOut**: `UInt`
  -  count of jobs in this queue that are staging out it's output files

**running**: `UInt`
  -  count of jobs in this queue that are running

**suspended**: `UInt`
  -  count of jobs in this queue that are suspended by user or scheduler

**exiting**: `UInt`
  -  count of jobs in this queue that are exiting now

**done**: `UInt`
  -  count of jobs in this queue that are successfully finished

**failed**: `UInt`
  -  count of jobs in this queue that are finished with failure

**moved**: `UInt`
  -  count of jobs in this queue that are moved to another cluster


### Reservation

#### Fields

**metadata**: `[StrNameValue!]`
  -  metadata key-value pair to match on reservation

**name**: `String`
  -  name of reservation

**resvId**: `String!`
  -  reservation ID for the operation

**type**: `Int!`
  -  type of reservation (see ResvInput->type for values)

**status**: `Int!`
  -  state of the reservation (see ResvsFilter->state for values)

**owner**: `String`
  -  owner of the reservation

**resources**: `ResvResources!`
  -  resources required for reservation

**startTime**: `EpochTime`
  -  reservation start time

**endTime**: `EpochTime`
  -  reservation end time

**duration**: `UInt`
  -  duration of reservation (in seconds)

**submitTime**: `EpochTime`
  -  datetime in microseconds epoch when reservation was submitted

**modifiedTime**: `EpochTime`
  -  datetime in microseconds epoch when reservation was modified

**allocatedMachines**: `[Machine!]`
  -  list of machines information on which the reservation is running

**authorizedUsers**: `AccessList!`
  -  list of authorized users

**authorizedHosts**: `AccessList`
  -  list of authorized hosts

**authorizedGroups**: `AccessList`
  -  list of authorized groups

**recurrence**: `Rrule`
  -  recurrence rule for standing job reservation


### Resource

 information about resource

#### Fields

**name**: `String`
  -  name of resource

**type**: `Int`
  - type of resource

Possible values:

0 = boolean type resource

1 = int type resource

2 = long type resource

3 = string type resource

4 = size type resource

5 = float type resource

6 = double type resource

7 = string array type resource

8 = time type resource

**flag**: `[Int!]`
  - flag of resource

Possible values:

0 = CanRequest, Can be requested

1 = CaseInsensitiveMatch, case insensitive comparison (Only applied when resource type == string)

2 = CanRequestForTask, Can be requested for task level

3 = AvailOnlyOnFirstMachine, Only available on first machine

4 = AvailOnAllMachine, Available on on all machines

5 = AvailOnServer, Available on server level

6 = AvailOnQueue, Available on queue level

7 = CanSendToMachine, Can be sent to machine when job executes

8 = UserRD, User can read

9 = UserWR, User can write

10 = OnlyAdminWR, Only admin writable

**extension**: `JSON`
  -  extension in json format


### ResourceConnection

 a connection to list of Resource values

#### Fields

**edges**: `[Unknown!]!`
  -  a list of edges which contains the Resource and cursor to aid in connection

**pageInfo**: `PageInfo!`
  -  information to aid in pagination

**totalCount**: `UInt!`
  -  the count of all resources you could get from this connection


### ResourceEdge

 information about resource edges in connection

**Implements:** ResourceInfo

#### Fields

**node**: `Resource`
  -  information about resource

**error**: `ResourceError`
  -  information about error occured during some resource operation

**cursor**: `Cursor`
  -  cursor value for this edge


### ResourceError

 information about error occured during some resource operation

**Implements:** Error

#### Fields

**name**: `String!`
  -  name of resource on which this error occurs

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### ResourcePayload

 information about resource payload

**Implements:** ResourceInfo

#### Fields

**node**: `Resource`
  -  information about resource

**error**: `ResourceError`
  -  information about error occured during some resource operation


### ResvConnection

#### Fields

**edges**: `[Unknown!]!`
  -  a list of edges which contains the reservation and cursor to aid in connection

**pageInfo**: `PageInfo!`
  -  information to aid in pagination

**totalCount**: `UInt!`
  -  the count of all reservations you could get from this connection


### ResvEdge

 information about reservation edges in connection

**Implements:** ResvInfo

#### Fields

**node**: `Reservation`
  -  information about reservation

**error**: `ResvError`
  -  information about error occured during some reservation operation

**cursor**: `Cursor`
  -  cursor value for this edge


### ResvError

 information about error occured during reservation operation

**Implements:** Error

#### Fields

**resvId**: `String!`
  -  Reservation ID on which this error occurs

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### ResvPayload

**Implements:** ResvInfo

#### Fields

**node**: `Reservation`
  -  information about reservation

**error**: `ResvError`
  -  information about error occured during the reservation operation


### ResvResources

#### Fields

**peName**: `String`
  -  name of parallel environment (if any)

**taskCount**: `ResvTaskCount`
  -  total number of tasks

**resvPlacement**: `Int`
  - reservation placement policy

Possible values:

0 = None, let WLM decide which placement policy to be used for reservation

1 = Free, place reservation on any available machine(s)

2 = Pack, try to place reservation on one machine

3 = RoundRobin, tries to place each task on single machine, If more tasks are requested than the available machines, then the placement begins from the first host

4 = Scatter, Only one reservation task with any parallel processes is placed on a machine A reservation task with no parallel processes may be placed on the same machine as another task

5 = VScatter, Only one reservation task is placed on a machine. Each task must fit on a machine

6 = NProc, NProc specifies the maximum number of tasks to be launched on individual machines. It needs resvPlacementNProcCount

**resvPlacementSharing**: `Int`
  - reservation placement sharing policy

Possible values:

0 = None, let WLM decide which placement sharing policy to be used reservation

1 = Excl, Only this reservation uses the vnodes chosen

2 = ExclHost, The entire host is allocated to this reservation

3 = Shared, This reservation can share the vnodes chosen

**resvPlacementNProcCount**: `UInt`
  -  number of proc count on indiviual host if placement policy = NProc

**resvPlacementRescGroupName**: `String`
  -  name of resource group for placement policy

**tasksResources**: `[ResvTasksResources!]`
  -  resources requested for each task

**resvResources**: `ResvTasksResources`
  - Reservation-wide resources or common task resources requested.
If task resources are provided for specific tasks, then only task resources will apply.
Otherwise, this will be used as common task resources for the total task count.


### ResvTaskCount

 min/max count of reservation tasks

#### Fields

**min**: `UInt`
  -  minimum tasks count

**max**: `UInt`
  -  maximum tasks count


### ResvTasksResources

 resv task level resources

#### Fields

**index**: `String`
  - index of tasks requesting this resources
Format: [""]|[startIndex[-endIndex[:step]][,<specificIndex>]]...
Empty string value indicates that this is job-wide resources requested/used

**wallClockTime**: `UInt`
  -  value of wallclock in seconds

**cpuTime**: `UInt`
  -  value of cputime in seconds

**cpuPercentage**: `Float`
  -  value of cpu usage in percentage

**architecture**: `String`
  -  value of architecture

**candidateMachineName**: `String`
  -  requested machine name of this task

**slots**: `UInt`
  -  value of slots

**physicalMemory**: `ULL`
  -  value of physical memory in KB

**virtualMemory**: `ULL`
  -  value of virtual memory in KB

**gpus**: `UInt`
  -  number of gpus used by the reservation

**customResources**: `[StrNameValue!]`
  -  list of custom resources of task


### Rrule

 Recurrence rule for standing reservations

#### Fields

**rule**: `String!`
  - format for recurrence rule: <day-range/day> <hour-range/hour> count=<count>/until=<until>

- day-range/day:
    - Can be a single day (MO, TU, WE, TH, FR, SA, SU)
    - Can be a range of days separated by "-" (e.g., MO-FR for weekdays)
    - Can be both of the above seperated by comma (e.g., MO-WE,FR)
- hour-range/hour:
    - Can be a single hour in military format (0-2359)
    - Can be a range of hours separated by "-" (e.g., 1400-1500)
    - Can be both of the above seperated by comma (e.g., 1400-1500,1800-2000)
- count:
    - Number of occurrences of the reservation (positive integer)
- until:
    - time in microseconds till when the reservation will run

**occurrence**: `Int!`
  -  soonest occurrence

**maxAllocate**: `Int!`
  -  Maximum number of occurrence to be allocated at once


### StrNameValue

 string name-value pair

#### Fields

**name**: `String`
  -  name of name-value pair

**value**: `String`
  -  value of name-value pair


### UserAccessList

 user access list

#### Fields

**allowed**: `[String!]`
  -  list of allowed user names

**denied**: `[String!]`
  -  list of denied user names



## Input Types

### AccessListInput

 Generic access list input for users, groups and hosts

#### Input Fields

- **allowed**: `[String!]` -  list of allowed users/hosts/groups
- **denied**: `[String!]` -  list of denied user names

### ControlJobInput

 parameters for control job

#### Input Fields

- **action**: `JobControlAction!` -  control action to be applied on job
- **data**: `String` -  additional data for the given action
- **extension**: `JSON` -  extension in json format
- **errOnExtension**: `Boolean` -  indicates no error should be generated if WLM doesn't support given extension

### ControlJobsInput

 parameters to filter and control jobs

#### Input Fields

- **filter**: `JobsFilter!` -  information to filter jobs on which control action will be applied
- **info**: `ControlJobInput!` -  parameters for control jobs

### CustomResourceFilter

 custom resource filter with comparator

#### Input Fields

- **name**: `String!` -  name of custom resource
- **value**: `String!` -  value of custom resource
- **comp**: `FilterComp` -  filter comparator  (default: `EQ`)

### DeleteJobInput

 parameters for delete job

#### Input Fields

- **force**: `Boolean` -  indicates to force delete job
- **history**: `Boolean` -  indicates to delete job along with history (if any) also
- **extension**: `JSON` -  extension in json format
- **errOnExtension**: `Boolean` -  indicates no error should be generated if WLM doesn't support given extension

### DeleteJobsInput

 parameters to filter and delete jobs

#### Input Fields

- **filter**: `JobsFilter!` -  information to filter jobs which will be deleted
- **info**: `DeleteJobInput!` -  parameters for delete jobs

### DeleteResvsInput

 parameters to delete reservation

#### Input Fields

- **filter**: `ResvsFilter!` -  information to filter reservations which will be deleted

### EpochTimeFilter

 filter for epochtime with comparator

#### Input Fields

- **datetime**: `EpochTime!` -  datetime in microseconds epoch
- **comp**: `FilterComp` -  filter comparator  (default: `EQ`)

### IntFilter

 filter for Int with comparator

#### Input Fields

- **value**: `Int!` -  Int value
- **comp**: `FilterComp` -  filter comparator  (default: `EQ`)

### JobArrayInput

#### Input Fields

- **beginIndex**: `UInt!` -  start index of job array
- **endIndex**: `UInt!` -  last index of job array
- **step**: `UInt` -  step size of job array
- **maxParallel**: `UInt` -  maximum number of subjobs that can run parallely in job array

### JobDependInput

 information about job dependency during job creation

#### Input Fields

- **dependType**: `JobDependencyType!` -  dependency type
- **jobIds**: `[Unknown!]!` -  job ids (in format [jobid]|[array-jobid]|[array-jobid[startIndex[-endIndex[:step]][,<specificIndex>]]...]...) on which the current job can depend on

### JobInput

#### Input Fields

- **metadata**: `[StrNameValueInput!]` -  the list of key-value pair of metadata for job
- **scriptContent**: `Base64` -  urlsafe base64 encoded job script content
- **remoteCommand**: `String` -  the command to be executed
- **commandArgs**: `[String!]` -  the list of arguments to remoteCommand
- **shellPath**: `String` -  path to shell that runs command/script
- **rerunnable**: `Boolean` -  indicates whether job is re-runnable or not
- **workDir**: `String` -  path of the directory in which the job will run
- **category**: `String` -  the job category to be used
- **interactivePort**: `UInt` -  the port number to connect by execution for interactive job
- **email**: `[String!]` -  the list of emails to send notifications about job status
- **noEmail**: `Boolean` -  indicates no notification for job
- **emailOnStarted**: `Boolean` -  indicates to get a notification when the job starts
- **emailOnTerminated**: `Boolean` -  indicates to get a notification when the job terminated/ends
- **emailOnAborted**: `Boolean` -  indicates to get a notification when the job is aborted
- **resourcesRequested**: `JobResourcesRequestedInput` -  resources requested for the job
- **env**: `[StrNameValueInput!]` -  the list of environment variables set for the job
- **queue**: `JobQueueInput` -  queue information in which job belongs
- **priority**: `Int` -  priority of the job
- **earliestStartTime**: `EpochTime` -  datetime in microseconds epoch when the job may be eligible to be run
- **name**: `String` -  name of the job
- **stageIn**: `[JobStageInput!]` -  the list of files to be staged-in when the job runs
- **stageOut**: `[JobStageInput!]` -  the list of files to be staged-out when the job finishes
- **errorPath**: `String` -  path of stderr of the job
- **outputPath**: `String` -  path of stdout of the job
- **inputPath**: `String` -  path of stdin of the job
- **joinFiles**: `Boolean` -  indicates join error and output into outputPath of the job
- **keepFiles**: `Boolean` -  indicates to keep files of the job on machine where job ran
- **privateJobDir**: `Boolean` -  indicates to use given workDir as private directory to the job
- **array**: `JobArrayInput` -  information about job array indices if current job is job array
- **dependOnJob**: `[JobDependInput!]` -  list of the job ids on which this job is dependent
- **accountingId**: `String` -  specifies the name of the account used for job, for accounting purposes
- **resvId**: `String` -  reservation Id for job, for reservation purposes
- **submitAsHold**: `Boolean` -  indicates to put job in held state after submit
- **holdType**: `String` -  type of hold placed on the job
- **extension**: `JSON` -  extension in json format
- **errOnExtension**: `Boolean` -  indicates no error should be generated if WLM doesn't support given extension

### JobQueueInput

 information about queue during job creation

#### Input Fields

- **name**: `String!` -  name of queue

### JobResourceFilter

 information about job's requested/used resources filter

#### Input Fields

- **slots**: `UIntFilter` -  value of slots with comparator
- **physicalMemory**: `ULLFilter` -  value of physical memory in KB with comparator
- **virtualMemory**: `ULLFilter` -  value of virtual memory in KB with comparator
- **wallClockTime**: `UIntFilter` -  value of wallclock in seconds with comparator
- **cpuTime**: `UIntFilter` -  value of cputime in seconds with comparator
- **customResources**: `[CustomResourceFilter!]` -  list of custom resources filter

### JobResourcesRequestedInput

#### Input Fields

- **peName**: `String` -  name of parallel environment (if any)
- **taskCount**: `JobTaskCountInput` -  total number of tasks
- **jobPlacement**: `Int` - job placement policy (default: 0)

Possible values:

0 = None, let WLM decide which placement policy to be used for job

1 = Free, place job on any available machine(s)

2 = Pack, try to place job on one machine

3 = RoundRobin, tries to place each task on single machine, If more tasks are requested than the available machines, then the placement begins from the first host

4 = Scatter, Only one job task with any parallel processes is placed on a machine A job task with no parallel processes may be placed on the same machine as another task

5 = VScatter, Only one job task is placed on a machine. Each task must fit on a machine

6 = NProc, NProc specifies the maximum number of tasks to be launched on individual machines. It needs JobPlacementNProcCount
- **jobPlacementSharing**: `Int` - job placement sharing policy (default: 0)

Possible values:

0 = None, let WLM decide which placement sharing policy to be used job

1 = Excl, Only this job uses the vnodes chosen

2 = ExclHost, The entire host is allocated to this job

3 = Shared, This job can share the vnodes chosen
- **jobPlacementNProcCount**: `UInt` -  number of proc count on indiviual host if placement policy = NProc
- **jobPlacementRescGroupName**: `String` -  name of resource group for placement policy
- **tasksResources**: `[JobTasksResourcesInput!]` -  resources requested for each task
- **jobResources**: `JobTasksResourcesInput` - job-wide resources or common task resources requested
NOTE: If task resources is given for any particular/range of tasks then
ONLY tasks resources will be applied else this will be used as common task
resources to match total tasks count

### JobStageInput

 job stage in/our file info input

#### Input Fields

- **execPath**: `String!` -  path of file on execution host
- **hostname**: `String` -  hostname/ip addr of storage host
- **storagePath**: `String!` -  path of file on storage host

### JobTaskCountInput

 min/max count of job tasks

#### Input Fields

- **min**: `UInt` -  minimum tasks count (default: 1)
- **max**: `UInt!` -  maximum tasks count

### JobTasksResourcesInput

 job task level resources input

#### Input Fields

- **index**: `String!` - index of tasks requesting this resources
Format: [""]|[startIndex[-endIndex[:step]][,<specificIndex>]]...
Empty string value indicates that this is job-wide resources requested
- **wallClockTime**: `UInt` -  value of wallclock in seconds
- **cpuTime**: `UInt` -  value of cputime in seconds
- **architecture**: `String` -  value of architecture
- **candidateMachineName**: `String` -  requested machine name that this task should run on this machine
- **slots**: `UInt` -  value of slots
- **physicalMemory**: `ULL` -  value of physical memory in KB
- **virtualMemory**: `ULL` -  value of virtual memory in KB
- **gpus**: `UInt` -  number of gpus required for the job
- **customResources**: `[StrNameValueInput!]` -  list of custom resources of task

### JobsFilter

 information to filter jobs

#### Input Fields

- **metadata**: `[StrNameValueInput!]` -  metadata key-value pair to match on job
- **jobIds**: `[String!]` -  list of job ids in format [jobid]|[array-jobid]|[array-jobid[startIndex[-endIndex[:step]][,<specificIndex>]]...]...
- **states**: `[Int!]` -  list of state of the job (see JobStatus->state for values)
- **owner**: `String` -  owner of the job
- **category**: `String` -  category of the job
- **queue**: `String` -  queue in which the job was submitted
- **accountingId**: `String` -  accounting id of the job
- **priority**: `IntFilter` - priority of the job
- **startTime**: `EpochTimeFilter` -  start epochtime of the job
- **modifiedTime**: `EpochTimeFilter` -  last modified epochtime of the job
- **submitTime**: `EpochTimeFilter` -  submission epochtime of the job
- **earliestStartTime**: `EpochTimeFilter` -  earliest start epochtime of the job
- **eligibleTime**: `EpochTimeFilter` -  eligible epochtime of the job
- **endTime**: `EpochTimeFilter` -  end epochtime of the job
- **resourcesRequested**: `JobResourceFilter` -  requested resources by the job
- **resourcesUsed**: `JobResourceFilter` -  used resources by the job
- **withSubJobs**: `Boolean` -  indicates to include non-queued subjobs information along with its parent array job (Default: false)
- **onlyArrayJobs**: `Boolean` -  indicates to include only array jobs information (Default: false)
- **onlyNonArrayJobs**: `Boolean` -  indicates to include only non-array jobs information (Default: false)
- **withHistoryJobs**: `Boolean` -  indicates to include history jobs information also (Default: false)

### MachineResourceFilter

 information about machine total/free resources filter

#### Input Fields

- **slots**: `UIntFilter` -  value of slots on machine with comparator
- **physicalMemory**: `ULLFilter` -  value of physical memory in KB on machine with comparator
- **virtualMemory**: `ULLFilter` -  value of virtual memory in KB on machine with comparator
- **customResources**: `[CustomResourceFilter!]` -  list of custom resources on machine

### MachinesFilter

 information to filter machines

#### Input Fields

- **names**: `[String!]` -  list of name of machines
- **hostnames**: `[String!]` -  list of hostnames of machines
- **states**: `[Int!]` - list of state of machine

Possible values:

0 = Idle, no jobs running and ready to accept jobs

1 = Busy, jobs running, can accept more jobs

2 = Full, jobs running and all resources assigned, can not accept any jobs

3 = Maintenance, connected but marked as under maintenance by admin, can not accept any jobs

4 = Unlicensed, machine is unlicensed and can not accept any jobs

5 = Down, disconnected
- **resourcesAvail**: `MachineResourceFilter` -  available resources of machines
- **resourcesAssigned**: `MachineResourceFilter` -  assigned resources of machines

### OrderJobInput

 parameters to change order of job

#### Input Fields

- **jobId**: `String!` -  first job id
- **otherJobId**: `String!` -  second job id
- **extension**: `JSON` -  extension in json format
- **errOnExtension**: `Boolean` -  indicates no error should be generated if WLM doesn't support given extension

### ParallelEnvsFilter

#### Input Fields

- **names**: `[String!]` -  list of name of parallel envs

### QueuesFilter

#### Input Fields

- **names**: `[String!]` -  list of name of queues

### ResourcesFilter

#### Input Fields

- **names**: `[String!]` -  list of name of resources

### ResvInput

#### Input Fields

- **metadata**: `[StrNameValueInput!]` -  metadata key-value pair to match on reservation
- **name**: `String` -  name of reservation
- **type**: `Int!` - type of reservation

Possible values:

0 = Advance, for advance reservation.

1 = Standing, for standing reservation.
- **resources**: `ResvResourcesRequestedInput` -  resources required for reservation
- **startTime**: `EpochTime` -  reservation start time
- **endTime**: `EpochTime` -  reservation end time
- **duration**: `UInt` -  Duration of reservation (in seconds)
- **authorizedUsers**: `AccessListInput` -  List of authorized users
- **authorizedHosts**: `AccessListInput` -  list of authorized hosts
- **authorizedGroups**: `AccessListInput` -  list of authorized groups
- **recurrence**: `RruleInput` -  recurrence rule for standing job reservation

### ResvResourceFilter

 information about reservation's requested/used resources filter

#### Input Fields

- **slots**: `UIntFilter` -  value of slots with comparator
- **physicalMemory**: `ULLFilter` -  value of physical memory in KB with comparator
- **virtualMemory**: `ULLFilter` -  value of virtual memory in KB with comparator
- **wallClockTime**: `UIntFilter` -  value of wallclock in seconds with comparator
- **cpuTime**: `UIntFilter` -  value of cputime in seconds with comparator
- **customResources**: `[CustomResourceFilter!]` -  list of custom resources filter

### ResvResourcesRequestedInput

#### Input Fields

- **peName**: `String` -  name of parallel environment (if any)
- **taskCount**: `ResvTaskCountInput` -  total number of tasks
- **resvPlacement**: `Int` - reservation placement policy

Possible values:

0 = None, let WLM decide which placement policy to be used for reservation

1 = Free, place reservation on any available machine(s)

2 = Pack, try to place reservation on one machine

3 = RoundRobin, tries to place each task on single machine, If more tasks are requested than the available machines, then the placement begins from the first host

4 = Scatter, Only one reservation task with any parallel processes is placed on a machine A reservation task with no parallel processes may be placed on the same machine as another task

5 = VScatter, Only one reservation task is placed on a machine. Each task must fit on a machine

6 = NProc, NProc specifies the maximum number of tasks to be launched on individual machines. It needs resvPlacementNProcCount
- **resvPlacementSharing**: `Int` - reservation placement sharing policy

Possible values:

0 = None, let WLM decide which placement sharing policy to be used reservation

1 = Excl, Only this reservation uses the vnodes chosen

2 = ExclHost, The entire host is allocated to this reservation

3 = Shared, This reservation can share the vnodes chosen
- **resvPlacementNProcCount**: `UInt` -  Number of proc count on individual hosts if placement policy is NProc.
- **resvPlacementRescGroupName**: `String` -  Name of resource group for placement policy
- **tasksResources**: `[ResvTasksResourcesInput!]` -  Resources requested for each task
- **resvResources**: `ResvTasksResourcesInput` - Reservation-wide resources or common task resources requested.
If task resources are provided for specific tasks, then only task resources will apply.
Otherwise, this will be used as common task resources for the total task count.

### ResvTaskCountInput

 min/max count of reservation tasks

#### Input Fields

- **min**: `UInt` -  minimum tasks count (default: 1)
- **max**: `UInt!` -  maximum tasks count

### ResvTasksResourcesInput

 reservation task level resources input

#### Input Fields

- **index**: `String!` - index of tasks requesting this resources
Format: [""]|[startIndex[-endIndex[:step]][,<specificIndex>]]...
Empty string value indicates that this is reservation-wide resources requested
- **wallClockTime**: `UInt` -  value of wallclock in seconds
- **cpuTime**: `UInt` -  value of cputime in seconds
- **architecture**: `String` -  value of architecture
- **candidateMachineName**: `String` -  requested machine name that this task should run on this machine
- **slots**: `UInt` -  value of slots
- **physicalMemory**: `ULL` -  value of physical memory in KB
- **virtualMemory**: `ULL` -  value of virtual memory in KB
- **gpus**: `UInt` -  number of gpus required for the reservation
- **customResources**: `[StrNameValueInput!]` -  list of custom resources of task

### ResvsFilter

#### Input Fields

- **metadata**: `[StrNameValueInput!]` -  metadata key-value pair to match on reservation
- **resvIds**: `[String!]` -  reservation ID for the operation
- **states**: `[Int!]` - Reservation State

Possible values:

0 = RESV_UNCONFIRMED, Reservation is unconfirmed.

1 = RESV_CONFIRMED, Reservation is confirmed.

2 = RESV_RUNNING, Reservation is currently running.

3 = RESV_FINISHED, Reservation has finished.

4 = RESV_DELETED, Reservation is deleted.

5 = RESV_BEING_DELETED, Reservation is being deleted

6 = RESV_DEGRADED, Vnode(s) allocated to reservation unavailable
- **types**: `[Int!]` - type of reservation

Possible values:

0 = Advance, for advance reservation.

1 = Standing, for standing reservation.
- **owner**: `String` -  owner of the reservation
- **startTime**: `EpochTimeFilter` -  start epochtime of the reservation
- **endTime**: `EpochTimeFilter` -  end epochtime of the reservation
- **modifiedTime**: `EpochTimeFilter` -  reservation modified time
- **duration**: `UIntFilter` -  duration of the reservation (in seconds)
- **resources**: `ResvResourceFilter` -  requested resources by the reservation

### RruleInput

 Recurrence rule for standing reservations

#### Input Fields

- **rule**: `String!` - format for recurrence rule: <day-range/day> <hour-range/hour> count=<count>/until=<until>

- day-range/day:
    - Can be a single day (MO, TU, WE, TH, FR, SA, SU)
    - Can be a range of days separated by "-" (e.g., MO-FR for weekdays)
    - Can be both of the above seperated by comma (e.g., MO-WE,FR)
- hour-range/hour:
    - Can be a single hour in military format (0-2359)
    - Can be a range of hours separated by "-" (e.g., 1400-1500)
    - Can be both of the above seperated by comma (e.g., 1400-1500,1800-2000)
- count:
    - Number of occurrences of the reservation (positive integer)
- until:
    - time in microseconds till when the reservation will run
- **maxAllocate**: `Int` -  Maximum number of occurrence to be allocated at once

### StrNameValueInput

 string name-value pair input

#### Input Fields

- **name**: `String!` -  name of name-value pair
- **value**: `String!` -  value of name-value pair

### UIntFilter

 filter for UInt with comparator

#### Input Fields

- **value**: `UInt!` -  UInt value
- **comp**: `FilterComp` -  filter comparator  (default: `EQ`)

### ULLFilter

 filter for ULL with comparator

#### Input Fields

- **value**: `ULL!` -  ULL value
- **comp**: `FilterComp` -  filter comparator  (default: `EQ`)

### UpdateJobsInput

 parameters to filter and update jobs

#### Input Fields

- **filter**: `JobsFilter!` -  information to filter jobs which will be updated
- **info**: `JobInput!` -  information of job updates to be applied

### UpdateResvsInput

 parameters to update reservations

#### Input Fields

- **filter**: `ResvsFilter!` -  information to filter reservations which will be updated
- **info**: `ResvInput!` -  information of reservations updates to be applied


## Enums

### FilterComp

 filter comparator

#### Values

- **EQ** -  for equal
- **GT** -  for greater than
- **LT** -  for less than
- **GE** -  for greater than or equal
- **LE** -  for less than or equal
- **NE** -  for not equal

### JobControlAction

 job control actions

#### Values

- **Move** -  to move queued job between server or queue
- **Signal** -  to send signal to running job
- **Hold** -  to hold queued or running job
- **Release** -  to release held job
- **Rerun** -  to rerun running job
- **Suspend** -  to suspend running job
- **Resume** -  to resume suspended job

### JobDependencyType

 job dependency type

#### Values

- **JDT_AFTER** -  The dependent job may start only after all dependee jobs have started execution.
- **JDT_AFTEROK** -  The dependent job may start only after all dependee jobs have terminated with no errors
- **JDT_AFTERANY** -  The dependent job may start after all dependee jobs have finished execution, with or without errors
- **JDT_BEFORE** -  The following dependency types are unsupported currently
   The dependent jobs using this dependency type may start once the dependee job has begun execution
- **JDT_BEFOREOK** -  The dependent jobs using this dependency type may start once the dependee job terminates without errors
- **JDT_BEFOREANY** -  The dependent jobs using this dependency type may start once the dependee job terminates execution, with or without errors

### JobsOrderBy

 information about orderBy key and direction for jobs

#### Values

- **J_NO_ORDERBY** -  let WLM decide what to use for order jobs
- **J_JOBID_ASC** -  order jobs by jobid in ascending order
- **J_JOBID_DESC** -  order jobs by jobid in descending order
- **J_STATE_ASC** -  order jobs by job state in ascending order
- **J_STATE_DESC** -  order jobs by job state in descending order
- **J_OWNER_ASC** -  order jobs by job owner in ascending order
- **J_OWNER_DESC** -  order jobs by job owner in descending order
- **J_CATEGORY_ASC** -  order jobs by category of job in ascending order
- **J_CATEGORY_DESC** -  order jobs by category of job in descending order
- **J_QUEUE_ASC** -  order jobs by queue name in which job belongs in ascending order
- **J_QUEUE_DESC** -  order jobs by queue name in which job belongs in descending order
- **J_PRIORITY_ASC** -  order jobs by priority of job in ascending order
- **J_PRIORITY_DESC** -  order jobs by priority of job in descending order
- **J_EARLIEST_START_TIME_ASC** -  order jobs by earliest start time of job in ascending order
- **J_EARLIEST_START_TIME_DESC** -  order jobs by earliest start time of job in descending order
- **J_SUBMIT_TIME_ASC** -  order jobs by submit time of job in ascending order
- **J_SUBMIT_TIME_DESC** -  order jobs by submit time of job in descending order
- **J_START_TIME_ASC** -  order jobs by start time of job in ascending order
- **J_START_TIME_DESC** -  order jobs by start time of job in descending order
- **J_END_TIME_ASC** -  order jobs by end time of job in ascending order
- **J_END_TIME_DESC** -  order jobs by end time of job in descending order
- **J_MODIFIED_TIME_ASC** -  order jobs by modifed time of job in ascending order
- **J_MODIFIED_TIME_DESC** -  order jobs by modified time of job in descending order
- **J_NAME_ASC** -  order jobs by name of job in ascending order
- **J_NAME_DESC** -  order jobs by name of job in descending order
- **J_ACCOUNTING_ID_ASC** -  order jobs by accounting id of job in ascending order
- **J_ACCOUNTING_ID_DESC** -  order jobs by accounting id of job in descending order

### MachinesOrderBy

 information about orderBy key and direction for machines

#### Values

- **M_NO_ORDERBY** -  let WLM decide what to use for order machines
- **M_NAME_ASC** -  order machine by name of machine in ascending order
- **M_NAME_DESC** -  order machine by name of machine in descending order
- **M_HOSTNAME_ASC** -  order machine by hostname of machine in ascending order
- **M_HOSTNAME_DESC** -  order machine by hostname of machine in descending order
- **M_STATE_ASC** -  order machine by state of machine in ascending order
- **M_STATE_DESC** -  order machine by state of machine in descending order
- **M_LOAD_ASC** -  order machine by load of machine in ascending order
- **M_LOAD_DESC** -  order machine by load of machine in descending order
- **M_ARCH_ASC** -  order machine by architecture of machine in ascending order
- **M_ARCH_DESC** -  order machine by architecture of machine in descending order

### QueuesOrderBy

 information about orderBy key and direction for queues

#### Values

- **Q_NO_ORDERBY** -  let WLM decide what to use for order queues
- **Q_NAME_ASC** -  order queue by name of queue in ascending order
- **Q_NAME_DESC** -  order queue by name of queue in descending order

### ResvOrderBy

 information about orderBy key and direction for reservations

#### Values

- **RESV_NO_ORDERBY** -  let WLM decide what to use for order reservations
- **RESV_ID_ASC** -  order reservations by resvId in ascending order
- **RESV_ID_DESC** -  order reservations by resvId in descending order
- **RESV_STATE_ASC** -  order reservations by reservation state in ascending order
- **RESV_STATE_DESC** -  order reservations by reservation state in descending order
- **RESV_OWNER_ASC** -  order reservations by reservation owner in ascending order
- **RESV_OWNER_DESC** -  order reservations by reservation owner in descending order
- **RESV_START_TIME_ASC** -  order reservations by start time of reservation in ascending order
- **RESV_START_TIME_DESC** -  order reservations by start time of reservation in descending order
- **RESV_END_TIME_ASC** -  order reservations by end time of reservation in ascending order
- **RESV_END_TIME_DESC** -  order reservations by end time of reservation in descending order
- **RESV_DURATION_ASC** -  order reservations by duration of reservation in ascending order
- **RESV_DURATION_DESC** -  order reservations by duration of reservation in descending order
- **RESV_NAME_ASC** -  order reservations by name of reservation in ascending order
- **RESV_NAME_DESC** -  order reservations by name of reservation in descending order


## Interfaces

### Error

 information about generic error

#### Fields

**errorCode**: `Int!`
  -  error code

**errorMessage**: `String!`
  -  error message


### JobInfo

 information about job or error

#### Fields

**node**: `Job`
  -  information about job

**error**: `JobError`
  -  information about error occured during some job operation


### MachineInfo

 information about machine or error

#### Fields

**node**: `Machine`
  -  information about machine

**error**: `MachineError`
  -  information about error occured during some machine operation


### ParallelEnvInfo

 information about parallel evironment or error

#### Fields

**node**: `ParallelEnv`
  -  information about parallel evironment

**error**: `ParallelEnvError`
  -  information about error occured during some parallel evironment operation


### QueueInfo

 information about queue or error

#### Fields

**node**: `Queue`
  -  information about queue

**error**: `QueueError`
  -  information about error occured during some queue operation


### ResourceInfo

 information about resource or error

#### Fields

**node**: `Resource`
  -  information about resource

**error**: `ResourceError`
  -  information about error occured during some job operation


### ResvInfo

#### Fields

**node**: `Reservation`
  -  information about reservation

**error**: `ResvError`
  -  information about error occured during some reservation operation



## Directives

### @defer

The @defer directive may be specified on a fragment spread to imply de-prioritization, that causes the fragment to be omitted in the initial response, and delivered as a subsequent response afterward. A query with @defer directive will cause the request to potentially return multiple responses, where non-deferred data is delivered in the initial response and data deferred delivered in a subsequent response. @include and @skip take precedence over @defer.

**Locations:** FRAGMENT_SPREAD, INLINE_FRAGMENT

#### Arguments

- **if**: `Boolean` (default: `true`)
- **label**: `String`

### @deprecated

The @deprecated built-in directive is used within the type system definition language to indicate deprecated portions of a GraphQL service's schema, such as deprecated fields on a type, arguments on a field, input fields on an input type, or values of an enum type.

**Locations:** FIELD_DEFINITION, ARGUMENT_DEFINITION, INPUT_FIELD_DEFINITION, ENUM_VALUE

#### Arguments

- **reason**: `String` (default: `"No longer supported"`)

### @include

The @include directive may be provided for fields, fragment spreads, and inline fragments, and allows for conditional inclusion during execution as described by the if argument.

**Locations:** FIELD, FRAGMENT_SPREAD, INLINE_FRAGMENT

#### Arguments

- **if**: `Boolean!`

### @oneOf

The `@oneOf` _built-in directive_ is used within the type system definition language to indicate an Input Object is a OneOf Input Object.

**Locations:** INPUT_OBJECT


### @skip

The @skip directive may be provided for fields, fragment spreads, and inline fragments, and allows for conditional exclusion during execution as described by the if argument.

**Locations:** FIELD, FRAGMENT_SPREAD, INLINE_FRAGMENT

#### Arguments

- **if**: `Boolean!`

### @specifiedBy

The @specifiedBy built-in directive is used within the type system definition language to provide a scalar specification URL for specifying the behavior of custom scalar types.

**Locations:** SCALAR

#### Arguments

- **url**: `String!`
