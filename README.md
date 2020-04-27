# Node Job Queue Example

### worker.js

A job queue which has three rabbitmq queues running (small, medium, large) to handle consuming incoming
messages. Using `node-resque`, I have implemented a multi-worker to handle concurrent processing of jobs.

Three jobs make up the job queue, `uploadData`, `requestStatus` and `sendResponse`.

The job `uploadData`
handles the initial upload of the data, which contains the number of jobs to process for the upload.

The job `requestStatus` polls the status of a requestId that is passed to the job. If the requestId fails
to resolve then it will be marked with a failure status. If the requestId resolves successfully, it is
marked as such. Every time a request is resolved, either successfully or unsuccessfully, the overall status
of the upload is checked to determine if a `sendResponse` job should be created.

The `sendResponse` job
is responsible for notifying users that their upload has finished.

### task_creator.js

A rabbitmq channel that prodcues a configured amount of uploads with a configured amount of jobs per upload.
