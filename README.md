The project contains the template.yaml file which contains the AWS SAM code to deploy the required infastructure, The file raw_data_files_s3_uploader.py contains the code to upload the raw json files from the data folder into s3 bucket at 15 min intervals. The python file uses paramiko library to connect via sftp to a sftp server via aws transfer family, it requires a private key to connect to the server which we have provided as "eon" (you can generate and use your own ssh key as well, in which case you also need to adjust the key password inside the raw_data_files_s3_uploader.py).

This step needs to be done manually because aws transfer family is not supported by neither aws sam nor terraform, but it does provide a secure and scalable way to upload files into s3 buckets. You need to go the aws transfer family and create a new server, once created , add a new user by the name "test-user" and assign to the user the role created by the SAM template by the name of "eon-transfer-role" and also attach the policy "eon-ingest-SFTPS3READWRITE" to this user, this policy is also created by the sam template. This role provides the test-user permissions to upload files into the "eon-s3bronze" bucket. Also set the "eon-s3bronze" bucket as the home directory of this user, Finally add the contents of the public ssh key "eon.pub" (or the public ssh key that you generated yourself) to the ssh fingerprint of the test-user.

Once the sftp server and the user are created, kindly adjust the raw_data_files_s3_uploader.py to change the HOSTNAME and replace it with the url of your own sftp server.

You can deploy the template using the following 2 commands

sam build

sam deploy --capabilities CAPABILITY_NAMED_IAM


In order to run the raw_data_files_s3_uploader.py and the unit tests, please install the dependencies in the requirements.txt located in the root folder (this project was built using python3.12)

In order to run the unit test please execute the shell script build.sh

The whole working of the project once deployed is explained below

raw_data_files_s3_uploader.py : uploads the raw json files into eon-s3bronze bucket every 15 minutes, this bucket is configured to send a notification to the sqs queue named "eon-sqs-queue", The lambda function "EonSQSPoller" polls the queue for incoming messages which contains the object key of the uploaded item, this lambda function is triggered every 5 mins by a cloudwatchevent trigger. Once this lambda function executes, it invokes the step function and passes the object key as input, the step function first executes the "EonFileProcessorFunction" lambda function which reads the json file and performs validation checks on it, if successful, it converts the json into a parquet file and pushes the file into the eon-s3silver bucket using a similar path, after that the step function delays for 5 mins and then executes the "EonParquetProcessorFunction" lambda function, which combines all the files on a given day folder into a single snappy compressed parquet file and pushes it into the combined folder inside the eon-s3silverbucket.




