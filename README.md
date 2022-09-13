# oee

[![License: MIT](https://img.shields.io/github/license/ramp-eu/TTE.project1.svg)](https://opensource.org/licenses/MIT)

An OEE calculator microservice to be used with Fiware Cygnus time series data. For more information, see the relevant [Fiware Cygnus tutorial](https://github.com/FIWARE/tutorials.Historic-Context-Flume).

## Contents

- [OEE](#title)
  - [Contents](#contents)
  - [Background](#background)
  - [Build](#build)
  - [Usage](#usage)
  - [API](#api)
  - [Testing](#testing)
  - [Limitations](#limitations)
  - [License](#license)

## Background

The [Fiware Orion Context Broker](https://github.com/Fiware/tutorials.Getting-Started) can be configured to send notifications to Fiware Cygnus whenever an object changes. Cygnus can be configured to log all historical data into a time series database (in our case, PostgreSQL).

The OEE microservice can handle systems matching the manufacturing system [requirements](#requirements). The JSONs stored in the Orion Context Broker must match the [JSONs](jsons). If configure the manufacturing system according to the way described in [Usage](#usage), we can calculate the OEE of each Workstation object, and also upload them to PostgreSQL for data visualisation.

## Build
You can run the component from a docker image. You can build it using the [Dockerfile](Dockerfile):

	docker build -t <component>:<version> .

## Requirements
The OEE microservice is designed to be able to handle manufacturing systems that match the criteria of the [Job-shop scheduling](https://en.wikipedia.org/wiki/Job-shop_scheduling) problem. The criteria:
- We are given n Jobs: J_1, J_2, ... J_n.
- We are given m Workstation.
- Each Job consists of a set of Operations: O_1, O_2, ... O_p.
- The number of Operations of the Jobs can differ.
- The Operations must be carried out in a specific order.
- For each Job, only one Operation can be processed at any given time.
- Each Operation can be processed only at a specific Workstation.

Currently, the ROSE-AP is not intended to be fully automatic. Whenever a new operation is started, human intervention is needed. A human must update the Workstation's following attributes: RefJob, CurrentOperationNumber and CurrentOperationType.

## Usage
The component is designed to run inside a docker-compose project defined in the Robo4Toys ROSE-AP's [docker-compose.yml](https://github.com/aviharos/momams/blob/main/docker-compose.yml) file. Whenever the docker-compose project is started, the OEE microservice also starts. However, the Component does not depend on any microservice besides the Orion Context Broker, Cygnus, MongoDB and PostgreSQL; so it can be used without many of the Robo4Toys TTE's microservices.

### Notifying Cygnus of all context changes
After running the docker-compose project, you need to set Orion to notify Cygnus of all context changes using the script:

	curl --location --request POST 'http://localhost:1026/v2/subscriptions' \
	--header 'Content-Type: application/json' \
	--data-raw '{
	  "description": "Notify Cygnus Postgres of all context changes",
	  "subject": {
	    "entities": [
	      {
	        "idPattern": ".*"
	      }
	    ]
	  },
	  "notification": {
	    "http": {
	      "url": "http://cygnus:5055/notify"
	    }
	  }
	}'

In the docker-compose file, Cygnus is configured to store all historic data into PostgreSQL.

### Objects in the Orion Context Broker
You need to create and keep these objects up-to-date in the Orion Context Broker according to the syntax of the [JSON files](json). You cannot change the attribute names, but you can change their content. You cannot change the object types. The manufacturing system and the processes are also defined in these json files.

- One Workstation object for each Workstation. In our case, there is one: "urn:ngsi_ld:Workstation:1", representing the robotic injection moulding cell.
- One Job object for each Job.
- One object for each of the following storages: CubeMagnetStorage, CoverMagnetStorage,            TrayLoaderStorage, TrayUnloaderStorage. The magnet storage objects store magnets needed for        assembly. The TrayLoaderStorage stores the trays that will be filled with injection moulded parts. The TrayUnLoaderStorage stores trays that cannot contain more of the current Part.
- One OperatorSchedule object containing the shift data.
- One object for each produced Part in the system. These contain OperationTime and PartsPerOperation values, one for each type of Job. For example, a part could have many JobTypes   (injection moulding, quality control, deburring, etc.). These operations have specific times and   batch sizes in the OperationTime and PartsPerOperation attributes, respectively.

### Relationships among the objects

The OEE microservice iterates over all Workstations.
For each Workstation, the app gets the current Job from the Workstation's RefJob attribute.
Then it downloads the current Job from the Orion broker. Then it gets the CurrentOperationType and the Part being processed.
Then it downloads the currently processed Part from the Orion broker. It then reads the OperationTime and PartsPerOperation based on the CurrentOperationType.

## Example

The OEE microservice iterates over the Workstations, gets to "urn:ngsi_ld:Workstation:1"
Current Job: RefJob: "urn:ngsi_ld:Job:202200045"

Downloads "urn:ngsi_ld:Job:202200045"
Current CurrentOperationType: "Core001_injection_moulding"
Current Part: RefPart: "urn:ngsi_ld:Part:Core001"

Downloads "urn:ngsi_ld:Part:Core001"
Current OperationTime: part_json --> find operation --> operation["OperationTime"]["value"] -> 46 sec.
Current PartsPerOperation: part_json --> find operation --> operation["PartsPerOperation"]["value"] -> 8 pcs.

If you cannot trace the Workstation through the Job object to the Part object's Operation, somethings is missing.

## API

The microservice does not contain an API.

## Testing

For testing, you need to create a conda environment and install necessary packages.

    conda create -n oee python=3.8
    conda activate oee
    conda install pandas psycopg2 requests sqlalchemy

Then run the tests as follows.
WARNING: the tests set environment variables, change the Orion broker and PostgreSQL data,
and need MOMAMS up and running. Any overwritten data is deleted forever. Proceed at your own risk.

    cd tests
    source env 
    python test_object_to_template.py
    python test_Orion.py
    python test_OEE.py
    python test_LoopHandler.py
    python test_main.py

## Limitations
We intend to more thorough testing and refactoring in the future. The OEE microservice cannot handle HTTPS and Fiware’s authentication system.

## License

[MIT license](LICENSE)

The Robo4Toys TTE does not hold any copyright of any FIWARE or 3rd party software.

