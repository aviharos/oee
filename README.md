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

The OEE microservice can handle systems matching the manufacturing system [requirements](#requirements). The objects stored in the Orion Context Broker must match the data model (see an example configuration in [jsons](jsons)). If we configure the manufacturing system according to the way described in [Usage](#usage), we can calculate the OEE and Throughput of each Workstation object, and also upload them as objects to Orion.

## Build
You can run the component from a docker image. You can build it using the [Dockerfile](Dockerfile):

	docker build -t oee:<version> .

## Requirements
The OEE microservice is designed to be able to handle manufacturing systems that match the criteria of the [Job-shop scheduling](https://en.wikipedia.org/wiki/Job-shop_scheduling) problem. The criteria:
- We are given n Jobs: J_1, J_2, ... J_n.
- We are given m Workstations.
- Each Job consists of a set of Operations: O_1, O_2, ... O_p.
- The number of Operations of the Jobs can differ.
- The Operations must be carried out in a specific order.
- For each Job, only one Operation can be processed at any given time.

The OEE microservice does not support different CycleTimes for different Workstations.

Currently, the microservice is not intended to be fully automatic. Whenever a new operation is started, human intervention is needed. A human must update a few attributes as outlined below.

## Usage
The microservice is designed to run inside a docker-compose project. See a minimal [docker-compose.yml](docker-compose.yml) file. The Robo4Toys TTE's project solution repository, MOMAMS also provides a more compley [docker-compose.yml](https://github.com/aviharos/momams/blob/main/docker-compose.yml) file. However, since the microservice does not depend on any microservice besides the Orion Context Broker, Cygnus, MongoDB and PostgreSQL; it can be used without the Robo4Toys TTE's other microservices.

The microservice does not store data or have any kind of memory. It just periodically performs a calculation. If the container crashes, it is safe to restart it automatically.

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
You need to create and keep these objects up-to-date in the Orion Context Broker according to the data model in [json](json). You cannot change the attribute names, but you can change their content. You cannot change the object types. The manufacturing system and the processes are also defined in these json files. You can arbitratily extend the data model with additional attributes.

You need to configure and constantly update:
- One Workstation object for each Workstation. In our case, there is one: "urn:ngsi_ld:Workstation:1".
- One Job object for each Job.
- One OperatorSchedule object containing the Workstation's schedule data.
- One object for each produced Part in the system. Each part contains the manufacturing data of the parts, including all operations.

You need to create each object each object upon startup in the Orion Context Broker. During the short term, only the number of Jobs is not known in advance, so you need to create as many as you need. The microservice does not have a function for creating these objects.

The OEE microservice never updates the Workstation, Job, OperatorSchedule and Part objects. Your manufacturing system or employeer need to keep them up to date.

The OEE microservice creates and updates 2 objects for each Workstation with the ids contained in the Workstation object: an OEE and a Throughput object for each Workstation.

You can find examples for each object explained below.

#### Workstation

    {
        "type": "Workstation",
        "id": "urn:ngsi_ld:Workstation:1",
        "Available": {"type": "Boolean", "value": true},
        "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
        "RefOEE": {"type": "Relationship", "value": "urn:ngsi_ld:OEE:1"},
        "RefThroughput": {"type": "Relationship", "value": "urn:ngsi_ld:Throughput:1"},
        "RefOperatorSchedule": {"type": "Relationship", "value": "urn:ngsi_ld:OperatorSchedule:1"}
    }

Attributes:
- Available (Boolean): true if the Workstation is on, false otherwise.
- RefJob (Relationship): refers to the currently active Job object.
- RefOEE (Relationship): refers to the Workstation's OEE object.
- RefThroughput (Relationship): refers to the Workstation's Throughput object.
- RefOperatorSchedule (Relationship): refers to the OperatorSchedule object that provides information about when the Workstation should be on or off. Since the OperatorSchedule objects are not universal, each Workstation can refer to a different OperatorSchedule.

#### Job

    {
        "type": "Job",
        "id": "urn:ngsi_ld:Job:202200045",
        "RefPart": {"type": "Relationship", "value": "urn:ngsi_ld:Part:Core001"},
        "CurrentOperationType": {"type": "Text", "value": "Core001_injection_moulding"},
        "JobTargetNumber": {"type": "Number", "value": 8000},
        "GoodPartCounter": {"type": "Number", "value": 0},
        "RejectPartCounter": {"type": "Number", "value": 0}
    }

Attributes:
- RefPart (Relationship): the Part that the Job produces.
- CurrentOperationType (Text): refers to the current operation's OperationType inside the Part's Operations attribute. The OperationType acts as an id to the operation.
- JobTargetNumber (Number): the number of Parts to be produced in this Job.
- GoodPartCounter (Number): the counter of the good parts.
- RejectPartCounter (Number): the counter of the reject parts.

#### Part 

    {
        "type": "Part",
        "id": "urn:ngsi_ld:Part:Core001",
        "Operations": {
            "type": "List",
            "value": [
                {
                    "type": "Operation",
                    "OperationNumber": {"type": "Number", "value": 10},
                    "OperationType": {"type": "Text", "value": "Core001_injection_moulding"},
                    "CycleTime": {"type": "Number", "value": 46},
                    "PartsPerCycle": {"type": "Number", "value": 8}
                }
            ]
        }
    }

Attributes:
- Operations (List): Contains the Operations.

The Operations are not separate Orion objects. Their attributes are as follows:
- OperationNumber (Number): the Operation's number. Currently it is not used by the microservice, but you can provide this information.
- OperationType (Number): this is the id of the Operation that the Job refers to. The Job's CurrentOperationType must always refer to the current operation's OperationType.
- CycleTime (Number): the cycle time of the operation in seconds. Every cycle, the Workstation produces a set of parts.
- PartsPerCycle (Number): each cycle produces this many parts.

If you cannot trace the Workstation through the Job object to the Part object's Operation, something is missing.

#### OperatorSchedule 

    {
        "type": "OperatorSchedule",
        "id": "urn:ngsi_ld:OperatorSchedule:1",
        "OperatorWorkingScheduleStartsAt": {"type": "Time", "value": "8:00:00"},
        "OperatorWorkingScheduleStopsAt": {"type": "Time", "value": "16:00:00"}
    }

Attributes:
- OperatorWorkingScheduleStartsAt (Time): the time the Workstation should be turned on each day.
- OperatorWorkingScheduleStopsAt (Time): the time the Workstation should be turned off each day.

#### OEE 

    {
        "type": "OEE",
        "id": "urn:ngsi_ld:OEE:1",
        "RefWorkstation": {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"},
        "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
        "Availability": {"type": "Number", "value": 0.9},
        "Performance": {"type": "Number", "value": 0.9},
        "Quality": {"type": "Number", "value": 0.9},
        "OEE": {"type": "Number", "value": 0.729}
    }

Attributes:
- RefWorkstation (Relationship): the Workstation object the OEE object belongs to.
- RefJob (Relationship): the Job object the OEE object belongs to. The OEE microservice does not consider multiple Jobs, only the current one.
- Availability (Number): the current Availability of the referred Workstation.
- Performance (Number): the current Performance of the referred Workstation.
- Quality (Number): the current Quality of  the referred Workstation.
- OEE (Number): the current OEE of  the referred Workstation.

The OEE microservice deletes the Availability, Performance, Quality and OEE attributes' value if the Workstation should not be on according to its OperatorSchedule or if the calculation fails for some reason.


#### Throughput

    {
        "type": "Throughput",
        "id": "urn:ngsi_ld:Throughput:1",
        "RefWorkstation": {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"},
        "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
        "ThroughputPerShift": {"type": "Number", "value": 4100}
    }

Attributes:
- RefWorkstation (Relationship): the Workstation object the Throughput object belongs to.
- RefJob (Relationship): the Job object the Throughput object belongs to. The OEE microservice does not consider multiple Jobs, only the current one.
- ThroughputPerShift (Number): the predicted throughput of the Workstation for the amount of time it should be turned on according to its OperatorSchedule.

## API

The microservice does not contain an API.

## Testing

WARNING: the tests set environment variables, change the Orion Context Broker and PostgreSQL data. Any overwritten data is deleted forever. Proceed at your own risk.

For testing, you need to create an environment and install necessary packages. Example with conda:

    conda create -n oee python=3.8
    conda activate oee
    conda install pandas psycopg2 requests sqlalchemy

Then start the *test* [docker-compose](test/docker-compose.yml) project, that is not identical to the minimal [docker-compose.yml](docker-compose.yml) mentioned in Usage:

    cd test
    docker-compose up -d

Then run the tests as follows.

    source env 
    python test_object_to_template.py
    python test_Orion.py
    python test_OEE.py
    python test_LoopHandler.py
    python test_main.py

Now you can stop the test docker-compose project:

    docker-compose down

Please note that the tests were originally written in the GMT+2 time zone. The tests have not been executed in any other timezone yet.

## Limitations
The OEE microservice currently cannot handle HTTPS and Fiware’s authentication system.

## License

[MIT license](LICENSE)

The Robo4Toys TTE does not hold any copyright of any FIWARE or 3rd party software.

