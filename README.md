# oee

[![License: MIT](https://img.shields.io/github/license/ramp-eu/TTE.project1.svg)](https://opensource.org/licenses/MIT)

An OEE calculator microservice to be used with Fiware Cygnus time series data. A microservice of [MOMAMS](https://github.com/aviharos/momams).

## Contents

- [OEE](#title)
  - [Contents](#contents)
  - [Background](#background)
  - [Build](#build)
  - [Usage](#usage)
  - [API](#api)
  - [Demo](#demo)
  - [Testing](#testing)
  - [Troubleshooting](#troubleshooting)
  - [Limitations](#limitations)
  - [License](#license)

## Background

The [Fiware Orion Context Broker](https://github.com/Fiware/tutorials.Getting-Started) can be configured to send notifications to Fiware Cygnus whenever an object changes. Cygnus can be configured to log all historical data into a time series database (in our case, PostgreSQL), as you can see [here](https://github.com/FIWARE/tutorials.Historic-Context-Flume).

The OEE microservice can handle systems matching the manufacturing system [requirements](#requirements). The objects stored in the Orion Context Broker must match the data model (see an example configuration in [jsons](jsons)). If we configure the manufacturing system according to the way described in [Usage](#usage), we can calculate the OEE and ThroughputPerShift of each Workstation object.

Before reading further, it is strongly advised to read the following official Fiware tutorials:

- [Getting Started](https://github.com/FIWARE/tutorials.Getting-Started)
- [Entity Relationships](https://github.com/FIWARE/tutorials.Entity-Relationships)
- [Persisting Context Data using Apache Flume (MongoDB, MySQL, PostgreSQL)](https://github.com/FIWARE/tutorials.Historic-Context-Flume).

## Build

You can run the component from a docker image. You can build it using the [Dockerfile](Dockerfile):

	$ docker build -t oee:<version> .

## Requirements

The OEE microservice is designed to be able to handle manufacturing systems that match the criteria of the [Job-shop scheduling](https://en.wikipedia.org/wiki/Job-shop_scheduling) problem. The criteria:
- We are given n Jobs: J_1, J_2, ... J_n.
- We are given m Workstations.
- Each Job consists of a set of Operations: O_1, O_2, ... O_p.
- The number of Operations of the Jobs can differ.
- The Operations must be carried out in a specific order.
- For each Job, only one Operation can be processed at any given time.

The OEE microservice does not support any operation having different cycleTimes for different Workstations.

## Usage
The microservice is designed to run inside a docker compose project. See a minimal [docker-compose.yml](docker-compose.yml) file. The Robo4Toys TTE's project solution repository, MOMAMS also provides a more complete [docker-compose.yml](https://github.com/aviharos/momams/blob/main/docker-compose.yml) file. However, since the microservice does not depend on any microservice besides the Orion Context Broker, Cygnus, MongoDB and PostgreSQL; it can be used without the Robo4Toys TTE's other microservices.

The microservice does not store data or have any kind of memory. It just periodically performs a calculation. If the container crashes, it is safe to restart it automatically.

You do not need to activate the oee microservice. If you add the microservice to your docker compose file, create your manufacturing system's Orion Context Broker objects according to the data model and keep all data up-to-date in the Orion Context Broker, the microservice will automatically and periodically perform the OEE calculation for each Workstation object.

You need to set your timezone in the docker compose file in the oee microservice's environment. The timezone must be [one of the available ones in python](https://stackoverflow.com/questions/13866926/is-there-a-list-of-pytz-timezones).

### Notifying Cygnus of all context changes
After running the docker compose project, you need to set Orion to notify Cygnus of all context changes using the script:

    $ ./notify_cygnus.sh

This script should be executed once, immediately after starting the docker compose project.

In the docker compose file, Cygnus is configured to store all historic data into PostgreSQL.

### Objects in the Orion Context Broker
You need to investigate your manufacturing system and determine what events influence the OEE. Then you need to translate your manufacturing system to the data model outlined in [json](json) and keep it in sync with the physical system.

You need to design your objects as outlined below. When the Orion Context Broker is started, you need to post these objects into the broker to initiate them.

As the manufacturing process is working, you need to keep these objects up-to-date in the Orion Context Broker according to the data model. You can use any method you like: PLC HTTP connection, industrial PC, Raspberry Pi, etc. The point is that you need to take care that the Orion Context Broker's contents match the reality. The OEE microservice does not modify production data at all except for the OEE and ThroughputPerShift attributes of the Workstation objects.

Below you can find the design of the key objects. You cannot change the attribute names, but you can change their content. You cannot change the object types. The manufacturing system and the processes are also defined in these json files. You can arbitratily extend the data model with additional attributes and other types of objects. Make sure that if you have other Orion objects, none of them is of type `i40Asset` and of subType `Workstation`.

You need to configure and constantly update:
- One Workstation object for each Workstation. In the example json files, there is one: `urn:ngsiv2:i40Asset:Workstation:001`.
- One Job object for each Job.

In addition, you need to configure the manufacturing technology in the following objects. These objects rarely change.
- One Part object for each produced part in the system.
- One SequenceOfOperations object for each part, that contains the manufacturing sequence of operations.
- One Operation object for each operation in the manufacturing system.

In addition, you need to create
- One Shift object for each different Shift in the manufacturing system, containing the shift's start and end times.

You need to create each object upon startup in the Orion Context Broker. During the short term, only the number of Jobs is not known in advance, so you need to create as many as you need. The microservice does not have a function for creating these objects.

The OEE microservice never updates any object's any data except for the Workstation's OEE and ThroughputPerShift related attributes. Your manufacturing system or employees need to keep them up to date at all times.

The OEE microservice periodically updates the OEE and ThroughputPerShift related attributes of each Workstation object.

You can find examples for each object explained below.

#### Workstation

    {
        "id": "urn:ngsiv2:i40Asset:Workstation:001",
        "type": "i40Asset",
        "i40AssetType": {
            "type": "Text",
            "value": "Workstation"
        },
        "available": {
            "type": "Boolean",
            "value": true
        },
        "refJob": {
            "type": "Relationship",
            "value": "urn:ngsiv2:i40Process:Job:000001"
        },
        "refShift": {
            "type": "Relationship",
            "value": "urn:ngsiv2:i40Recipe:Shift:001"
        },
        "oee": {
            "type": "Number",
            "value": null
        },
        "oeeAvailability": {
            "type": "Number",
            "value": null
        },
        "oeePerformance": {
            "type": "Number",
            "value": null
        },
        "oeeQuality": {
            "type": "Number",
            "value": null
        },
        "oeeObject": {
            "type": "OEE",
            "value": {
                "oee": null,
                "availability": null,
                "performance": null,
                "quality": null 
            }
        },
        "throughputPerShift": {
            "type": "Number",
            "value": null
        }
    }

Attributes:
- i40AssetType (Text): Workstation. You cannot change it.
- available (Boolean): `true` if the Workstation is on, `false` otherwise.
- refJob (Relationship): refers to the currently active Job object.
- refShift (Relationship): refers to the Shift object that provides information about when the Workstation should be on or off. Since the Shift objects are not universal, each Workstation can refer to a different Shift.
The following attributes are calculated by the OEE microservice:
- oee (Number): the OEE metric of the Workstation. It is a number if all calculations succeed, `null` otherwise.
- oeeAvailability (Number): the Availability metric of the Workstation. It is a number if all calculations succeed, `null` otherwise.
- oeePerformance (Number): the Performance metric of the Workstation. It is a number if all calculations succeed, `null` otherwise.
- oeeQuality (Number): the Quality metric of the Workstation. It is a number if all calculations succeed, `null` otherwise.
- oeeObject (OEE): the OEE metric of the Workstation with all sub-metrics bundled into a single attribute.The values are as described above.
- throughputPerShift (Number): the estimated throughputPerShift metric of the Workstation. It is a number if all calculations succeed, `null` otherwise.

The Workstation cannot be turned on any day before the Shift's `start` attribute. The reason for this is that if the Workstation is turned off after the Shift's start time, the OEE microservice will find at least one entry in the Postgres logs and will not need to guess about the Workstation's availability. Not following this requirement may cause incorrect results.

#### Job
    {
        "id": "urn:ngsiv2:i40Process:Job:000001",
        "type": "i40Process",
        "i40ProcessType": {
            "type": "Text",
            "value": "Job"
        },
        "refPart": {
            "type": "Relationship",
            "value": "urn:ngsiv2:i40Asset:Part:core001"
        },
        "refOperation": {
            "type": "Relationship",
            "value": "urn:ngsiv2:i40Recipe:Operation:core001:001"
        },
        "jobTargetNumber": {
            "type": "Number",
            "value": 8000
        },
        "goodPartCounter": {
            "type": "Number",
            "value": 0
        },
        "rejectPartCounter": {
            "type": "Number",
            "value": 0
        }
    }

Attributes:
- i40ProcessType (Text): Job. Constant.
- refPart (Relationship): refers to the Part that the Job produces.
- refOperation (Relationship): refers to the current Operation of the Job that is being performed on the Workstation.
- jobTargetNumber (Number): the number of Parts to be produced in this Job.
- goodPartCounter (Number): the counter of the good parts.
- rejectPartCounter (Number): the counter of the reject parts.

#### Part 
    {
        "id": "urn:ngsiv2:i40Asset:Part:core001",
        "type": "i40Asset",
        "i40AssetType": {
            "type": "Text",
            "value": "Part"
        },
        "refSequenceOfOperations": {
            "type": "Relationship",
            "value": "urn:ngsiv2:SequenceOfOperations:Core001"
        }
    }

Attributes:
- i40AssetType (Text): Part. Constant.
- refSequenceOfOperations (Relationship): refers to the SequenceOfOperations object of the Part.

#### SequenceOfOperations

    {
        "id": "urn:ngsiv2:i40Recipe:sequenceOfOperations:core001",
        "type": "i40Recipe",
        "i40RecipeType": {
            "type": "Text",
            "value": "SequenceOfOperations"
        },
        "refPart": {
            "type": "Relationship",
            "value": "urn:ngsiv2:i40Asset:Part:core001"
        },
        "operations": {
            "type": "List",
            "value": [
                "urn:ngsiv2:i40Recipe:Operation:core001:001"
            ]
        }
    }

Attributes:
- i40RecipeType (Text): SequenceOfOperations. Constant.
- refPart (Relationship): refers to the Part object whose sequence of operations are included in this object.
- operations (List): contains the ids of the Operation objects in order that are needed to complete the manufacturing of each part.

The SequenceOfOperations objects are not compulsory. The OEE microservice does not use them. But they make the data model complete.

#### Operation

    {
        "id": "urn:ngsiv2:i40Recipe:Operation:core001:001",
        "type": "i40Recipe",
        "i40RecipeType": {
            "type": "Text",
            "value": "Operation"
        },
        "refSequenceOfOperations": {
            "type": "Relationship",
            "value": "urn:ngsiv2:i40Recipe:sequenceOfOperations:core001"
        },
        "cycleTime": {
            "type": "Number",
            "value": 46
        },
        "partsPerCycle": {
            "type": "Number",
            "value": 8
        }
    }

Attributes:
- i40RecipeType (Text): Operation. Constant.
- refSequenceOfOperations (Relationship): refers to the SequenceOfOperations object in which this operation is included.
- cycleTime (Number): the cycle time of the operation in seconds. Every cycle, the Workstation produces a set of parts. Important: the OEE microservice currently does not support sets of parts that contain good and bad parts too. Each cycle, the resulting set is considered to have 100% good or 100% reject parts.
- partsPerCycle (Number): each cycle produces this many parts.

If you cannot trace the Workstation through the Job object to the current Operation, something is missing.

#### Shift

    {
        "id": "urn:ngsiv2:i40Recipe:Shift:001",
        "type": "i40Recipe",
        "i40RecipeType": {
            "type": "Text",
            "value": "Shift"
        },
        "start": {
            "type": "Time",
            "value": "8:00:00"
        },
        "end": {
            "type": "Time",
            "value": "16:00:00"
        }
    }

Attributes:
- i40RecipeType (Text): Shift. Constant.
- start (Time): the time the Workstation should be turned on each day.
- end (Time): the time the Workstation should be turned off each day.

## API

The microservice does not contain an API.

## Demo

You can try the OEE microservice as described [here](https://github.com/aviharos/momams#try-momams).

## Testing

WARNING: the tests set environment variables, change the Orion Context Broker and PostgreSQL data. Any overwritten data is deleted forever. Proceed at your own risk.

For testing, you need to create an environment and install necessary packages. Example with conda:

    $ conda create -n oee python=3.8
    $ conda activate oee
    $ conda install pandas psycopg2 requests sqlalchemy

Then start the *test* [docker-compose](test/docker-compose.yml) project, that is not identical to the minimal [docker-compose.yml](docker-compose.yml) mentioned in Usage:

    $ cd test
    $ docker compose up -d

Then run the tests as follows.

    $ source env
    $ python test_Orion.py
    $ python test_OEE.py
    $ python test_LoopHandler.py
    $ python test_main.py

Now you can stop the test docker compose project:

    $ docker compose down

Please note that the tests were originally written in the GMT+2 time zone, so they might fail in other time zones.

## Troubleshooting

If you encounter any trouble using the OEE microservice, inspect its logs:

    $ docker logs <container name>

In the default MOMAMS setup, the container name is `momams-oee`.

## Limitations

The OEE microservice currently cannot handle HTTPS and Fiware’s authentication system.

## License

[MIT license](LICENSE)

The Robo4Toys TTE does not hold any copyright of any FIWARE or 3rd party software.

