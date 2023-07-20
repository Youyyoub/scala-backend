# Functional programming project #2
### Building a ZIO Application Backend

**Authors :**
Karam MANSOUR, Sofian YAHYAOUI, Ayyoub ZEBDA

## Overview
The application is a REST API that communicates with a database of baseball games provided by MLB.

## Run the application
Simply run the `src/main/scala/MlbApi.scala` file to start the server. If the http://localhost:8080/text route returns the "Hello MLB Fans!" string, it works!

You can change the file used by modifying the path in the following line in the `appLogic` method of the `MlbApi` class:
`CSVReader.open(new File("./src/data/mlb_elo_latest.csv"))`

You can also change your port by modifyling the following line in the `run` method of the `MlbApi` class:
`Server.defaultWithPort(8080)`

It is then possible to communicate with the server via the following endpoints:
- Get the list of the last 20 matches played between two teams (you can modify the number of games recovered by modifying the associated function): http://localhost:8080/game/latests/{team1}/{team2}
- Predicts the outcome of a match between two given teams: http://localhost:8080/game/predict/{team1}/{team2}
- Get a team's ELO: http://localhost:8080/team/elo/{team}
- Get the total number of games in the database: http://localhost:8080/games/count
- Get the total number of games in the database for a given team: http://localhost:8080/games/history/{team}

## Example endpoints
http://localhost:8080/game/latests/LAD/MIL
http://localhost:8080/game/predict/LAD/MIL
http://localhost:8080/team/elo/LAD
http://localhost:8080/games/count
http://localhost:8080/games/history/LAD