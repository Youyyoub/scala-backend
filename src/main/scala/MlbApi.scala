package mlb

import com.github.tototoshi.csv._

import zio._
import zio.http._
import zio.jdbc._
import zio.stream.ZStream

import java.io.File
import java.time.LocalDate

import GameDates.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*
import HomeScores.*
import AwayScores.*
import HomeElos.*
import AwayElos.*
import HomeProbElos.*
import AwayProbElos.*

// The MlbApi object is used to launch the server and initialize the database. It defines all routes used to access data.
object MlbApi extends ZIOAppDefault {

  import DataService._
  import ApiService._

  // Test routes
  val static: App[Any] = Http
    .collect[Request] {
      case Method.GET -> Root / "text" => Response.text("Hello MLB Fans!")
      case Method.GET -> Root / "json" =>
        Response.json("""{"greetings": "Hello MLB Fans!"}""")
    }
    .withDefaultErrorResponse

  val endpoints: App[ZConnectionPool] = Http
    .collectZIO[Request] {
      // Initialize database
      case Method.GET -> Root / "init" =>
        ZIO.succeed(
          Response
            .text("Not Implemented, database is created at startup")
            .withStatus(Status.NotImplemented)
        )

      // Get the list of the last 20 matches played between two teams
      case Method.GET -> Root / "game" / "latests" / homeTeam / awayTeam =>
        for {
          games: List[Game] <- latests(HomeTeam(homeTeam), AwayTeam(awayTeam), 20)
          res: Response = gameHistoryResponse(games)
        } yield res

      // Predicts the outcome of a match between two given teams
      case Method.GET -> Root / "game" / "predict" / homeTeam / awayTeam =>
        for {
          game: List[Game] <- predictMatch(
            HomeTeam(homeTeam),
            AwayTeam(awayTeam)
          )
          res: Response = predictMatchResponse(game, homeTeam, awayTeam)
        } yield res

      // Get a team's ELO
      case Method.GET -> Root / "team" / "elo" / homeTeam =>
        for {
          team: Option[Game] <- latest(HomeTeam(homeTeam))
          res: Response = eloTeamGameResponse(team, homeTeam)
        } yield res

      // Get the total number of games in the database
      case Method.GET -> Root / "games" / "count" =>
        for {
          count: Option[Int] <- count
          res: Response = countResponse(count)
        } yield res

      // Get the total number of games in the database for a given team
      case Method.GET -> Root / "games" / "history" / team =>
        for {
          games: List[Game] <- findHistoryTeam(team)
          res: Response = gameHistoryResponse(games)
        } yield res
      case _ =>
        ZIO.succeed(Response.text("Not Found").withStatus(Status.NotFound))
    }
    .withDefaultErrorResponse

  val appLogic: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    _ <- for {
      // Create connection
      conn <- create

      // Load CSV file
      source <- ZIO.succeed(
        CSVReader.open(new File("./src/data/mlb_elo_latest.csv"))
      )

      stream <- ZStream
        .fromIterator[Seq[String]](source.iterator)
        .filter(row =>
          row.nonEmpty && row(0) != "date"
        ) // Skiping the first row and empty ones
        .map[Game](row =>
          // Create a Game instance from a row
          Game(
            GameDate(LocalDate.parse(row(0))),
            SeasonYear(row(1).toInt),
            HomeTeam(row(4)),
            AwayTeam(row(5)),
            HomeScore(row(24).toIntOption.getOrElse(-1)),
            AwayScore(row(25).toIntOption.getOrElse(-1)),
            HomeElo(row(6).toDouble),
            AwayElo(row(7).toDouble),
            HomeProbElo(row(8).toDouble),
            AwayProbElo(row(9).toDouble)
          )
        )
        .grouped(1000)
        .foreach(chunk => insertRows(chunk.toList))

      _ <- ZIO.succeed(source.close())
      res <- ZIO.succeed(conn)
    } yield res
    _ <- Server.serve[ZConnectionPool](static ++ endpoints)
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    appLogic.provide(
      createZIOPoolConfig >>> connectionPool,
      Server.defaultWithPort(8080)
    )
}

// The ApiService object retrieves query results, formats them correctly and sends them to the API
object ApiService {

  import zio.json.EncoderOps

  def countResponse(count: Option[Int]): Response = {
    count match
      case Some(c) =>
        Response.text(s"$c game(s) in historical data").withStatus(Status.Ok)
      case None =>
        Response.text("No game in historical data").withStatus(Status.NotFound)
  }

  def eloTeamGameResponse(game: Option[Game], homeTeam: String): Response = {
    game match {
      case Some(g) => {
        val elo = f"${g.homeElo}%1.1f"
        val res = s"$homeTeam Elo: $elo"
        Response.text(res).withStatus(Status.Ok)
      }
      case None =>
        Response
          .text("No game found in historical data")
          .withStatus(Status.NotFound)
    }
  }

  def latestGameResponse(game: Game): Response = {
    game match
      case null =>
        Response
        .text("No game found in historical data")
        .withStatus(Status.NotFound)
      case _ => Response.json(game.toJson).withStatus(Status.Ok)
  }

  def gameHistoryResponse(games: List[Game]): Response = {
    games match
      case List() =>
        Response
          .text("No game found in historical data")
          .withStatus(Status.NotFound)
      case _ => Response.json(games.toJson).withStatus(Status.Ok)
  }

  // The prediction based on a list of the latest games between the two teams
  def predictMatchResponse(
      games: List[Game],
      homeTeam: String,
      awayTeam: String
  ): Response = {
    games match
      case List() =>
        Response
          .text("No game found in historical data")
          .withStatus(Status.NotFound)

      case _ =>
        // Number of games won
        val homeWins: Int = games.count(g =>
          HomeScore.unapply(g.homeScore) > AwayScore.unapply(g.awayScore)
        )
        val awayWins: Int = games.count(g =>
          HomeScore.unapply(g.homeScore) < AwayScore.unapply(g.awayScore)
        )

        // Winrate
        val homeWinrate: Double = homeWins.toDouble / games.size * 100
        val awayWinrate: Double = awayWins.toDouble / games.size * 100

        // Formating the response
        val predictionResponse: String =
          s"Prediction: $homeTeam VS $awayTeam (based on ${games.size} games)"
            + s"\n$homeTeam winrate: " + f"$homeWinrate%1.0f" + "%"
            + " - "
            + s"$awayTeam winrate: " + f"$awayWinrate%1.0f" + "%"

        Response.text(predictionResponse).withStatus(Status.Ok)
  }
}

// The DataService object directly handles concrete requests to the database
object DataService {

  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  // Creates a connection with the database
  val connectionPool
      : ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "mlb",
      props = properties
    )

  // Sends an SQL query that creates a table to store Games
  val create: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
    execute(
      sql"CREATE TABLE IF NOT EXISTS games(date DATE NOT NULL, season_year INT NOT NULL, home_team VARCHAR(3), away_team VARCHAR(3), home_score INT, away_score INT, home_elo DOUBLE, away_elo DOUBLE, home_prob_elo DOUBLE, away_prob_elo DOUBLE)"
    )
  }

  // Sends an SQL query that adds a game to the database
  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = games.map(_.toRow)
    transaction {
      insert(
        sql"INSERT INTO games(date, season_year, home_team, away_team, home_score, away_score, home_elo, away_elo, home_prob_elo, away_prob_elo)"
          .values[Game.Row](rows)
      )
    }
  }

  // Sends an SQL query that retrieves the number of games in the database
  val count: ZIO[ZConnectionPool, Throwable, Option[Int]] = transaction {
    selectOne(
      sql"SELECT COUNT(*) FROM games".as[Int]
    )
  }

  /** Sends an SQL query that retrieves the list of the last `count` games between two teams
    *
    * @param homeTeam the home team
    * @param awayTeam the away team
    * @param count the max number of games you want to retrive 
    */
  def latests(homeTeam: HomeTeam, awayTeam: AwayTeam, count: Int): ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction {
      selectAll(
        sql"SELECT * FROM games WHERE (home_team = ${HomeTeam.unapply(homeTeam)} AND away_team = ${AwayTeam.unapply(awayTeam)}) ORDER BY date DESC LIMIT $count"
          .as[Game]
      ).map(_.toList)
    }
  }

  /** Sends an SQL query that retrieves the list of the games played by a team
    *
    * @param homeTeam the team for which you want the games
    */
  def latest(homeTeam: HomeTeam): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT * FROM games WHERE (home_team = ${HomeTeam.unapply(homeTeam)} OR away_team = ${HomeTeam.unapply(homeTeam)})"
          .as[Game]
      )
    }
  }

  /** Sends a SQL query that retrieves data from the last 20 games between two teams to predict the score of their next match
    *
    * @param homeTeam the home team
    * @param awayTeam the away team
    */
  def predictMatch(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction {
      selectAll(
        sql"SELECT * FROM games WHERE (home_team = ${HomeTeam.unapply(homeTeam)} AND away_team = ${AwayTeam.unapply(awayTeam)} AND home_score != -1 AND away_score != -1) ORDER BY date DESC LIMIT 20"
          .as[Game]
      ).map(_.toList)
    }
  }

  /** Sends an SQL query that retrieves the list of all games played by a given team
   * 
   * @param homeTeam the team for which you want the games list
   */
  def findHistoryTeam(homeTeam: String): ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction {
      selectAll(
        sql"SELECT * FROM games WHERE home_team = ${homeTeam}"
          .as[Game]
      ).map(_.toList)
    }
  }
}
