package mlb

import zio.json._
import zio.jdbc._

import java.time.LocalDate

// OBJECTS USED IN THE GAME CLASS
// (The elements that define what a game is)

/* For each object, we define:
 *  - An opaque type
 *  - An "apply" method that handles instance creation
 *  - An "unapply" method that handles instance extraction
 *  - A JSON encoder and decoder
 */

object HomeTeams {

  opaque type HomeTeam = String

  object HomeTeam {

    def apply(value: String): HomeTeam = value

    def unapply(homeTeam: HomeTeam): String = homeTeam
  }

  given CanEqual[HomeTeam, HomeTeam] = CanEqual.derived
  implicit val homeTeamEncoder: JsonEncoder[HomeTeam] = JsonEncoder.string
  implicit val homeTeamDecoder: JsonDecoder[HomeTeam] = JsonDecoder.string
}

object AwayTeams {

  opaque type AwayTeam = String

  object AwayTeam {

    def apply(value: String): AwayTeam = value

    def unapply(awayTeam: AwayTeam): String = awayTeam
  }

  given CanEqual[AwayTeam, AwayTeam] = CanEqual.derived
  implicit val awayTeamEncoder: JsonEncoder[AwayTeam] = JsonEncoder.string
  implicit val awayTeamDecoder: JsonDecoder[AwayTeam] = JsonDecoder.string
}

object HomeScores {

  opaque type HomeScore = Int

  object HomeScore {

    def apply(value: Int): HomeScore = value

    def unapply(homeScore: HomeScore): Int = homeScore
  }

  given CanEqual[HomeScore, HomeScore] = CanEqual.derived
  implicit val homeScoreEncoder: JsonEncoder[HomeScore] = JsonEncoder.int
  implicit val homeScoreDecoder: JsonDecoder[HomeScore] = JsonDecoder.int
}

object AwayScores {

  opaque type AwayScore = Int

  object AwayScore {

    def apply(value: Int): AwayScore = value

    def unapply(awayScore: AwayScore): Int = awayScore
  }

  given CanEqual[AwayScore, AwayScore] = CanEqual.derived
  implicit val awayScoreEncoder: JsonEncoder[AwayScore] = JsonEncoder.int
  implicit val awayScoreDecoder: JsonDecoder[AwayScore] = JsonDecoder.int
}

object HomeElos {

  opaque type HomeElo = Double

  object HomeElo {

    def apply(value: Double): HomeElo = value

    def unapply(homeElo: HomeElo): Double = homeElo
  }

  given CanEqual[HomeElo, HomeElo] = CanEqual.derived
  implicit val homeEloEncoder: JsonEncoder[HomeElo] = JsonEncoder.double
  implicit val homeEloDecoder: JsonDecoder[HomeElo] = JsonDecoder.double
}

object AwayElos {

  opaque type AwayElo = Double

  object AwayElo {

    def apply(value: Double): AwayElo = value

    def unapply(awayElo: AwayElo): Double = awayElo
  }

  given CanEqual[AwayElo, AwayElo] = CanEqual.derived
  implicit val awayEloEncoder: JsonEncoder[AwayElo] = JsonEncoder.double
  implicit val awayEloDecoder: JsonDecoder[AwayElo] = JsonDecoder.double
}

object HomeProbElos {

  opaque type HomeProbElo = Double

  object HomeProbElo {

    def apply(value: Double): HomeProbElo = value

    def unapply(homeProbElo: HomeProbElo): Double = homeProbElo
  }

  given CanEqual[HomeProbElo, HomeProbElo] = CanEqual.derived
  implicit val homeProbEloEncoder: JsonEncoder[HomeProbElo] = JsonEncoder.double
  implicit val homeProbEloDecoder: JsonDecoder[HomeProbElo] = JsonDecoder.double
}

object AwayProbElos {

  opaque type AwayProbElo = Double

  object AwayProbElo {

    def apply(value: Double): AwayProbElo = value

    def unapply(awayProbElo: AwayProbElo): Double = awayProbElo
  }

  given CanEqual[AwayProbElo, AwayProbElo] = CanEqual.derived
  implicit val awayProbEloEncoder: JsonEncoder[AwayProbElo] = JsonEncoder.double
  implicit val awayProbEloDecoder: JsonDecoder[AwayProbElo] = JsonDecoder.double
}

object GameDates {

  opaque type GameDate = LocalDate

  object GameDate {

    def apply(value: LocalDate): GameDate = value

    def unapply(gameDate: GameDate): LocalDate = gameDate
  }

  given CanEqual[GameDate, GameDate] = CanEqual.derived
  implicit val gameDateEncoder: JsonEncoder[GameDate] = JsonEncoder.localDate
  implicit val gameDateDecoder: JsonDecoder[GameDate] = JsonDecoder.localDate
}

object SeasonYears {

  opaque type SeasonYear <: Int = Int

  object SeasonYear {

    def apply(year: Int): SeasonYear = year

    def safe(value: Int): Option[SeasonYear] =
      Option.when(value >= 1876 && value <= LocalDate.now.getYear)(value)

    def unapply(seasonYear: SeasonYear): Int = seasonYear
  }

  given CanEqual[SeasonYear, SeasonYear] = CanEqual.derived
  implicit val seasonYearEncoder: JsonEncoder[SeasonYear] = JsonEncoder.int
  implicit val seasonYearDecoder: JsonDecoder[SeasonYear] = JsonDecoder.int
}

object PlayoffRounds {

  opaque type PlayoffRound <: Int = Int

  object PlayoffRound {

    def apply(round: Int): PlayoffRound = round

    def safe(value: Int): Option[PlayoffRound] =
      Option.when(value >= 1 && value <= 4)(value)

    def unapply(playoffRound: PlayoffRound): Int = playoffRound
  }

  given CanEqual[PlayoffRound, PlayoffRound] = CanEqual.derived
  implicit val playoffRoundEncoder: JsonEncoder[PlayoffRound] = JsonEncoder.int
  implicit val playoffRoundDEncoder: JsonDecoder[PlayoffRound] = JsonDecoder.int
}

// IMPLEMENTATION OF THE GAME CLASS

import GameDates.*
import PlayoffRounds.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*
import HomeScores.*
import AwayScores.*
import HomeElos.*
import AwayElos.*
import HomeProbElos.*
import AwayProbElos.*

// We haven't implemented the playoff round in our game management because we haven't managed to deal with the fact that it can be optional.
final case class Game(
    date: GameDate,
    season: SeasonYear,
    // playoffRound: Option[PlayoffRound],
    homeTeam: HomeTeam,
    awayTeam: AwayTeam,
    homeScore: HomeScore,
    awayScore: AwayScore,
    homeElo: HomeElo,
    awayElo: AwayElo,
    homeProbElo: HomeProbElo,
    awayProbElo: AwayProbElo
)

object Game {

  given CanEqual[Game, Game] = CanEqual.derived
  implicit val gameEncoder: JsonEncoder[Game] = DeriveJsonEncoder.gen[Game]
  implicit val gameDecoder: JsonDecoder[Game] = DeriveJsonDecoder.gen[Game]

  def unapply(game: Game): (
      GameDate,
      SeasonYear,
      // Option[PlayoffRound],
      HomeTeam,
      AwayTeam,
      HomeScore,
      AwayScore,
      HomeElo,
      AwayElo,
      HomeProbElo,
      AwayProbElo
  ) =
    (
      game.date,
      game.season,
      // game.playoffRound,
      game.homeTeam,
      game.awayTeam,
      game.homeScore,
      game.awayScore,
      game.homeElo,
      game.awayElo,
      game.homeProbElo,
      game.awayProbElo
    )

  // a custom decoder from a tuple
  type Row = (
      String,
      Int,
      // Option[Int],
      String,
      String,
      Int,
      Int,
      Double,
      Double,
      Double,
      Double
  )

  extension (g: Game)
    def toRow: Row =
      val (
        d,
        y,
        // p,
        h,
        a,
        hs,
        as,
        he,
        ae,
        hpe,
        ape
      ) = Game.unapply(g)
      (
        GameDate.unapply(d).toString,
        SeasonYear.unapply(y),
        // p.map(PlayoffRound.unapply),
        HomeTeam.unapply(h),
        AwayTeam.unapply(a),
        HomeScore.unapply(hs),
        AwayScore.unapply(as),
        HomeElo.unapply(he),
        AwayElo.unapply(ae),
        HomeProbElo.unapply(hpe),
        AwayProbElo.unapply(ape)
      )

  implicit val jdbcDecoder: JdbcDecoder[Game] = JdbcDecoder[Row]().map[Game] {
    t =>
      val (
        date,
        season,
        // maybePlayoff,
        home,
        away,
        homeScore,
        awayScore,
        homeElo,
        awayElo,
        homeProbElo,
        awayProbElo
      ) = t
      Game(
        GameDate(LocalDate.parse(date)),
        SeasonYear(season),
        // maybePlayoff.map(PlayoffRound(_)),
        HomeTeam(home),
        AwayTeam(away),
        HomeScore(homeScore),
        AwayScore(awayScore),
        HomeElo(homeElo),
        AwayElo(awayElo),
        HomeProbElo(homeProbElo),
        AwayProbElo(awayProbElo)
      )
  }
}

// val games: List[Game] = List(
//   Game(GameDate(LocalDate.parse("2021-10-03")), SeasonYear(2023), None, HomeTeam("ATL"), AwayTeam("NYM")),
//   Game(GameDate(LocalDate.parse("2021-10-03")), SeasonYear(2023), None, HomeTeam("STL"), AwayTeam("CHC"))
// )
