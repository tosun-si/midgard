package fr.groupbees.midgard

import junitparams.JUnitParamsRunner
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TypeDescriptor
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import java.io.Serializable

@RunWith(JUnitParamsRunner::class)
class CodeExampleTest : Serializable {

    @Transient
    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    @Test
    @Category(ValidatesRunner::class)
    fun testThreeOperator() {
        val psgPlayers = listOf(
            Player(firstName = "Kylian", lastName = "Mbappe", 24),
            Player(firstName = "Marco", lastName = "Verrati", 28)
        )

        val realPlayers = listOf(
            Player(firstName = "Karim", lastName = "Benzema", 35),
            Player(firstName = "Luca", lastName = "Modric", 39)
        )

        // Given.
        val psgTeam = Team(name = "PSG", slogan = "Ici c'est Paris", psgPlayers)
        val realTeam = Team(name = "REAL", slogan = "Hala Madrid", realPlayers)

        // Usual Beam pipeline.
        val resultPlayers: PCollection<Player> = pipeline
            .apply("Create", Create.of(listOf(psgTeam, realTeam)))
            .apply(
                "To Team with Slogan V2",
                MapElements
                    .into(TypeDescriptor.of(Team::class.java))
                    .via(SerializableFunction { it.copy(slogan = "${it.slogan} VERSION 2") })
            )
            .apply(
                "To Players",
                FlatMapElements
                    .into(TypeDescriptor.of(Player::class.java))
                    .via(SerializableFunction { it.players })
            )
            .apply("Filter age > 25", Filter.by(SerializableFunction { it.age > 25 }))

        // Beam pipeline with Midgard.
        val resultPlayersMidgard: PCollection<Player> = pipeline
            .apply("Create", Create.of(listOf(psgTeam, realTeam)))
            .map("To Team with Slogan V2") { it.copy(slogan = "${it.slogan} VERSION 2") }
            .flatMap("To Players") { it.players }
            .filter("Filter age > 25") { it.age > 25 }


        // Beam pipeline with Midgard.
        val resultTeamMidgardMapLifeCycle: PCollection<Team> = pipeline
            .apply("Create", Create.of(listOf(psgTeam, realTeam)))
            .mapFn(
                name = "To Team with Slogan V2",
                transform = { it.copy(slogan = "${it.slogan} VERSION 2") },
                setupAction = { println("Setup Action") },
                startBundleAction = { println("Start Bundle Action") },
                finishBundleAction = { println("Finish Bundle Action") },
                teardownAction = { println("Teardown Action") }
            )

        val resultPlayersMidgardFlatMapLifeCycle: PCollection<Player> = pipeline
            .apply("Create", Create.of(listOf(psgTeam, realTeam)))
            .map("To Team with Slogan V2") { it.copy(slogan = "${it.slogan} VERSION 2") }
            .flatMapFn(
                name = "To Players",
                transform = { it.players },
                setupAction = { println("Setup Action") },
                startBundleAction = { println("Start Bundle Action") },
                finishBundleAction = { println("Finish Bundle Action") },
                teardownAction = { println("Teardown Action") }
            )

        // Then.
//        PAssert.that(resultPlayers).containsInAnyOrder(listOf(benzemaPlayer))

        val slogansSideInput: PCollectionView<String> = pipeline
            .apply("Read slogans", Create.of("VERSION 2"))
            .apply("Create as collection view", View.asSingleton())

        val resultTeamMidgardMapContextLifeCycle: PCollection<Team> = pipeline
            .apply("Create", Create.of(listOf(psgTeam, realTeam)))
            .mapFnWithContext(
                name = "To Team with Slogan V2",
                transform = { context -> toTeamWithSloganSuffixFromSideInput(slogansSideInput, context) },
                setupAction = { println("Setup Action") },
                startBundleAction = { println("Start Bundle Action") },
                finishBundleAction = { println("Finish Bundle Action") },
                teardownAction = { println("Teardown Action") }
            )

        val resultPlayersMidgardFlatMapContextLifeCycle: PCollection<Player> = pipeline
            .apply("Create", Create.of(listOf(psgTeam, realTeam)))
            .map("To Team with Slogan V2") { it.copy(slogan = "${it.slogan} VERSION 2") }
            .flatMapFnWithContext(
                name = "To Players",
                transform = { context -> context.element().players },
                setupAction = { println("Setup Action") },
                startBundleAction = { println("Start Bundle Action") },
                finishBundleAction = { println("Finish Bundle Action") },
                teardownAction = { println("Teardown Action") }
            )

        pipeline.run().waitUntilFinish()
    }

    private fun toTeamWithSloganSuffixFromSideInput(
        sideInput: PCollectionView<String>,
        context: DoFn<Team, Team>.ProcessContext
    ): Team {
        val currentTeam: Team = context.element()
        val sloganSuffixSideInput: String = context.sideInput(sideInput)

        return currentTeam.copy(slogan = "${currentTeam.slogan} $sloganSuffixSideInput")
    }

    private fun addNickNameToPlayer(player: Player): Player {
        return player.copy(nickname = "Kbenz")
    }
}