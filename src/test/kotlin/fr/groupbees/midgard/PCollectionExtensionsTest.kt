package fr.groupbees.midgard

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.assertj.core.api.Assertions.assertThat
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.io.Serializable

@RunWith(JUnitParamsRunner::class)
class PCollectionExtensionsTest : Serializable {

    @Transient
    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    fun mapPipelineParams(): Array<Any> {
        return arrayOf(
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .map("Map with nick name") { addNickNameToPlayer(it) }
                }
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn("Map with nick name", { addNickNameToPlayer(it) })
                }
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext("Map with nick name", { addNickNameToPlayer(it.element()) })
                }
            )
        )
    }

    fun mapPipelineWithLifecycleActionsParams(): Array<Any> {
        return arrayOf(
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it) },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it) },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_START_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it) },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_TEARDOWN
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it) },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP.plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFn(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it) },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
                    .plus(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext<Player, Player>(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it.element()) },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext<Player, Player>(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it.element()) },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_START_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext<Player, Player>(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it.element()) },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext<Player, Player>(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it.element()) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_TEARDOWN
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext<Player, Player>(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it.element()) },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP.plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            ),
            arrayOf(
                { p: TestPipeline, players: List<Player> ->
                    p
                        .apply("Create", Create.of(players))
                        .mapFnWithContext<Player, Player>(
                            "Map with nick name",
                            transform = { addNickNameToPlayer(it.element()) },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
                    .plus(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            )
        )
    }

    fun flatMapPipelineParams(): Array<Any> {
        return arrayOf(
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMap("FlatMap to Players") { it.players }
                }
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn("FlatMap to players", { it.players })
                }
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext("FlatMap to players", { it.element().players })
                }
            )
        )
    }

    fun flatMapPipelineWithLifecycleActionsParams(): Array<Any> {
        return arrayOf(
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn(
                            "FlatMap to players",
                            transform = { it.players },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn(
                            "FlatMap to players",
                            transform = { it.players },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_START_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn(
                            "FlatMap to players",
                            transform = { it.players },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn(
                            "FlatMap to players",
                            transform = { it.players },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_TEARDOWN
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn<Team, Player>(
                            "FlatMap to players",
                            transform = { it.players },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP.plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFn<Team, Player>(
                            "FlatMap to players",
                            transform = { it.players },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
                    .plus(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext<Team, Player>(
                            "FlatMap to players",
                            transform = { it.element().players },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext<Team, Player>(
                            "FlatMap to players",
                            transform = { it.element().players },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_START_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext<Team, Player>(
                            "FlatMap to players",
                            transform = { it.element().players },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext<Team, Player>(
                            "FlatMap to players",
                            transform = { it.element().players },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_TEARDOWN
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext<Team, Player>(
                            "FlatMap to players",
                            transform = { it.element().players },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP.plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            ),
            arrayOf(
                { p: TestPipeline, teams: List<Team> ->
                    p
                        .apply("Create", Create.of(teams))
                        .flatMapFnWithContext<Team, Player>(
                            "FlatMap to players",
                            transform = { it.element().players },
                            setupAction = { print(LIFECYCLE_ACTION_MESSAGE_SETUP) },
                            startBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE) },
                            finishBundleAction = { print(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE) },
                            teardownAction = { print(LIFECYCLE_ACTION_MESSAGE_TEARDOWN) }
                        )
                },
                LIFECYCLE_ACTION_MESSAGE_SETUP
                    .plus(LIFECYCLE_ACTION_MESSAGE_START_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE)
                    .plus(LIFECYCLE_ACTION_MESSAGE_TEARDOWN)
            )
        )
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "mapPipelineParams")
    fun givenOnePlayer_whenApplyMapToPlayerWithNickName_thenExpectedPlayerInResult(
        mapPipelineParams: (p: TestPipeline, players: List<Player>) -> PCollection<Player>
    ) {
        // Given.
        val player = Player(
            firstName = "Karim",
            lastName = "Benzema",
            age = 37
        )

        // When.
        val resultPlayers: PCollection<Player> = mapPipelineParams(pipeline, listOf(player))

        // Then.
        PAssert.that(resultPlayers).containsInAnyOrder(listOf(addNickNameToPlayer(player)))

        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "mapPipelineWithLifecycleActionsParams")
    fun givenOnePlayer_whenApplyMapToPlayerWithNickNameAndExecuteLifecycleActions_thenExpectedPlayerInResultAndActionExecuted(
        mapPipelineWithLifecycleActionsParams: (p: TestPipeline, players: List<Player>) -> PCollection<Player>,
        actionExpectedMessageConsole: String
    ) {
        // Given.
        val player = Player(
            firstName = "Karim",
            lastName = "Benzema",
            age = 37
        )

        // Allows testing side effect.
        // We perform a System.out.print and checks if the message has been correctly printed in the console.
        val outContent = overrideAndGetOutForTesting()

        // When.
        val resultPlayers: PCollection<Player> = mapPipelineWithLifecycleActionsParams(pipeline, listOf(player))

        // Then.
        PAssert.that(resultPlayers).containsInAnyOrder(listOf(addNickNameToPlayer(player)))

        pipeline.run().waitUntilFinish()

        assertThat(outContent.toString()).isEqualTo(actionExpectedMessageConsole)

        // Adds the original out at the end of test.
        setOriginalOut()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "flatMapPipelineParams")
    fun givenTwoTeamsWithPlayers_whenApplyFlatMapToPlayers_thenExpectedPlayersInResult(
        flatMapPipelineParams: (p: TestPipeline, teams: List<Team>) -> PCollection<Player>
    ) {
        // Given.
        val psgPlayers = listOf(
            Player(firstName = "Kylian", lastName = "Mbappe", age = 24),
            Player(firstName = "Marco", lastName = "Verrati", age = 29)
        )

        val realPlayers = listOf(
            Player(firstName = "Karim", lastName = "Benzema", age = 35),
            Player(firstName = "Junior", lastName = "Vinicius", age = 22)
        )

        val team1 = Team(
            name = "PSG",
            slogan = "ICI C'EST PARIS",
            players = psgPlayers
        )
        val team2 = Team(
            name = "REAL",
            slogan = "HALA MADRID",
            players = realPlayers
        )

        val expectedPlayers = listOf(psgPlayers, realPlayers).flatten()

        // When.
        val resultPlayers: PCollection<Player> = flatMapPipelineParams(pipeline, listOf(team1, team2))

        // Then.
        PAssert.that(resultPlayers).containsInAnyOrder(expectedPlayers)

        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "flatMapPipelineWithLifecycleActionsParams")
    fun givenTwoTeamsWithPlayers_whenApplyFlatMapToPlayersAndExecuteLifecycleAction_thenExpectedPlayersInResultAndActionExecuted(
        flatMapPipelineWithLifecycleActionsParams: (p: TestPipeline, teams: List<Team>) -> PCollection<Player>,
        actionExpectedMessageConsole: String
    ) {
        // Given.
        val psgPlayers = listOf(
            Player(firstName = "Kylian", lastName = "Mbappe", age = 24),
            Player(firstName = "Marco", lastName = "Verrati", age = 29)
        )

        val team = Team(
            name = "PSG",
            slogan = "ICI C'EST PARIS",
            players = psgPlayers
        )

        val outContent = overrideAndGetOutForTesting()

        // When.
        val resultPlayers: PCollection<Player> =
            flatMapPipelineWithLifecycleActionsParams(pipeline, listOf(team))

        // Then.
        PAssert.that(resultPlayers).containsInAnyOrder(psgPlayers)

        pipeline.run().waitUntilFinish()

        assertThat(outContent.toString()).isEqualTo(actionExpectedMessageConsole)

        setOriginalOut()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenTwoPlayers_whenApplyTwoFilterOnAgeAndLastName_thenExpectedOnePlayerInResult() {
        // Given.
        val benzemaPlayer = Player(firstName = "Karim", lastName = "Benzema", 37)

        val players = listOf(
            benzemaPlayer,
            Player(firstName = "Kylian", lastName = "Mbappe", 24),
            Player(firstName = "Marco", lastName = "Verrati", 28)
        )

        // When.
        val resultPlayers: PCollection<Player> = pipeline
            .apply("Create", Create.of(players))
            .filter("Filter on age") { it.age > 25 }
            .filter("Filter on last name") { it.lastName != "Verrati" }

        // Then.
        PAssert.that(resultPlayers).containsInAnyOrder(listOf(benzemaPlayer))

        pipeline.run().waitUntilFinish()
    }

    private fun overrideAndGetOutForTesting(): ByteArrayOutputStream {
        val outContent = ByteArrayOutputStream()
        System.setOut(PrintStream(outContent))

        return outContent
    }

    private fun setOriginalOut() {
        val originalOut = System.out
        System.setOut(originalOut)
    }

    private fun addNickNameToPlayer(player: Player): Player {
        return player.copy(nickname = "Kbenz")
    }

    companion object {
        const val LIFECYCLE_ACTION_MESSAGE_SETUP = "Action executed Setup"
        const val LIFECYCLE_ACTION_MESSAGE_START_BUNDLE = "Action executed Start Bundle"
        const val LIFECYCLE_ACTION_MESSAGE_FINISH_BUNDLE = "Action executed Finish Bundle"
        const val LIFECYCLE_ACTION_MESSAGE_TEARDOWN = "Action executed Teardown"
    }
}