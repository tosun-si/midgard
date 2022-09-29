package fr.groupbees.midgard

import junitparams.JUnitParamsRunner
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import java.io.Serializable


@RunWith(JUnitParamsRunner::class)
class PCollectionExtensionsTest : Serializable {

    @Transient
    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    @Test
    @Category(ValidatesRunner::class)
    fun givenOnePlayer_whenApplyMapToPlayerWithNickName_thenExpectedPlayerInResult() {
        // Given.
        val player = Player(
            firstName = "Karim",
            lastName = "Benzema",
            age = 37
        )

        // When.
        val resultPlayers: PCollection<Player> = pipeline
            .apply("Create", Create.of(listOf(player)))
            .map("Map with nick name") { addNickNameToPlayer(it) }

        // Then.
        PAssert.that(resultPlayers).containsInAnyOrder(listOf(addNickNameToPlayer(player)))

        pipeline.run().waitUntilFinish()
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

    private fun addNickNameToPlayer(player: Player): Player {
        return player.copy(nickname = "Kbenz")
    }
}