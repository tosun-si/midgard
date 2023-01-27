package fr.groupbees.midgard

import java.io.Serializable

data class Team(
    val name: String,
    val slogan: String,
    val players: List<Player>
) : Serializable