package fr.groupbees.midgard

import java.io.Serializable

data class Team(
    val firstName: String,
    val lastName: String,
    val age: Int,
    val nickname: String = ""
) : Serializable