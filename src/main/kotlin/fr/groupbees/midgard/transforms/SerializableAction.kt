package fr.groupbees.midgard.transforms

import java.io.Serializable

/**
 * Function that allows to execute an action.
 *
 * No input and no output, only an action to execute.
 *
 * To be used in Beam Workers, we need to make this function as [java.io.Serializable].
 *
 * @author mazlum
 */
fun interface SerializableAction : Serializable {

    /**
     * Execute the action.
     */
    fun execute()
}