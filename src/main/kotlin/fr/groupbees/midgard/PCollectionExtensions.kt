package fr.groupbees.midgard

import fr.groupbees.midgard.transforms.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TypeDescriptor
import java.io.Serializable

/**
 * Extension for a map operation on a [PCollection].
 *
 * ```kotlin
 * inputTeamsPCollection
 *     .map("Step name") { team -> TestSettings.toOtherTeam(team) }
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <I, reified O : Serializable> PCollection<I>.map(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<I, O>
): PCollection<O> {
    return this.apply(name, MapElements.into(TypeDescriptor.of(O::class.java)).via(transform))
}

/**
 * Extension for a flatMap operation on a [PCollection].
 *
 * ```kotlin
 * inputTeamsPCollection
 *     .flatMap("Step name") { team -> team.players }
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <I, reified O : Serializable> PCollection<I>.flatMap(
    name: String = "flatMap to ${O::class.simpleName}",
    transform: SerializableFunction<I, Iterable<O>>
): PCollection<O> {
    return this.apply(name, FlatMapElements.into(TypeDescriptor.of(O::class.java)).via(transform))
}

/**
 * Extension for a map operation in a [PCollection] with DoFn lifecycle functions.
 * Via this method, while the transformation is done, some methods can be passed to interact with DoFn lifecycle methods
 * (setup, startBundle, finishBundle, teardown).
 *
 * ```kotlin
 *  inputTeamsPCollection
 *      .mapFn(
 *          "Step name",
 *          { team -> TestSettings.toOtherTeam(team) },
 *          setupAction = { print("Test setup action") },
 *          startBundleAction = { print("Test start bundle action") },
 *          finishBundleAction = { print("Test finish bundle action") },
 *          teardownAction = { print("Test teardown action") },
 *      )
 *      .result
 * ```
 *
 * @param name step name
 * @param transform current transformation function
 * @param setupAction setup action function
 * @param startBundleAction start bundle action function
 * @param finishBundleAction finish bundle function
 * @param teardownAction teardown action function
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <I, reified O : Serializable> PCollection<I>.mapFn(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<I, O>,
    setupAction: SerializableAction = SerializableAction { },
    startBundleAction: SerializableAction = SerializableAction { },
    finishBundleAction: SerializableAction = SerializableAction { },
    teardownAction: SerializableAction = SerializableAction { }
): PCollection<O> {
    val doFn = MapElementFn
        .into(TypeDescriptor.of(O::class.java))
        .via(transform)
        .withSetupAction(setupAction)
        .withStartBundleAction(startBundleAction)
        .withFinishBundleAction(finishBundleAction)
        .withTeardownAction(teardownAction);

    return this.apply(name, ParDo.of(doFn))
}


/**
 * Extension for a flatMap operation in a [PCollection] with DoFn lifecycle functions.
 * Via this method, while the transformation is done, some methods can be passed to interact with DoFn lifecycle methods
 * (setup, startBundle, finishBundle, teardown).
 *
 * ```kotlin
 *  inputTeamsPCollection
 *      .flatMapFn(
 *          "Step name",
 *          { team -> team.players },
 *          setupAction = { print("Test setup action") },
 *          startBundleAction = { print("Test start bundle action") },
 *          finishBundleAction = { print("Test finish bundle action") },
 *          teardownAction = { print("Test teardown action") },
 *      )
 *      .result
 * ```
 *
 * @param name step name
 * @param transform current transformation function
 * @param setupAction setup action function
 * @param startBundleAction start bundle action function
 * @param finishBundleAction finish bundle action function
 * @param teardownAction teardown action function
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <I, reified O : Serializable> PCollection<I>.flatMapFn(
    name: String = "flatMap to ${O::class.simpleName}",
    transform: SerializableFunction<I, Iterable<O>>,
    setupAction: SerializableAction = SerializableAction { },
    startBundleAction: SerializableAction = SerializableAction { },
    finishBundleAction: SerializableAction = SerializableAction { },
    teardownAction: SerializableAction = SerializableAction { }
): PCollection<O> {
    val doFn = FlatMapElementFn
        .into(TypeDescriptor.of(O::class.java))
        .via(transform)
        .withSetupAction(setupAction)
        .withStartBundleAction(startBundleAction)
        .withFinishBundleAction(finishBundleAction)
        .withTeardownAction(teardownAction)

    return this.apply(name, ParDo.of(doFn))
}

/**
 * Extension for map operation on [PCollection] with DoFn lifecycle functions.
 * In this case, the current function for transformation has the [DoFn.ProcessContext] as input.
 * So, via this feature we can access to process context of the current transformation.
 * For example [DoFn.ProcessContext] give the possibility to retrieve side inputs while applying the transformation logic.
 *
 * Via this method, while the transformation is done, some methods can be passed to interact with DoFn lifecycle methods
 * (setup, startBundle, finishBundle, teardown).
 *
 * Side inputs as [PCollectionView] can be passed to this DoFn.
 *
 * ```kotlin
 * inputTeamsPCollection
 *      .mapFnWithContext(
 *          "Step name",
 *          { context: DoFn<Team, OtherTeam>.ProcessContext -> toOtherTeamWithSideInputField(sideInput, context) },
 *          setupAction = { print("Test setup action") },
 *          startBundleAction = { print("Test start bundle action") },
 *          finishBundleAction = { print("Test finish bundle action") },
 *          teardownAction = { print("Test teardown action") },
 *          sideInputs = listOf(sideInput)
 *      )
 *      .result
 *
 * fun toOtherTeamWithSideInputField(
 *     sideInput: PCollectionView<String>,
 *     context: DoFn<Team, OtherTeam>.ProcessContext
 * ): OtherTeam {
 *
 *     val inputTeam: Team = context.element()
 *     val otherTeam = TestSettings.toOtherTeam(inputTeam)
 *
 *     otherTeam.sideInputField = context.sideInput(sideInput)
 *
 *     return otherTeam
 * }
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @param setupAction setup action function
 * @param startBundleAction start bundle action function
 * @param finishBundleAction finish bundle action function
 * @param teardownAction teardown action function
 * @param sideInputs side inputs associated to this DoFn class
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <reified I, reified O : Serializable> PCollection<I>.mapFnWithContext(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<DoFn<I, O>.ProcessContext, O>,
    setupAction: SerializableAction = SerializableAction { },
    startBundleAction: SerializableAction = SerializableAction { },
    finishBundleAction: SerializableAction = SerializableAction { },
    teardownAction: SerializableAction = SerializableAction { },
    sideInputs: Iterable<PCollectionView<*>> = emptyList()
): PCollection<O> {
    val doFn = MapProcessContextFn
        .from(I::class.java)
        .into(TypeDescriptor.of(O::class.java))
        .via(transform)
        .withSetupAction(setupAction)
        .withStartBundleAction(startBundleAction)
        .withFinishBundleAction(finishBundleAction)
        .withTeardownAction(teardownAction)

    return this.apply(name, ParDo.of(doFn).withSideInputs(sideInputs))
}

/**
 * Extension for flatMap operation on [PCollection] with DoFn lifecycle functions.
 * In this case, the current function for transformation has the [DoFn.ProcessContext] as input.
 * So, via this feature we can access to process context of the current transformation.
 * For example [DoFn.ProcessContext] give the possibility to retrieve side inputs while applying the transformation logic.
 *
 * Via this method, while the transformation is done, some methods can be passed to interact with DoFn lifecycle methods
 * (setup, startBundle, finishBundle, teardown).
 *
 * Side inputs as [PCollectionView] can be passed to this DoFn.
 *
 * ```kotlin
 * inputTeamsPCollection
 *      .flatMapFnWithContext(
 *          "Step name",
 *          { context: DoFn<Team, Player>.ProcessContext -> toPlayers(sideInput, context) },
 *          setupAction = { print("Test setup action") },
 *          startBundleAction = { print("Test start bundle action") },
 *          finishBundleAction = { print("Test finish bundle action") },
 *          teardownAction = { print("Test teardown action") },
 *          sideInputs = listOf(sideInput)
 *      )
 *      .result
 *
 * fun toPlayers(
 *      sideInput: PCollectionView<String>,
 *      context: DoFn<Team, Player>.ProcessContext
 * ): List<Player> {
 *    // Get side input field.
 *    val sideInputField: String = context.sideInput(sideInput)
 *
 *    val inputTeam: Team = context.element()
 *    val players = inputTeam.players
 *
 *    // Can add logic based on side input field....
 *
 *    return players
 * }
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @param setupAction setup action function
 * @param startBundleAction start bundle action function
 * @param finishBundleAction finish bundle action function
 * @param teardownAction teardown action function
 * @param sideInputs side inputs associated to this DoFn class
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <reified I, reified O : Serializable> PCollection<I>.flatMapFnWithContext(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<DoFn<I, O>.ProcessContext, Iterable<O>>,
    setupAction: SerializableAction = SerializableAction { },
    startBundleAction: SerializableAction = SerializableAction { },
    finishBundleAction: SerializableAction = SerializableAction { },
    teardownAction: SerializableAction = SerializableAction { },
    sideInputs: Iterable<PCollectionView<*>> = emptyList()
): PCollection<O> {
    val doFn = FlatMapProcessContextFn
        .from(I::class.java)
        .into(TypeDescriptor.of(O::class.java))
        .via(transform)
        .withSetupAction(setupAction)
        .withStartBundleAction(startBundleAction)
        .withFinishBundleAction(finishBundleAction)
        .withTeardownAction(teardownAction)

    return this.apply(name, ParDo.of(doFn).withSideInputs(sideInputs))
}

/**
 * Extension for filter operation on a [PCollection].
 *
 * ```kotlin
 * inputTeamsPCollection
 *      .filter("Step name") { team: Team -> this.isNotBarcelona(team) }
 *      .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return the [PCollection] instance after applying the given operation
 */
inline fun <reified I> PCollection<I>.filter(
    name: String = "filter to ${I::class.simpleName}",
    transform: SerializableFunction<I, Boolean>
): PCollection<I> {
    return this.apply(name, Filter.by(transform))
}
