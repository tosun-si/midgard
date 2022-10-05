package fr.groupbees.midgard.transforms

import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors
import java.util.*
import java.util.function.Consumer

/**
 * This class proposes a custom flatMap operation while interacting with [org.apache.beam.sdk.transforms.DoFn]
 * lifecycle.
 *
 * <br></br>
 *
 * This class is based on an output type descriptor and take a [SerializableFunction] to execute the mapping treatment
 * lazily.
 * This output type allows to give type information and handle default coder for output.
 * This function is mandatory and executed in the ProcessElement stage of Beam lifecycle.
 *
 * <br></br>
 *
 * This class can take actions [SerializableAction], used in the DoFn Beam lifecycle.
 *
 *  * withSetupAction : executed in the setup method
 *  * withStartBundleAction : executed in the start bundle method
 *  * withFinishBundleAction : executed in the finish bundle method
 *  * withTeardownAction : executed in the teardown method
 *
 * These functions are not required and if they are given, they are executed lazily in the dedicated method.
 *
 *
 * <br></br>
 * <br></br>
 *
 *
 * Example usage:
 *
 * ```kotlin
 *      // With serializable function but without lifecycle actions.
 *      FlatMapElementFn.into(TypeDescriptor.of(Player.class))
 *                      .via(team -> team.getPlayers())
 *
 *      // With serializable function and some lifecycle actions.
 *      FlatMapElementFn.into(TypeDescriptor.of(Player.class))
 *                      .via(team -> team.getPlayers())
 *                      .withSetupAction(() -> System.out.println("Starting of mapping...")
 *                      .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
 *                      .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
 *                      .withTeardownAction(() -> System.out.println("Ending of mapping...")
 *```
 * @author mazlum
 */
class FlatMapElementFn<InputT, OutputT> private constructor(
    inputType: TypeDescriptor<InputT>,
    outputType: TypeDescriptor<OutputT>,
    private val setupAction: SerializableAction,
    private val startBundleAction: SerializableAction,
    private val finishBundleAction: SerializableAction,
    private val teardownAction: SerializableAction,
    private val inputElementMapper: SerializableFunction<InputT, Iterable<OutputT>>
) : BaseElementFn<InputT, OutputT>(inputType, outputType) {

    /**
     * Method that takes the [SerializableFunction] that will be evaluated in the process element phase.
     *
     *
     * This function is mandatory in process element phase.
     *
     * @param inputElementMapper serializable function from input and to an iterable of output
     * @param <NewInputT>        a NewInputT class
     * @return a [FlatMapElementFn] object
     */
    fun <NewInputT> via(inputElementMapper: SerializableFunction<NewInputT, Iterable<OutputT>>): FlatMapElementFn<NewInputT, OutputT> {
        Objects.requireNonNull(inputElementMapper)

        val inputDescriptor = TypeDescriptors.inputOf(inputElementMapper)
        return FlatMapElementFn(
            inputType = inputDescriptor,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            inputElementMapper = inputElementMapper
        )
    }

    /**
     * Method that takes the [SerializableAction] that will be evaluated in the setup phase.
     *
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction setup action
     * @return a [FlatMapElementFn] object
     */
    fun withSetupAction(setupAction: SerializableAction): FlatMapElementFn<InputT, OutputT> {
        Objects.requireNonNull(setupAction)

        return FlatMapElementFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            inputElementMapper = inputElementMapper
        )
    }

    /**
     * Method that takes the [SerializableAction] that will be evaluated in the start bundle phase.
     *
     * This function is not mandatory in the start bundle phase.
     *
     * @param startBundleAction start bundle action
     * @return a [FlatMapElementFn] object
     */
    fun withStartBundleAction(startBundleAction: SerializableAction): FlatMapElementFn<InputT, OutputT> {
        Objects.requireNonNull(startBundleAction)

        return FlatMapElementFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            inputElementMapper = inputElementMapper
        )
    }

    /**
     * Method that takes the [SerializableAction] that will be evaluated in the finish bundle phase.
     *
     * This function is not mandatory in the finish bundle phase.
     *
     * @param finishBundleAction start bundle action
     * @return a [FlatMapElementFn] object
     */
    fun withFinishBundleAction(finishBundleAction: SerializableAction): FlatMapElementFn<InputT, OutputT> {
        Objects.requireNonNull(finishBundleAction)

        return FlatMapElementFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            inputElementMapper = inputElementMapper
        )
    }

    /**
     * Method that takes the [SerializableAction] that will be evaluated in the teardown phase.
     *
     * This function is not mandatory in the teardown phase.
     *
     * @param teardownAction teardown action
     * @return a [FlatMapElementFn] object
     */
    fun withTeardownAction(teardownAction: SerializableAction): FlatMapElementFn<InputT, OutputT> {
        Objects.requireNonNull(teardownAction)

        return FlatMapElementFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            inputElementMapper = inputElementMapper
        )
    }

    /**
     *
     * Setup action in the DoFn worker lifecycle.
     */
    @Setup
    fun setup() {
        setupAction.execute()
    }

    /**
     *
     * Start bundle action in the DoFn worker lifecycle.
     */
    @StartBundle
    fun startBundle() {
        startBundleAction.execute()
    }

    /**
     *
     * Finish bundle action in the DoFn worker lifecycle.
     */
    @FinishBundle
    fun finishBundle() {
        finishBundleAction.execute()
    }

    /**
     *
     * Teardown action in the DoFn worker lifecycle.
     */
    @Teardown
    fun teardown() {
        teardownAction.execute()
    }

    /**
     * ProcessElement.
     *
     * @param ctx a ProcessContext object
     */
    @ProcessElement
    fun processElement(ctx: ProcessContext) {
        Objects.requireNonNull(inputElementMapper)

        val outputs: Iterable<OutputT> = inputElementMapper.apply(ctx.element())
        outputs.forEach(Consumer { output -> ctx.output(output) })
    }

    companion object {

        /**
         * Factory method of class, that take the output [TypeDescriptor].
         *
         * @param outputType a [TypeDescriptor] object
         * @param <OutputT>  a OutputT class
         * @return a [FlatMapElementFn] object
         */
        fun <OutputT> into(outputType: TypeDescriptor<OutputT>): FlatMapElementFn<*, OutputT> {
            val defaultAction = SerializableAction {}

            return FlatMapElementFn<Any, OutputT>(
                inputType = TypeDescriptor.of(Any::class.java),
                outputType = outputType,
                setupAction = defaultAction,
                startBundleAction = defaultAction,
                finishBundleAction = defaultAction,
                teardownAction = defaultAction,
                inputElementMapper = { t -> t as Iterable<OutputT> }
            )
        }
    }
}