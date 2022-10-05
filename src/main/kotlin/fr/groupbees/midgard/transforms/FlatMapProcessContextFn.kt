package fr.groupbees.midgard.transforms

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.TypeDescriptor
import java.util.*
import java.util.function.Consumer

/**
 * This class allows to handle a generic and custom [org.apache.beam.sdk.transforms.DoFn] for flatMap operation
 * with error handling.
 *
 * <br></br>
 *
 * This class is based on an input class and output type descriptor and take a [org.apache.beam.sdk.transforms.SerializableFunction] to execute
 * the mapping treatment lazily.
 * These types allow to give type information and handle default coders.
 * This function is from [org.apache.beam.sdk.transforms.DoFn.ProcessContext] object to an iterable of output type.
 * In some case, developers need to access to ProcessContext, to get technical data (timestamp...) or handle side inputs.
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
 * <br></br>
 *
 * If there are errors in the process, an failure Tag based on [fr.groupbees.midgard.Failure] object is used to handle
 * the failure output (and side outputs)
 *
 * <br></br>
 *
 * Example usage:
 *
 * ```kotlin
 *      // With serializable function but without lifecycle actions.
 *      FlatMapProcessContextFn.from(Team.class)
 *          .into(TypeDescriptor.of(Player.class))
 *          .via((ProcessContext ctx) -> ctx.element().getPlayers())
 *
 *      // With serializable function and some lifecycle actions.
 *      FlatMapProcessContextFn.from(Team.class)
 *          .into(TypeDescriptor.of(Player.class))
 *          .via((ProcessContext ctx) -> ctx.element().getPlayers())
 *          .withSetupAction(() -> System.out.println("Starting of mapping...")
 *          .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
 *          .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
 *          .withTeardownAction(() -> System.out.println("Ending of mapping...")
 * ```
 *
 * @author mazlum
 */
class FlatMapProcessContextFn<InputT, OutputT> private constructor(
    inputType: TypeDescriptor<InputT>,
    outputType: TypeDescriptor<OutputT>,
    private val setupAction: SerializableAction,
    private val startBundleAction: SerializableAction,
    private val finishBundleAction: SerializableAction,
    private val teardownAction: SerializableAction,
    processContextMapper: SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, Iterable<OutputT>>
) : BaseElementFn<InputT, OutputT>(inputType, outputType) {
    private val processContextMapper: SerializableFunction<ProcessContext, Iterable<OutputT>>

    init {
        this.processContextMapper = processContextMapper
    }

    /**
     * Add the output type descriptors, it's required because it allows to add default coder for Output.
     *
     * @param outputType   a [org.apache.beam.sdk.values.TypeDescriptor] object
     * @param <NewOutputT> a NewOutputT class
     * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
     */
    fun <NewOutputT> into(outputType: TypeDescriptor<NewOutputT>): FlatMapProcessContextFn<InputT, NewOutputT> {
        val defaultAction = SerializableAction {}

        return FlatMapProcessContextFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = defaultAction,
            startBundleAction = defaultAction,
            finishBundleAction = defaultAction,
            teardownAction = defaultAction,
            processContextMapper = { t -> t as Iterable<NewOutputT> }
        )
    }

    /**
     * Method that takes the [org.apache.beam.sdk.transforms.SerializableFunction] that will be evaluated in the process element phase.
     * This function is based on a [org.apache.beam.sdk.transforms.DoFn.ProcessContext] as input and a generic ouput.
     *
     *
     * This function is mandatory in process element phase.
     *
     * @param processContextMapper serializable function from process context and to output
     * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
     */
    fun via(processContextMapper: SerializableFunction<ProcessContext, Iterable<OutputT>>): FlatMapProcessContextFn<InputT, OutputT> {
        Objects.requireNonNull(processContextMapper)

        return FlatMapProcessContextFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            processContextMapper = processContextMapper
        )
    }

    /**
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the setup phase.
     *
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction setup action
     * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
     */
    fun withSetupAction(setupAction: SerializableAction): FlatMapProcessContextFn<InputT, OutputT> {
        Objects.requireNonNull(setupAction)

        return FlatMapProcessContextFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            processContextMapper = processContextMapper
        )
    }

    /**
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the start bundle phase.
     *
     * This function is not mandatory in the start bundle phase.
     *
     * @param startBundleAction start bundle action
     * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
     */
    fun withStartBundleAction(startBundleAction: SerializableAction): FlatMapProcessContextFn<InputT, OutputT> {
        Objects.requireNonNull(startBundleAction)

        return FlatMapProcessContextFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            processContextMapper = processContextMapper
        )
    }

    /**
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the finish bundle phase.
     *
     * This function is not mandatory in the finish bundle phase.
     *
     * @param finishBundleAction finish bundle action
     * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
     */
    fun withFinishBundleAction(finishBundleAction: SerializableAction): FlatMapProcessContextFn<InputT, OutputT> {
        Objects.requireNonNull(finishBundleAction)

        return FlatMapProcessContextFn(
            inputType = inputType,
            outputType = outputType,
            setupAction = setupAction,
            startBundleAction = startBundleAction,
            finishBundleAction = finishBundleAction,
            teardownAction = teardownAction,
            processContextMapper = processContextMapper
        )
    }

    /**
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the teardown phase.
     *
     * This function is not mandatory in the teardown phase.
     *
     * @param teardownAction teardown action
     * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
     */
    fun withTeardownAction(teardownAction: SerializableAction): FlatMapProcessContextFn<InputT, OutputT> {
        Objects.requireNonNull(teardownAction)
        return FlatMapProcessContextFn(
            inputType,
            outputType,
            setupAction,
            startBundleAction,
            finishBundleAction,
            teardownAction,
            processContextMapper
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
     *
     * processElement.
     *
     * @param ctx a [org.apache.beam.sdk.transforms.DoFn.ProcessContext] object
     */
    @ProcessElement
    fun processElement(ctx: ProcessContext) {
        val outputs: Iterable<OutputT> = processContextMapper.apply(ctx)
        outputs.forEach(Consumer { output: OutputT -> ctx.output(output) })
    }

    companion object {

        /**
         * Factory method of class, that take the input type class.
         *
         * @param inputClass a [java.lang.Class] object
         * @param <InputT>   a InputT class
         * @return a [fr.groupbees.midgard.transforms.FlatMapProcessContextFn] object
         */
        fun <InputT> from(inputClass: Class<InputT>): FlatMapProcessContextFn<InputT, *> {
            val defaultAction = SerializableAction {}
            return FlatMapProcessContextFn<InputT, Any>(
                inputType = TypeDescriptor.of(inputClass),
                outputType = TypeDescriptor.of(Any::class.java),
                setupAction = defaultAction,
                startBundleAction = defaultAction,
                finishBundleAction = defaultAction,
                teardownAction = defaultAction,
                processContextMapper = { t -> t as Iterable<Any> }
            )
        }
    }
}