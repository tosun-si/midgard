package fr.groupbees.midgard.transforms

import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors
import java.util.*

/**
 * This class proposes a custom map operation while interacting with [org.apache.beam.sdk.transforms.DoFn]
 * lifecycle.
 *
 * <br></br>
 *
 * This class is based on an output type descriptor and take a [org.apache.beam.sdk.transforms.SerializableFunction] to execute the mapping treatment
 * lazily. This output type allows to give type information and handle default coder for output.
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
 * Example usage:
 *
 * ```kotlin
 *      // With serializable function but without lifecycle actions.
 *      MapElementFn.into(TypeDescriptors.integers())
 *          .via((String word) -> 1 / word.length)
 *
 *      // With serializable function but without start action.
 *      MapElementFn.into(TypeDescriptors.integers())
 *          .via((String word) -> 1 / word.length)
 *          .withSetupAction(() -> System.out.println("Starting of mapping...")
 *          .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
 *          .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
 *          .withTeardownAction(() -> System.out.println("Ending of mapping...")
 * ```
 *
 * @author mazlum
 */
class MapElementFn<InputT, OutputT> private constructor(
    inputType: TypeDescriptor<InputT>,
    outputType: TypeDescriptor<OutputT>,
    private val setupAction: SerializableAction,
    private val startBundleAction: SerializableAction,
    private val finishBundleAction: SerializableAction,
    private val teardownAction: SerializableAction,
    private val inputElementMapper: SerializableFunction<InputT, OutputT>
) : BaseElementFn<InputT, OutputT>(inputType, outputType) {

    /**
     * Method that takes the [org.apache.beam.sdk.transforms.SerializableFunction] that will be evaluated in the process element phase.
     *
     *
     * This function is mandatory in process element phase.
     *
     * @param inputElementMapper serializable function from input and to output
     * @param <NewInputT>        a NewInputT class
     * @return a [fr.groupbees.midgard.transforms.MapElementFn] object
     */
    fun <NewInputT> via(inputElementMapper: SerializableFunction<NewInputT, OutputT>): MapElementFn<NewInputT, OutputT> {
        Objects.requireNonNull(inputElementMapper)

        val inputDescriptor = TypeDescriptors.inputOf(inputElementMapper)
        return MapElementFn(
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
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the setup phase.
     *
     *
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction setup action
     * @return a [fr.groupbees.midgard.transforms.MapElementFn] object
     */
    fun withSetupAction(setupAction: SerializableAction): MapElementFn<InputT, OutputT> {
        Objects.requireNonNull(setupAction)

        return MapElementFn(
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
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the start bundle phase.
     *
     *
     * This function is not mandatory in the start bundle phase.
     *
     * @param startBundleAction start bundle action
     * @return a [fr.groupbees.midgard.transforms.MapElementFn] object
     */
    fun withStartBundleAction(startBundleAction: SerializableAction): MapElementFn<InputT, OutputT> {
        Objects.requireNonNull(startBundleAction)

        return MapElementFn(
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
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the finish bundle phase.
     *
     *
     * This function is not mandatory in the finish bundle phase.
     *
     * @param finishBundleAction finish bundle action
     * @return a [fr.groupbees.midgard.transforms.MapElementFn] object
     */
    fun withFinishBundleAction(finishBundleAction: SerializableAction): MapElementFn<InputT, OutputT> {
        Objects.requireNonNull(finishBundleAction)

        return MapElementFn(
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
     * Method that takes the [fr.groupbees.midgard.transforms.SerializableAction] that will be evaluated in the teardown phase.
     *
     *
     * This function is not mandatory in the teardown phase.
     *
     * @param teardownAction teardown action
     * @return a [fr.groupbees.midgard.transforms.MapElementFn] object
     */
    fun withTeardownAction(teardownAction: SerializableAction): MapElementFn<InputT, OutputT> {
        Objects.requireNonNull(teardownAction)

        return MapElementFn(
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
        ctx.output(inputElementMapper.apply(ctx.element()))
    }

    companion object {

        /**
         * Factory method of class, that take the output [org.apache.beam.sdk.values.TypeDescriptor].
         *
         * @param outputType a [org.apache.beam.sdk.values.TypeDescriptor] object
         * @param <OutputT>  a OutputT class
         * @return a [fr.groupbees.midgard.transforms.MapElementFn] object
         */
        fun <OutputT> into(outputType: TypeDescriptor<OutputT>): MapElementFn<*, OutputT> {
            val defaultAction = SerializableAction {}

            return MapElementFn<Any, OutputT>(
                inputType = TypeDescriptor.of(Any::class.java),
                outputType = outputType,
                setupAction = defaultAction,
                startBundleAction = defaultAction,
                finishBundleAction = defaultAction,
                teardownAction = defaultAction,
                inputElementMapper = { t -> t as OutputT }
            )
        }
    }
}